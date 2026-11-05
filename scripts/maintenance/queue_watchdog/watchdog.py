#!/usr/bin/env python3
"""以业务进度检测停滞，使用原生重启或原容器恢复持久队列。仅依赖标准库。"""

import argparse
import contextlib
import datetime
import fcntl
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

LOCK_ERRORS = ("unknown pathlock lease ref", "owned lease capability does not match")


class ProbeError(Exception):
    def __init__(self, kind):
        self.kind = kind


def save(path, value):
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True)
    temporary.replace(path)


@contextlib.contextmanager
def locked(path, create=False):
    fd = os.open(path, os.O_RDWR | (os.O_CREAT if create else 0), 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(fd)


def plan(state, sample, now, policy, lock_error=False):
    """重试次数和积压增加不算进展；任一队列完成或模型调用增长都算进展。"""
    previous = state.get("sample")
    changed = previous is None or any(
        sample[key] != previous[key] for key in ("instance", "progress")
    )
    changed = changed or (sample["busy"] and previous is not None and not previous["busy"])
    if changed or not sample["busy"] or now < state.get("last_progress", now):
        state["last_progress"] = now
    state["sample"] = sample
    state.pop("unhealthy_since", None)
    pending = state.get("pending_restart")
    if pending and pending["instance"] != sample["instance"]:
        state.pop("pending_restart")
    if not sample["busy"]:
        return "idle"
    if sample.get("protected_work"):
        state["last_progress"] = now
        return "external_work_active"
    stalled = now - state["last_progress"]
    if lock_error and stalled >= policy["lock_stall_seconds"]:
        return "lock_stall"
    if stalled >= max(policy["stall_seconds"], sample["attempt_timeout"] + 120):
        return "stalled"
    return "progress" if changed else "observing"


def permitted(state, now, policy):
    recent = [t for t in state.get("restarts", []) if now - t < policy["window_seconds"]]
    state["restarts"] = recent
    if len(recent) >= policy["max_restarts"]:
        return False
    return not recent or now - recent[-1] >= policy["cooldown_seconds"]


class Monitor:
    def __init__(self, config, apply=False):
        self.config = config
        self.apply = apply
        self.path = Path(config["state_file"])
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {}
        access = json.loads(Path(config["access_file"]).read_text())
        self.base = config["base_url"].rstrip("/")
        self.headers = {
            "X-API-Key": access["root_api_key"],
            "X-OpenViking-Account": config["account_id"],
            "X-OpenViking-User": config["user_id"],
        }
        self.observer_headers = self.headers
        if config.get("observer_access_file"):
            users = json.loads(Path(config["observer_access_file"]).read_text())
            if users["account_id"] != config["account_id"]:
                raise ValueError("观察身份不属于指定账户")
            matches = [row for row in users["users"] if row["user_id"] == config["user_id"]]
            if len(matches) != 1:
                raise ValueError("观察身份必须唯一存在")
            self.observer_headers = {**self.headers, "X-API-Key": matches[0]["api_key"]}
        self.policy = {
            "lock_stall_seconds": 600,
            "stall_seconds": 2400,
            "unhealthy_seconds": 600,
            "observation_gap_seconds": 180,
            "cooldown_seconds": 3600,
            "window_seconds": 21600,
            "max_restarts": 2,
            **config.get("policy", {}),
        }
        if any(value <= 0 for value in self.policy.values()):
            raise ValueError("恢复阈值必须为正数")

    def request(self, path, body=None):
        request = urllib.request.Request(
            self.base + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                **(self.headers if path.startswith("/api/v1/admin/") else self.observer_headers),
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            kind = (
                "auth"
                if error.code in (401, 403)
                else "unreachable"
                if error.code >= 500
                else "schema"
            )
            raise ProbeError(kind) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            raise ProbeError("unreachable") from None
        except (ValueError, TypeError):
            raise ProbeError("schema") from None
        if not isinstance(value, dict) or value.get("status") != "ok":
            raise ProbeError("schema")
        return value.get("result", value)

    def collect(self):
        config = self.request("/api/v1/admin/configuration?source=file")
        queue = self.request("/api/v1/observer/queue?format=json")["status"]
        models = self.request("/api/v1/observer/models?format=json")["status"]
        tasks = self.request("/api/v1/tasks?status=running&include_internal=true&limit=200")
        queues = queue["queues"]
        progress = {
            "queues": sorted([q["queue"], q["processed"], q["errors"]] for q in queues),
            "models": sorted(
                [kind, row["Model"], row["Calls"]]
                for kind in ("vlm", "embedding", "rerank")
                for row in models[kind]
                if "Calls" in row
            ),
            "tasks": sorted([t["task_id"], t.get("stage")] for t in tasks),
        }
        content = config["content"]
        content = json.loads(content) if isinstance(content, str) else content
        vlm = content.get("vlm", {})
        sample = {
            "instance": config["restart"]["instance_id"],
            "progress": progress,
            "busy": any(
                q["pending"] + q["in_progress"] > 0
                for q in queues
                if q["queue"] != "Semantic-Nodes"
            ),
            "protected_work": any(
                q["queue"] == "ExternalTask" and q["in_progress"] > 0 for q in queues
            )
            or any("compile" in t.get("task_type", "") for t in tasks),
            "attempt_timeout": max(
                vlm.get("timeout", 0), vlm.get("media", {}).get("file_processing_timeout", 0)
            ),
        }
        return sample, config

    def run(self, args, timeout=15):
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=True)

    def lock_error(self, since):
        since = datetime.datetime.fromtimestamp(since, datetime.timezone.utc).isoformat()
        try:
            result = self.run(
                ["docker", "logs", "--since", since, "--tail", "5000", self.config["container"]]
            )
        except (subprocess.SubprocessError, OSError):
            return False
        return any(error in result.stdout or error in result.stderr for error in LOCK_ERRORS)

    def validate_container(self):
        info = json.loads(self.run(["docker", "inspect", self.config["container"]]).stdout)[0]
        labels = info["Config"]["Labels"]
        if (
            labels.get("com.docker.compose.project") != self.config["compose_project"]
            or labels.get("com.docker.compose.service") != self.config["compose_service"]
            or not info["State"]["Running"]
        ):
            raise ValueError("容器身份或运行状态不匹配")

    def resume_coordinator(self, now):
        unit = self.config.get("coordinator_unit")
        if not self.apply or not unit or now - self.state.get("coordinator_resume", 0) < 3600:
            return
        phase = json.loads(Path(self.config["coordinator_state_file"]).read_text())["phase"]
        status = self.run(
            ["systemctl", "show", unit, "--property=ActiveState", "--value"]
        ).stdout.strip()
        if phase == "collecting" and status == "failed":
            self.state["coordinator_resume"] = now
            save(self.path, self.state)
            self.run(["systemctl", "reset-failed", unit])
            self.run(["systemctl", "start", "--no-block", unit])

    def tick(self, now=None):
        now = time.time() if now is None else now
        if Path(self.config["pause_file"]).exists():
            self.state["needs_baseline"] = True
            self.state["last_observation"] = {"status": "paused", "at": now}
            save(self.path, self.state)
            return {"status": "paused"}
        last_check = self.state.get("last_check_at")
        if (
            self.state.pop("needs_baseline", False)
            or last_check is None
            or now < last_check
            or now - last_check > self.policy["observation_gap_seconds"]
        ):
            self.state["last_progress"] = now
            self.state.pop("unhealthy_since", None)
        self.state["last_check_at"] = now
        config = None
        try:
            sample, config = self.collect()
        except ProbeError as error:
            if error.kind in ("auth", "schema"):
                self.state.pop("unhealthy_since", None)
                self.state["needs_baseline"] = True
                self.state["last_observation"] = {
                    "status": "probe_error",
                    "kind": error.kind,
                    "at": now,
                }
                save(self.path, self.state)
                return {"status": "probe_error", "kind": error.kind}
            self.state.setdefault("unhealthy_since", now)
            reason = (
                "unreachable"
                if now - self.state["unhealthy_since"] >= self.policy["unhealthy_seconds"]
                else "probing"
            )
            # 没有成功读取过业务状态时，不凭网络错误启动恢复。
            previous = self.state.get("sample") or {}
            if not previous.get("busy") or previous.get("protected_work"):
                reason = "unknown_work_state"
        else:
            reason = plan(self.state, sample, now, self.policy)
            if (
                reason == "observing"
                and now - self.state["last_progress"] >= self.policy["lock_stall_seconds"]
            ):
                reason = plan(
                    self.state,
                    sample,
                    now,
                    self.policy,
                    self.lock_error(self.state["last_progress"]),
                )
            try:
                self.resume_coordinator(now)
                self.state.pop("coordinator_error", None)
            except (KeyError, ValueError, OSError, subprocess.SubprocessError) as error:
                self.state["coordinator_error"] = type(error).__name__
        action = {"status": reason, "at": now}
        if reason in ("lock_stall", "stalled", "unreachable"):
            if not permitted(self.state, now, self.policy):
                action["status"] = "recovery_budget_exhausted"
            elif not self.apply:
                action["would_restart"] = True
            else:
                self.validate_container()
                # 重启前再次采样；避免采样后已恢复或有人刚切换实例。
                if config is not None:
                    latest, latest_config = self.collect()
                    if (
                        latest["instance"] != sample["instance"]
                        or latest["progress"] != sample["progress"]
                        or not latest["busy"]
                        or latest.get("protected_work")
                    ):
                        plan(self.state, latest, now, self.policy)
                        action["status"] = "progress_before_restart"
                        save(self.path, self.state)
                        return action
                    config = latest_config
                self.state.setdefault("restarts", []).append(now)
                instance = self.state["sample"]["instance"]
                self.state["pending_restart"] = {"at": now, "instance": instance}
                self.state["last_progress"] = now
                save(self.path, self.state)  # 先保存意图；超时响应不重放 POST。
                if config is not None:
                    try:
                        self.request("/api/v1/admin/restart", {"revision": config["revision"]})
                        action["status"] = "native_restart_accepted"
                    except ProbeError:
                        action["status"] = "restart_outcome_uncertain"
                else:
                    self.run(
                        ["docker", "restart", "--time", "60", self.config["container"]], timeout=90
                    )
                    self.state.pop("pending_restart", None)
                    action["status"] = "container_restarted"
        self.state["last_observation"] = action
        save(self.path, self.state)
        return action


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--apply", action="store_true", help="允许执行恢复；默认只观察")
    args = parser.parse_args()
    os.umask(0o077)
    config = json.loads(Path(args.config).read_text())
    state = Path(config["state_file"])
    state.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with locked(str(state) + ".lock", create=True) as acquired:
        if not acquired:
            print(json.dumps({"status": "already_running"}))
            return
        with locked(config["deployment_lock"]) as acquired:
            if not acquired:
                previous = json.loads(state.read_text()) if state.exists() else {}
                previous["needs_baseline"] = True
                previous["last_observation"] = {
                    "status": "deployment_in_progress",
                    "at": time.time(),
                }
                save(state, previous)
                print(json.dumps({"status": "deployment_in_progress"}))
                return
            try:
                result = Monitor(config, args.apply).tick()
            except (KeyError, ValueError, OSError, subprocess.SubprocessError) as error:
                # 错误正文可能带业务路径或远端响应；日志只记类型。
                result = {"status": "monitor_error", "kind": type(error).__name__}
            print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
