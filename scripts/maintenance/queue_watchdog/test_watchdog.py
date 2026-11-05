"""隔离故障演练：不访问真实服务、队列或 Docker。"""

import copy
import importlib.util
import json
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("watchdog", Path(__file__).with_name("watchdog.py"))
w = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(w)

NOW = 1_000_000
POLICY = {
    "lock_stall_seconds": 600,
    "stall_seconds": 2400,
    "unhealthy_seconds": 600,
    "observation_gap_seconds": 180,
    "cooldown_seconds": 3600,
    "window_seconds": 21600,
    "max_restarts": 2,
}


def sample(calls=0, nodes=0, instance="first", busy=True):
    return {
        "instance": instance,
        "busy": busy,
        "attempt_timeout": 1800,
        "progress": {"calls": calls, "nodes": nodes},
    }


class FakeMonitor(w.Monitor):
    def __init__(self, directory, apply=True):
        self.config = {
            "container": "synthetic-ov",
            "compose_project": "synthetic",
            "compose_service": "ov",
            "pause_file": str(directory / "pause"),
        }
        self.apply = apply
        self.path = directory / "state.json"
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.policy = POLICY.copy()
        self.current = sample()
        self.config_file = {"revision": "same-config"}
        self.failure = None
        self.lock_failure = False
        self.calls = []
        self.samples = []

    def advance(self, now):
        previous = self.state.get("last_check_at", now)
        for timestamp in range(int(previous) + 60, int(now), 60):
            self.tick(timestamp)
        return self.tick(now)

    def collect(self):
        if self.failure:
            raise w.ProbeError(self.failure)
        return copy.deepcopy(
            self.samples.pop(0) if self.samples else self.current
        ), self.config_file

    def lock_error(self, since):
        return self.lock_failure

    def request(self, path, body=None):
        self.calls.append((path, body))
        return {"restarting": True}

    def validate_container(self):
        self.calls.append(("validate", None))

    def run(self, args, timeout=15):
        self.calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.state = {}
        w.plan(self.state, sample(), NOW, POLICY)

    def test_lock_failure_requires_ten_minutes_without_progress(self):
        self.assertEqual(w.plan(self.state, sample(), NOW + 599, POLICY, True), "observing")
        self.assertEqual(w.plan(self.state, sample(), NOW + 600, POLICY, True), "lock_stall")

    def test_model_or_semantic_node_completion_resets_deadline(self):
        for current in (sample(calls=1), sample(nodes=1)):
            with self.subTest(current=current):
                state = copy.deepcopy(self.state)
                self.assertEqual(w.plan(state, current, NOW + 2399, POLICY, True), "progress")
                self.assertEqual(state["last_progress"], NOW + 2399)

    def test_idle_never_restarts_and_restart_changes_baseline(self):
        self.assertEqual(w.plan(self.state, sample(busy=False), NOW + 9999, POLICY, True), "idle")
        self.assertEqual(
            w.plan(self.state, sample(instance="new"), NOW + 10000, POLICY, True), "progress"
        )

    def test_long_attempt_timeout_extends_silent_stall_window(self):
        current = sample()
        current["attempt_timeout"] = 5000
        self.assertEqual(w.plan(self.state, current, NOW + 5000, POLICY), "observing")
        self.assertEqual(w.plan(self.state, current, NOW + 5120, POLICY), "stalled")

    def test_clock_rollback_rebaselines_without_restart(self):
        self.assertEqual(w.plan(self.state, sample(), NOW - 1, POLICY, True), "observing")
        self.assertEqual(self.state["last_progress"], NOW - 1)

    def test_cooldown_and_window_budget_survive_new_process(self):
        state = {"restarts": [NOW]}
        self.assertFalse(w.permitted(state, NOW + 600, POLICY))
        self.assertTrue(w.permitted(state, NOW + 3600, POLICY))
        state["restarts"].append(NOW + 3600)
        self.assertFalse(w.permitted(state, NOW + 7200, POLICY))
        self.assertTrue(w.permitted(state, NOW + 21601, POLICY))


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.monitor = FakeMonitor(self.path)
        self.monitor.advance(NOW)

    def test_native_restart_uses_current_revision_and_persists_intent(self):
        self.monitor.lock_failure = True
        result = self.monitor.advance(NOW + 600)
        self.assertEqual(result["status"], "native_restart_accepted")
        self.assertIn(("/api/v1/admin/restart", {"revision": "same-config"}), self.monitor.calls)
        saved = json.loads(self.monitor.path.read_text())
        self.assertEqual(saved["pending_restart"]["instance"], "first")
        self.assertEqual(saved["restarts"], [NOW + 600])

    def test_observe_mode_and_maintenance_do_not_restart(self):
        self.monitor.apply = False
        self.monitor.lock_failure = True
        self.assertTrue(self.monitor.advance(NOW + 600)["would_restart"])
        self.assertEqual(self.monitor.calls, [])
        Path(self.monitor.config["pause_file"]).touch()
        self.assertEqual(self.monitor.advance(NOW + 1000)["status"], "paused")

    def test_progress_between_probes_aborts_restart(self):
        self.monitor.lock_failure = True
        self.monitor.advance(NOW + 599)
        self.monitor.samples = [sample(), sample(nodes=1)]
        self.assertEqual(self.monitor.advance(NOW + 600)["status"], "progress_before_restart")
        self.assertEqual(self.monitor.state.get("restarts", []), [])

    def test_auth_or_schema_error_never_restarts(self):
        for failure in ("auth", "schema"):
            self.monitor.failure = failure
            self.assertEqual(self.monitor.advance(NOW + 10000)["status"], "probe_error")
        self.assertEqual(self.monitor.calls, [])

    def test_authenticated_liveness_failure_needs_sustained_observation(self):
        self.monitor.failure = "unreachable"
        self.assertEqual(self.monitor.advance(NOW + 1)["status"], "probing")
        self.assertEqual(self.monitor.advance(NOW + 600)["status"], "probing")
        self.assertEqual(self.monitor.advance(NOW + 601)["status"], "container_restarted")
        self.assertIn(["docker", "restart", "--time", "60", "synthetic-ov"], self.monitor.calls)

    def test_no_prior_success_means_no_blind_container_restart(self):
        self.monitor.state = {}
        self.monitor.failure = "unreachable"
        self.monitor.advance(NOW)
        self.assertEqual(self.monitor.advance(NOW + 10000)["status"], "unknown_work_state")
        self.assertEqual(self.monitor.calls, [])

    def test_uncertain_post_is_not_replayed_after_process_restart(self):
        def lost_response(path, body=None):
            raise w.ProbeError("unreachable")

        self.monitor.request = lost_response
        self.monitor.lock_failure = True
        self.assertEqual(self.monitor.advance(NOW + 600)["status"], "restart_outcome_uncertain")
        monitor = FakeMonitor(self.path)
        monitor.lock_failure = True
        self.assertEqual(monitor.advance(NOW + 1200)["status"], "recovery_budget_exhausted")
        self.assertEqual(monitor.calls, [])

    def test_wedged_native_restart_does_not_bypass_hourly_cooldown(self):
        self.monitor.lock_failure = True
        self.monitor.advance(NOW + 600)
        self.monitor.failure = "unreachable"
        self.monitor.advance(NOW + 601)
        self.assertEqual(self.monitor.advance(NOW + 1201)["status"], "recovery_budget_exhausted")
        self.assertEqual(self.monitor.advance(NOW + 4200)["status"], "container_restarted")
        self.assertEqual(self.monitor.advance(NOW + 10000)["status"], "recovery_budget_exhausted")

    def test_container_identity_mismatch_blocks_mutation(self):
        self.monitor.lock_failure = True
        self.monitor.validate_container = lambda: (_ for _ in ()).throw(ValueError("mismatch"))
        with self.assertRaises(ValueError):
            self.monitor.advance(NOW + 600)
        self.assertEqual(self.monitor.calls, [])

    def test_coordinator_failure_isolated_from_core_monitor(self):
        def failed(now):
            raise OSError("simulated")

        self.monitor.resume_coordinator = failed
        self.assertEqual(self.monitor.advance(NOW + 1)["status"], "observing")
        self.assertEqual(self.monitor.state["coordinator_error"], "OSError")

    def test_empty_queue_with_old_lock_errors_never_restarts(self):
        self.monitor.current = sample(busy=False)
        self.monitor.lock_failure = True
        self.assertEqual(self.monitor.tick(NOW + 1)["status"], "idle")
        self.assertEqual(self.monitor.tick(NOW + 86400)["status"], "idle")
        self.assertEqual(self.monitor.calls, [])

    def test_new_work_after_long_idle_gets_a_fresh_stall_window(self):
        self.monitor.current = sample(busy=False)
        self.monitor.tick(NOW + 1)
        self.monitor.tick(NOW + 86400)
        self.monitor.current = sample()
        self.monitor.lock_failure = True
        self.assertEqual(self.monitor.tick(NOW + 86401)["status"], "progress")
        self.assertEqual(self.monitor.advance(NOW + 87000)["status"], "observing")
        self.assertEqual(self.monitor.calls, [])

    def test_queue_drained_between_probes_cancels_restart(self):
        self.monitor.lock_failure = True
        self.monitor.advance(NOW + 599)
        self.monitor.samples = [sample(), sample(busy=False)]
        self.assertEqual(self.monitor.tick(NOW + 600)["status"], "progress_before_restart")
        self.assertEqual(self.monitor.state.get("restarts", []), [])

    def test_probe_failures_after_idle_are_unknown_not_backlog(self):
        self.monitor.current = sample(busy=False)
        self.monitor.tick(NOW + 1)
        self.monitor.failure = "unreachable"
        self.assertEqual(self.monitor.advance(NOW + 3000)["status"], "unknown_work_state")
        self.assertEqual(self.monitor.calls, [])

    def test_observation_gap_requires_an_entire_new_stall_window(self):
        self.monitor.lock_failure = True
        self.assertEqual(self.monitor.tick(NOW + 10000)["status"], "observing")
        self.assertEqual(self.monitor.advance(NOW + 10599)["status"], "observing")
        self.assertEqual(self.monitor.calls, [])
        self.assertEqual(self.monitor.tick(NOW + 10600)["status"], "native_restart_accepted")

    def test_auth_failure_breaks_the_continuous_unreachable_window(self):
        self.monitor.failure = "unreachable"
        self.monitor.tick(NOW + 1)
        self.monitor.advance(NOW + 590)
        self.monitor.failure = "auth"
        self.assertEqual(self.monitor.tick(NOW + 591)["status"], "probe_error")
        self.monitor.failure = "unreachable"
        self.monitor.tick(NOW + 592)
        self.assertEqual(self.monitor.advance(NOW + 1191)["status"], "probing")
        self.assertEqual(self.monitor.calls, [])

    def test_long_external_compile_is_protected_from_global_restart(self):
        self.monitor.current["protected_work"] = True
        self.monitor.lock_failure = True
        self.assertEqual(self.monitor.advance(NOW + 3000)["status"], "external_work_active")
        self.monitor.failure = "unreachable"
        self.assertEqual(self.monitor.advance(NOW + 4000)["status"], "unknown_work_state")
        self.assertEqual(self.monitor.calls, [])

    def test_pause_resume_rebaselines_even_without_a_long_observation_gap(self):
        self.monitor.lock_failure = True
        self.monitor.advance(NOW + 599)
        pause = Path(self.monitor.config["pause_file"])
        pause.touch()
        self.assertEqual(self.monitor.tick(NOW + 600)["status"], "paused")
        pause.unlink()
        self.assertEqual(self.monitor.tick(NOW + 601)["status"], "observing")
        self.assertEqual(self.monitor.calls, [])


class TransportTests(unittest.TestCase):
    def test_only_virtual_nodes_and_historical_errors_mean_no_durable_backlog(self):
        monitor = object.__new__(w.Monitor)
        responses = {
            "/api/v1/admin/configuration?source=file": {
                "content": "{}",
                "restart": {"instance_id": "one"},
            },
            "/api/v1/observer/queue?format=json": {
                "status": {
                    "queues": [
                        {
                            "queue": "AddResource",
                            "processed": 100,
                            "errors": 9,
                            "pending": 0,
                            "in_progress": 0,
                        },
                        {
                            "queue": "Semantic-Nodes",
                            "processed": 1000,
                            "errors": 7,
                            "pending": 200,
                            "in_progress": 32,
                        },
                    ]
                }
            },
            "/api/v1/observer/models?format=json": {
                "status": {
                    "vlm": [{"Model": "synthetic", "Calls": 99}],
                    "embedding": [],
                    "rerank": [],
                }
            },
            "/api/v1/tasks?status=running&include_internal=true&limit=200": [
                {"task_id": "old-task", "stage": "processing_queue"}
            ],
        }
        monitor.request = lambda path: responses[path]
        current, _ = monitor.collect()
        self.assertFalse(current["busy"])

    def test_deployment_lock_blocks_all_probes_and_rebaselines_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "deploy.lock").touch()
            w.save(
                path / "config",
                {"state_file": str(path / "state"), "deployment_lock": str(path / "deploy.lock")},
            )
            with w.locked(path / "deploy.lock"):
                result = subprocess.run(
                    [
                        "python3",
                        str(Path(__file__).with_name("watchdog.py")),
                        "--config",
                        str(path / "config"),
                        "--apply",
                    ],
                    text=True,
                    capture_output=True,
                    check=True,
                )
            self.assertEqual(json.loads(result.stdout)["status"], "deployment_in_progress")
            self.assertTrue(json.loads((path / "state").read_text())["needs_baseline"])

    def test_real_http_stall_recovery_uses_owner_for_observation_and_root_for_restart(self):
        observed = []
        runtime = {"instance": "first", "restarts": 0}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                observed.append((self.path, self.headers.get("X-API-Key")))
                if self.path.startswith("/api/v1/admin/"):
                    result = {
                        "content": "{}",
                        "revision": "same",
                        "restart": {"instance_id": runtime["instance"]},
                    }
                elif "/queue?" in self.path:
                    result = {
                        "status": {
                            "queues": [
                                {
                                    "queue": "Semantic",
                                    "pending": 0,
                                    "in_progress": 1,
                                    "processed": 0,
                                    "errors": 0,
                                },
                                {
                                    "queue": "Semantic-Nodes",
                                    "pending": 2,
                                    "in_progress": 1,
                                    "processed": 0,
                                    "errors": 0,
                                },
                            ]
                        }
                    }
                elif "/models?" in self.path:
                    result = {"status": {"vlm": [], "embedding": [], "rerank": []}}
                else:
                    result = []
                self.send_response(200)
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok", "result": result}).encode())

            def do_POST(self):
                observed.append((self.path, self.headers.get("X-API-Key")))
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if (
                    body == {"revision": "same"}
                    and self.headers.get("X-API-Key") == "synthetic-root"
                ):
                    runtime.update(instance="second", restarts=runtime["restarts"] + 1)
                self.send_response(202)
                self.end_headers()
                self.wfile.write(b'{"status":"ok","result":{"restarting":true}}')

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory)
                w.save(path / "root", {"root_api_key": "synthetic-root"})
                w.save(
                    path / "users",
                    {
                        "account_id": "synthetic",
                        "users": [{"user_id": "owner", "api_key": "synthetic-owner"}],
                    },
                )
                monitor = w.Monitor(
                    {
                        "base_url": f"http://127.0.0.1:{server.server_port}",
                        "access_file": str(path / "root"),
                        "observer_access_file": str(path / "users"),
                        "account_id": "synthetic",
                        "user_id": "owner",
                        "state_file": str(path / "state"),
                        "pause_file": str(path / "pause"),
                        "container": "synthetic",
                        "compose_project": "synthetic",
                        "compose_service": "ov",
                    },
                    apply=True,
                )

                def docker(args, timeout=15):
                    if args[1] == "logs":
                        text = "unknown pathlock lease ref"
                    elif args[1] == "inspect":
                        text = json.dumps(
                            [
                                {
                                    "State": {"Running": True},
                                    "Config": {
                                        "Labels": {
                                            "com.docker.compose.project": "synthetic",
                                            "com.docker.compose.service": "ov",
                                        }
                                    },
                                }
                            ]
                        )
                    else:
                        raise AssertionError("演练不应调用其他 Docker 命令")
                    return subprocess.CompletedProcess(args, 0, stdout=text, stderr="")

                monitor.run = docker
                monitor.tick(NOW)
                for timestamp in range(NOW + 60, NOW + 600, 60):
                    monitor.tick(timestamp)
                self.assertEqual(monitor.tick(NOW + 600)["status"], "native_restart_accepted")
                self.assertEqual(monitor.tick(NOW + 601)["status"], "progress")
                self.assertEqual(runtime["restarts"], 1)
                self.assertNotIn("pending_restart", monitor.state)
                for endpoint, key in observed:
                    self.assertEqual(
                        key, "synthetic-root" if "/admin/" in endpoint else "synthetic-owner"
                    )
        finally:
            server.shutdown()
            worker.join(timeout=2)
            server.server_close()

    def test_collector_uses_node_and_model_completions_not_requeues(self):
        monitor = object.__new__(w.Monitor)
        responses = {
            "/api/v1/admin/configuration?source=file": {
                "content": {"vlm": {"timeout": 1800}},
                "restart": {"instance_id": "one"},
            },
            "/api/v1/observer/queue?format=json": {
                "status": {
                    "queues": [
                        {
                            "queue": "Semantic-Nodes",
                            "processed": 1,
                            "errors": 0,
                            "pending": 10,
                            "in_progress": 2,
                            "requeued": 0,
                        }
                    ]
                }
            },
            "/api/v1/observer/models?format=json": {
                "status": {
                    "vlm": [{"Model": "synthetic", "Calls": 1}],
                    "embedding": [],
                    "rerank": [],
                }
            },
            "/api/v1/tasks?status=running&include_internal=true&limit=200": [],
        }
        monitor.request = lambda path: copy.deepcopy(responses[path])
        first, _ = monitor.collect()
        responses["/api/v1/observer/queue?format=json"]["status"]["queues"][0]["requeued"] = 100
        second, _ = monitor.collect()
        self.assertEqual(first["progress"], second["progress"])
        responses["/api/v1/observer/queue?format=json"]["status"]["queues"][0]["processed"] = 2
        third, _ = monitor.collect()
        self.assertNotEqual(first["progress"], third["progress"])

    def test_exclusive_lock_and_private_state_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state"
            w.save(path, {"safe": 1})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with w.locked(path) as first, w.locked(path) as second:
                self.assertTrue(first)
                self.assertFalse(second)

    def test_http_auth_and_server_errors_are_distinguished(self):
        monitor = object.__new__(w.Monitor)
        monitor.base, monitor.headers, monitor.observer_headers = "http://localhost", {}, {}
        for status, expected in [
            (401, "auth"),
            (403, "auth"),
            (404, "schema"),
            (503, "unreachable"),
        ]:
            error = w.urllib.error.HTTPError(monitor.base, status, "synthetic", None, None)
            with patch.object(w.urllib.request, "urlopen", side_effect=error):
                with self.assertRaises(w.ProbeError) as caught:
                    monitor.request("/synthetic")
                self.assertEqual(caught.exception.kind, expected)

    def test_root_model_status_without_usage_does_not_break_collector(self):
        monitor = object.__new__(w.Monitor)
        responses = {
            "/api/v1/admin/configuration?source=file": {
                "content": "{}",
                "restart": {"instance_id": "one"},
            },
            "/api/v1/observer/queue?format=json": {"status": {"queues": []}},
            "/api/v1/observer/models?format=json": {
                "status": {
                    "vlm": [{"Model": "synthetic", "Status": "ready"}],
                    "embedding": [],
                    "rerank": [],
                }
            },
            "/api/v1/tasks?status=running&include_internal=true&limit=200": [],
        }
        monitor.request = lambda path: responses[path]
        current, _ = monitor.collect()
        self.assertEqual(current["progress"]["models"], [])


if __name__ == "__main__":
    unittest.main()
