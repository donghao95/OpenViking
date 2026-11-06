"""为 VM226 打包锁定的依赖更新和完整语法缓存，运行时可离线安装。"""

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import get_args

import tomllib
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_wheel_filename

BASELINE = "2ace1f2965a1e85bea198b243c5277b9bf3cf43b"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def parse_runtime_pins(exported, environment=None):
    pins = {}
    for line in exported.splitlines():
        line = line.strip().rstrip("\\").strip()
        if not line or line.startswith(("#", "--")):
            continue
        requirement = Requirement(line)
        if requirement.marker is not None and not requirement.marker.evaluate(environment):
            continue
        versions = [v.version for v in requirement.specifier if v.operator == "=="]
        if len(versions) != 1:
            raise ValueError("导出的运行依赖必须固定版本：" + line)
        name = str(canonicalize_name(requirement.name))
        if name in pins and pins[name] != versions[0]:
            raise ValueError("同一运行环境解析出多个版本：" + name)
        pins[name] = versions[0]
    return pins


def export_runtime(project, dockerfile, environment=None):
    extras = sorted(set(re.findall(r"--extra ([a-z0-9-]+)", dockerfile)))
    exported = subprocess.check_output(
        [
            "uv",
            "export",
            "--frozen",
            "--project",
            str(project),
            "--no-dev",
            "--no-emit-project",
            *[arg for name in extras for arg in ("--extra", name)],
        ],
        text=True,
    )
    return parse_runtime_pins(exported, environment)


def main():
    if (
        platform.system() != "Linux"
        or platform.machine() != "x86_64"
        or sys.version_info[:2] != (3, 13)
    ):
        raise RuntimeError("部署依赖必须在 Linux amd64 / Python 3.13 上准备")
    new = tomllib.loads(Path("uv.lock").read_text())
    # 先分别解析基线和目标的实际运行闭包，包含新激活的传递依赖及环境标记。
    with tempfile.TemporaryDirectory(prefix="ov-runtime-baseline-") as folder:
        baseline = Path(folder)
        for name in ["pyproject.toml", "uv.lock", "Dockerfile", "README.md"]:
            (baseline / name).write_bytes(
                subprocess.check_output(["git", "show", BASELINE + ":" + name])
            )
        previous = export_runtime(baseline, (baseline / "Dockerfile").read_text())
    desired = export_runtime(Path.cwd(), Path("Dockerfile").read_text())
    changed = {
        p["name"]: p
        for p in new["package"]
        if p["name"] in desired
        and p.get("version") == desired[p["name"]]
        and p.get("version") != previous.get(p["name"])
        and "registry" in p.get("source", {})
    }
    output = Path(".cache/deployment-runtime")
    output.mkdir(parents=True, exist_ok=False)
    wheels = output / "wheels"
    wheels.mkdir()
    requirements = sorted(name + "==" + p["version"] for name, p in changed.items())
    if requirements:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "download",
                "--no-deps",
                "--only-binary=:all:",
                "--dest",
                str(wheels),
                *requirements,
            ],
            check=True,
            timeout=180,
        )
    for wheel in wheels.glob("*.whl"):
        name, version, _, _ = parse_wheel_filename(wheel.name)
        package = changed[str(canonicalize_name(name))]
        assert str(version) == package["version"]
        allowed = {v["hash"] for v in package.get("wheels", [])}
        assert "sha256:" + digest(wheel) in allowed, "依赖制品不匹配 uv.lock"
    assert len(list(wheels.glob("*.whl"))) == len(requirements)
    # 旧运行镜像可能没有 pip；部署时只从这个工具 wheel 临时启动安装器。
    tools = output / "tools"
    tools.mkdir()
    installer_version = importlib.metadata.version("pip")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--no-deps",
            "--only-binary=:all:",
            "--dest",
            str(tools),
            "pip==" + installer_version,
        ],
        check=True,
        timeout=120,
    )

    # 与上游 Dockerfile 保持相同的全量预加载门禁；缺少任何语法即失败。
    from tree_sitter_language_pack import (
        PackConfig,
        SupportedLanguage,
        configure,
        download_all,
        downloaded_languages,
    )

    cache = Path(".cache/deployment-grammars").resolve()
    cache.mkdir(parents=True, exist_ok=False)
    configure(PackConfig(cache_dir=str(cache)))
    download_all()
    missing = sorted(set(get_args(SupportedLanguage)) - set(downloaded_languages()))
    if missing:
        raise RuntimeError("语法预加载不完整：" + ", ".join(missing[:20]))
    os.environ["OPENVIKING_TREE_SITTER_CACHE_DIR"] = str(cache)
    from openviking.parse.parsers.code.ast.process_engine import extract_process_skeleton

    skeleton = extract_process_skeleton(
        "check.py", "def bounded_parser(value):\n    return value\n"
    )
    if not skeleton or "bounded_parser" not in skeleton:
        raise RuntimeError("预加载后实际代码解析验收失败")
    grammar_files = {str(p.relative_to(cache)): digest(p) for p in cache.rglob("*") if p.is_file()}
    if not grammar_files:
        raise RuntimeError("语法缓存为空")
    with tarfile.open(output / "grammar-cache.tar.gz", "w:gz") as archive:
        archive.add(cache, arcname="grammars")
    manifest = {
        "schema": 1,
        "sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "baseline": BASELINE,
        "platform": "linux-amd64",
        "python": "3.13",
        "dependencies": requirements,
        "installer_version": installer_version,
        "grammar_package_version": importlib.metadata.version("tree-sitter-language-pack"),
        "grammar_files": grammar_files,
        "files": {str(p.relative_to(output)): digest(p) for p in output.rglob("*") if p.is_file()},
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(
        json.dumps(
            {
                "dependencies": len(requirements),
                "grammar_files": len(grammar_files),
                "sha": manifest["sha"],
            }
        )
    )


if __name__ == "__main__":
    main()
