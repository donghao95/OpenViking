"""持续集成回归：对照十倍 SQLite 积压下全量与分批恢复的真实 RSS 峰值。"""

import argparse
import asyncio
import json
import os
import resource
import subprocess
import sys
import tempfile
from pathlib import Path


def peak_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


async def worker(args):
    from openviking.storage.queuefs.named_queue import NamedQueue
    from openviking.utils.agfs_utils import RagfsBindingConfig, create_agfs_client
    from openviking_cli.utils.config.agfs_config import AGFSConfig

    client = create_agfs_client(
        RagfsBindingConfig(
            agfs=AGFSConfig(
                path=args.folder,
                backend="local",
                queuefs={"backend": "sqlite"},
            )
        )
    )
    queue = NamedQueue(client, "/queue", "Images")
    if args.mode == "seed":
        # 仅合成内联图片载荷，不调用模型，也不接触生产数据或卷。
        for index in range(args.count):
            await queue.enqueue({"number": index, "image": "data:image/png;base64," + "A" * 65536})
        return {"seeded": args.count}
    await queue._ensure_initialized()
    before_status = await queue._read_backend_status()
    initial = peak_bytes()
    numbers = set()
    if args.mode == "full":
        messages = await queue.snapshot()
        for message in messages:
            numbers.add(json.loads(message["data"])["number"])
    else:
        async for message in queue.iter_snapshot():
            numbers.add(json.loads(message["data"])["number"])
    delta = peak_bytes() - initial
    assert numbers == set(range(args.count)), "恢复消息出现遗漏或意外消息"
    assert await queue._read_backend_status() == before_status, "只读恢复改变了队列状态"
    return {"mode": args.mode, "count": args.count, "peak_delta_bytes": delta}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seed", "full", "paged"])
    parser.add_argument("--folder")
    parser.add_argument("--count", type=int)
    args = parser.parse_args()
    if args.mode:
        print(json.dumps(asyncio.run(worker(args))), flush=True)
        return
    script = Path(__file__).resolve()
    results = []
    with tempfile.TemporaryDirectory(prefix="ov-recovery-memory-") as folder:
        for count in [96, 960]:
            dataset = str(Path(folder) / str(count))
            for mode in ["seed", "full", "paged"]:
                process = subprocess.run(
                    [
                        sys.executable,
                        str(script),
                        "--mode",
                        mode,
                        "--folder",
                        dataset,
                        "--count",
                        str(count),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=180,
                    env=os.environ.copy(),
                    check=True,
                )
                result = json.loads(process.stdout.strip().splitlines()[-1])
                if mode != "seed":
                    results.append(result)
                    print(json.dumps(result), flush=True)
    paged = [row["peak_delta_bytes"] for row in results if row["mode"] == "paged"]
    full = [row["peak_delta_bytes"] for row in results if row["mode"] == "full"]
    assert paged[1] < paged[0] * 3 + 64 * 1024**2, "分批恢复内存随总积压明显增长"
    assert paged[1] < full[1], "分批恢复未降低相同积压下的内存峰值"
    print(json.dumps({"accepted": True, "backlog_scale": 10}), flush=True)


if __name__ == "__main__":
    main()
