#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


TARGETS = [
    ("ampere", "0"),
    ("ampere", "1"),
    ("rtx1-worker", "0"),
]

SETTINGS = [
    {
        "name": "gpt2-p512-o128-3g",
        "families": "llm",
        "llm_models": "gpt2",
        "llm_prompts": "512",
        "llm_outputs": "128",
        "profiles": "3g",
        "llm_requests": "5",
        "llm_warmup": "1",
        "e2e_requests": "5",
    },
    {
        "name": "llama32-3b-p1024-o128-3g",
        "families": "llm",
        "llm_models": "llama32_3b",
        "llm_prompts": "1024",
        "llm_outputs": "128",
        "profiles": "3g",
        "llm_requests": "5",
        "llm_warmup": "1",
        "e2e_requests": "5",
    },
    {
        "name": "llama32-3b-p2048-o64-3g",
        "families": "llm",
        "llm_models": "llama32_3b",
        "llm_prompts": "2048",
        "llm_outputs": "64",
        "profiles": "3g",
        "llm_requests": "5",
        "llm_warmup": "1",
        "e2e_requests": "5",
    },
    {
        "name": "llama32-3b-p4096-o512-3g",
        "families": "llm",
        "llm_models": "llama32_3b",
        "llm_prompts": "4096",
        "llm_outputs": "512",
        "profiles": "3g",
        "llm_requests": "3",
        "llm_warmup": "1",
        "e2e_requests": "3",
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run representative integrated capacity+E2E profiles.")
    parser.add_argument("--out-dir", default="profile/current/e2e-current")
    parser.add_argument("--port", default="10680")
    parser.add_argument("--router-url", default="http://115.145.179.144:10680")
    parser.add_argument("--namespace", default="or-sim")
    args = parser.parse_args()

    runner = Path(__file__).with_name("run_k8s_profile_matrix.py")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for node, gpu_index in TARGETS:
        for setting in SETTINGS:
            run_id = f"profile-e2e-20260624-{node}-gpu{gpu_index}-{setting['name']}"
            raw_path = out_dir / f"{run_id}-runtime-raw.csv"
            if raw_path.exists():
                print(f"skip existing {run_id}", flush=True)
                continue

            cmd = [
                sys.executable,
                str(runner),
                "--node",
                node,
                "--gpu-index",
                gpu_index,
                "--namespace",
                args.namespace,
                "--out-dir",
                str(out_dir),
                "--run-id",
                run_id,
                "--port",
                str(args.port),
                "--access-mode",
                "direct",
                "--families",
                setting["families"],
                "--profiles",
                setting["profiles"],
                "--router-url",
                args.router_url,
                "--e2e-requests",
                setting["e2e_requests"],
                "--e2e-warmup",
                "1",
                "--health-timeout-s",
                "1200",
                "--infer-timeout-s",
                "1800",
            ]
            if setting["families"] == "vision":
                cmd.extend(
                    [
                        "--vision-models",
                        setting["vision_models"],
                        "--vision-batches",
                        setting["vision_batches"],
                        "--vision-requests",
                        setting["vision_requests"],
                        "--vision-warmup",
                        setting["vision_warmup"],
                    ]
                )
            else:
                cmd.extend(
                    [
                        "--llm-models",
                        setting["llm_models"],
                        "--llm-prompts",
                        setting["llm_prompts"],
                        "--llm-outputs",
                        setting["llm_outputs"],
                        "--llm-requests",
                        setting["llm_requests"],
                        "--llm-warmup",
                        setting["llm_warmup"],
                    ]
                )
            print(f"run {run_id}", flush=True)
            subprocess.run(cmd, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
