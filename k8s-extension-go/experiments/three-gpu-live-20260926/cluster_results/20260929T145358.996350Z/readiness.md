# Readiness

- 状态：PASS
- 运行 ID：`20260929T145358.996350Z`
- namespace：`or-sim-exp`
- router：`http://115.145.179.144:10680`
- 实际入口：`/home/we1yq/miniconda3/bin/python3 live_runner_20260926.py --execute --e1 --catalog catalog_newest.csv --conservative-3g-mu --router-url http://115.145.179.144:10680 --stage3-variant sw-c --allow-unsafe-sw-c`
- profiling 原入口：`k8s-extension-go/tools/run_k8s_profile_matrix.py`
- 本轮 in-place 实现：`live_profile_20260926.py`，vision warmup=10、LLM warmup=1、共同 barrier、runtime CUDA 同步计时。
- 观测到 A100：`ampere-gpu0, ampere-gpu1, rtx1-worker-gpu0`
- 带 digest 的运行中容器记录：73 条，详见 `environment.json`。
- 时钟限制：尚无跨节点 offset 实测；跨节点 UTC 次序保留此限制，单进程 duration 使用 monotonic clock。
- In-place transition 未额外构造覆盖；本次只记录冻结 12 轮自然触发的动作。
