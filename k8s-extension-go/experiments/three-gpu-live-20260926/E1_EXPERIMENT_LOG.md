# Section 4.6 / E1 实验记录（2026-09-28 — 2026-09-30）

> 最终实验（2026-09-30，catalog v3、Poisson 发流、最新组件）及其分析见 `E1_FINAL_20260930.md`。本文第 3 节的结果（2026-09-29 的 run）已被取代，保留作为过程记录。

本文记录 Section 4.6（动作耗时、makespan）与 E1（全程带流量的 12 轮变换）从准备到最终结果的完整过程。

- 最终数据是第 3 节的两个 run。
- 中途失败或被取代的 run 已从仓库删除，第 5 节记录了它们暴露的问题和对应的修复。

## 1. 实验要回答的问题

**4.6（在修复后的 executor 上重测）**
- 按 Stage-3 动作类型 × workload 统计动作耗时。同一动作在不同 workload 间差异明显的，单独标出。
- 每一轮的 makespan。
- 实测 makespan 与"关键路径 × 中位动作耗时"估计值的对比。
- 两项检查：
  - MIG 操作受每 GPU 的锁保护：同一 GPU 上的 MIG 操作从不重叠，不同 GPU 之间可以并行。
  - 路由更新对每个 replica 是原子的。

**E1**
- 流程：初始化加 11 次变换，全程带流量，SliceWise 和 SW−C 在完全相同的负载下各跑一次。
- 负载：开环、固定到达间隔。变换期间按 commitment = min(旧需求, 新需求) 发；executor 报告完成后切到新需求，稳态停留 30 s。R13 把集群清空。
- 记录：
  - ledger 中每秒的就绪容量；
  - 每个变换窗口的请求统计：完成 / 发出、超 SLO 比例、最大延迟 / SLO。LLM 的 TTFT 和 TPOT 分开判断，不算每秒 p95。
- 交付物：vision 时序图、LLM 的逐轮表、最低吞吐 / commitment。

## 2. 最终系统配置

| 组件 | 镜像 / 设置 | 说明 |
|---|---|---|
| runtime（vision / gpt2 / llama） | `torchvision-worker-20260929`、`gpt2-medium-worker-20260929`、`llama32-3b-worker-20260929` | 所有 CUDA 调用在一个固定线程上执行（`CudaWorker`）；LLM 在一次 `generate()` 内部标记 TTFT；runtime 固定在一个物理核上（`OR_SIM_CPU_SET`） |
| runtime-router | `pipe-20260929` | vision 每个 replica 最多同时 2 个 batch 在路上（第二个只在满 batch 时发）；LLM 每个 replica 同一时间 1 个请求；带版本号的路由持久化；按 replica 的 PATCH |
| transition-executor | `control-plane:e1b-20260929` | 见 5.2 |
| planner-controller / planner-engine | `control-plane:headroom-20260929`、`planner-engine:headroom-20260929`（commit `cf6efae`） | catalog 来自 `profile/newest`；最终 run 打开 `conservative3gMu`，不加 h |
| node-agent / slot-device-plugin | `node-agent:e1-20260929` | 每 GPU 的 flock；自动清理过期的 `or-sim.io` 资源 |
| cluster-state-manager | `control-plane:batch-state-concurrency-20260928` | |
| catalog | `planner-engine/app/mock/profile-catalogs/*.yaml`、`catalog_newest.csv` | 每块 GPU 取中位延迟对应的吞吐，再在 3 块 GPU 中取最小值；全部 84 个 option 由 `profile/newest` 实测 |

运行命令：

```bash
python3 live_runner_20260926.py --execute --e1 --catalog catalog_newest.csv --conservative-3g-mu \
    --router-url http://115.145.179.144:10680                                  # SliceWise
python3 live_runner_20260926.py --execute --e1 --catalog catalog_newest.csv --conservative-3g-mu \
    --router-url http://115.145.179.144:10680 --stage3-variant sw-c --allow-unsafe-sw-c   # SW-C
python3 compact_requests.py <run>; python3 e1_analyze.py <run>; python3 s46_analyze.py <run>
```

## 3. 最终结果

| run | 变体 | 目录 |
|---|---|---|
| SliceWise | `--conservative-3g-mu` | `cluster_results/20260929T143422.387493Z` |
| SW−C | `--conservative-3g-mu --stage3-variant sw-c` | `cluster_results/20260929T145358.996350Z` |

**验收检查（两个 run 都通过）**
- 12 轮全部 reached target，final validation 通过，strict runtime audit PASS，R13 把集群清空。
- 12 轮的 plan 中都记录了 `milp.conservative3gMu = true`，`capacityHeadroomEffective = 0`。
- 发送端最长发送间隔为 0.099 s（SliceWise）和 0.085 s（SW−C）。
- SliceWise 在变换窗口中没有 404，也没有其他请求失败。
- 所有低于需求的情况都能归到 5.5 的 ampere 局限。

**makespan（秒）**

| | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 | R10 | R11 | R12 | 合计 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SliceWise | 27.8 | 3.1 | 16.4 | 31.4 | 40.3 | 3.7 | 3.7 | 2.1 | 25.0 | 7.9 | 29.0 | 24.5 | 214.9 |
| SW−C | 27.8 | 3.2 | 12.3 | 23.5 | 40.8 | 3.8 | 3.7 | 2.0 | 22.8 | 8.1 | 22.6 | 22.9 | 193.2 |

**E1：vision，只看变换窗口**

| | 最差窗口的整体比值（轮） | 每秒最低值 | 低于 90% 的秒数 | 404 | 稳态最低完成率（轮） |
|---|---|---|---|---|---|
| SliceWise resnet50 | 0.852（R4） | 0.695 | 44/297 | 0 | 0.813（R4） |
| SliceWise vgg16 | 0.952（R5） | 0.709 | 25/297 | 0 | 0.945（R4） |
| SliceWise vit_base | 0.996（R3） | 0.742 | 17/297 | 0 | 0.967（R9） |
| SW−C resnet50 | 0.596（R12） | 0.000 | 43/276 | 2134 | 0.808（R4） |
| SW−C vgg16 | 0.675（R5） | 0.000 | 75/276 | 3040 | 0.955（R4） |
| SW−C vit_base | 0.345（R5） | 0.000 | 44/276 | 4010 | 0.972（R9） |

说明：
- **整体比值**：窗口内完成的请求数 ÷（commitment × 窗口秒数）。
- **每秒最低值**：按整秒统计的完成量 ÷ commitment 中最低的一秒；它受批量完成时间点的影响，会有抖动。
- **SliceWise 中 resnet 的 0.85**：来自 R4 的稳态不足（ampere 上的 1g b1，见 5.5），不是变换本身造成的。

**E1：LLM，变换窗口中完成的请求 / 发出的请求**

| | gpt2_p64_o64 | gpt2_p512_o512 | llama_p1024_o128 | llama_p2048_o64 |
|---|---|---|---|---|
| SliceWise | 22/22 | 19/19 | 23/23 | 19/19 |
| SW−C | 14/21 | 18/18 | 23/23 | 13/18 |

- 两个 run 中 TPOT 都没有超 SLO。
- TTFT 超 SLO 的请求数：SliceWise 1 个，SW−C 2 个，都是 gpt2_p512 的请求排在前一个长请求后面等待。

**最低吞吐 / commitment（`analysis/min_ratio.csv`）**

| | resnet50 | vgg16 | vit_base | gpt2_p64 | gpt2_p512 | llama_p1024 | llama_p2048 |
|---|---|---|---|---|---|---|---|
| SliceWise | 0.695（R4） | 0.709（R11） | 0.742（R5） | 1.0 | 1.0 | 1.0 | 1.0 |
| SW−C | 0.0（R12） | 0.0（R12） | 0.0（R5） | 0.364（R5） | 1.0 | 1.0 | 0.167（R4） |

vision 取每秒最低值，LLM 取每窗口的完成 / 发出。

**4.6（`analysis/s46_*.csv`，SliceWise）**
- **动作耗时中位数**：
  - `activate_instance_route` 在新 replica 放置之后为 14.5 s，要等模型加载；**不同 workload 差异明显**：vgg16 9.0、vit_base 11.0、resnet50 12.0、gpt2 12.7–14.3、llama 15.5–16.6 s。
  - `register_mig_devices` 6.8 s，`delete_instance` 4.0 s，`activate_instance_route` 在原地改 batch 之后为 3.7 s，`configure_full_template` 3.1 s，`clear_template` 1.9 s，`configure_partial_profile` 1.5 s，`place_instance` 0.59 s，`return_gpu` 0.34 s。
  - 其余 8 种动作都 ≤ 0.02 s。
  - SW−C 的数值接近。
- **makespan 与估计值**：估计值是 DAG 最长路径上各动作按"类型 × workload"取中位耗时之和，`activate_instance_route` 按"放置之后 / 改 batch 之后"分开计算。实测 / 估计的中位数为 0.98，范围 0.58–1.30（SW−C 为 0.98，范围 0.62–1.43）。偏低的是只改 batch 的小轮次，例如 R8。
- **MIG 每 GPU 锁**：同一 GPU 上的 MIG 操作重叠 0 次；不同 GPU 之间有 9 次重叠（SW−C 为 15 次），说明跨 GPU 是并行的。
- **路由原子性**：同一 replica 上重叠的路由 / batch 更新为 0 次。SliceWise 变换窗口中，除了发送端 pending 上限的拒绝，没有其他请求失败。

**SliceWise 与 SW−C 对比**：SW−C 的 makespan 短 10%（193.2 s 对 214.9 s）。但它在变换中让 3 个 vision workload 都出现了断流，整体比值最低降到 0.35，LLM 也有请求失败。SliceWise 在变换中保住了全部容量。

## 4. Catalog

- **测量**：`profile/newest`，runtime 固定在一个线程上。每块 GPU 上一次只运行一个 runtime，runtime 固定在一个物理核上。
  - vision：10 次预热，50 次测量；
  - LLM：1 次预热，10 次测量。
  - 每块 GPU 3520 个样本，0 个错误。
- **标记为 `fit=false` 的 9 个 option**：已在 `unmeasured_options/` 实测，全部超出 SLO，所以继续标记为不可用。
- **SLO**：所有可用 option 的 p95 都在 SLO 以内，`fitSlo` 标记和实测一致。这里的 p95 是隔离条件下测的，不含排队。
- **单调性**：按 1g < 2g < 3g < 4g < 7g 比较 65 组相邻 profile，有 8 组吞吐下降，降幅 0.3–3.8%。
  - 涉及的 option：resnet50 b1（2g→3g、3g→4g）、resnet50 b4（3g→4g）、vgg16 b1（3g→4g）、gpt2_p64（2g→3g、3g→4g）、gpt2_p512（3g→4g、4g→7g）。
  - 原因：这些 option 受 CPU 发射 kernel 的速度限制，不受 GPU 算力限制。3g 和 4g 的显存份额相同，只是 SM 数量不同。
  - 只用 rtx1 的数据仍有 6 组下降，而且主要是同一批 option，所以大部分是真实现象，不是噪声。
  - vgg16 b1 3g→4g 那一组来自 ampere 的 CPU 侧延迟，重测结果相同（`vgg16_b1_3g4g_repeat/`）。
  - 这些值都保留实测值，没有调整。
- **planner 如何处理 3g→4g**：planner 不允许一块 GPU 上放两个 3g（会留出一段不能用的空间），所以两个 3g 需求中的一个会放进 4g slot。
  - Stage 2 用 4g 的实际 mu 检查容量，因此不会出现容量低于需求的情况。
  - 但 Stage 1 是按 3g 的 mu 规划的，4g 更慢时 Stage 2 可能无解。
  - `conservative3gMu` 让 Stage 1 对 3g option 使用 min(mu₃g, mu₄g)，Stage 2 因此一定可行。
  - 离线对比：h = 0–25% 的 6 档下，打开这个开关后，12 轮的完整 target（含每个实例的 mu）和 Stage-3 动作序列完全不变。

## 5. 过程、失败的尝试与修复

### 5.1 主机与测量环境
- **rtx1 的 CPU 频率被锁在 2.3 GHz**：turbo 被 MSR 0x1a0 bit38 关闭，cpufrequtils 的 MIN/MAX 都设为 2.3 GHz，C6 被禁用。修复后恢复正常。
- **rtx1 上有一个慢的物理核**：CPU 1/33 会让 LLM decode 变慢约 26%。runtime 通过 `OR_SIM_CPU_EXCLUDE` 避开这个核。
- **ampere 延迟呈双峰分布**：不固定 CPU 时，gpt2_p64 的单次延迟在约 640 ms 和约 920 ms 两个值之间跳动。之后 runtime 改为从启动起就固定在一个物理核上，入口是节点注解 `mig.or-sim.io/runtime-cpu-pool`。
- **过期的 `or-sim.io` 扩展资源**：slot-device-plugin 增加了自动清理，并配置了对应的 RBAC。
- **kubelet 的镜像 GC 删掉了 busybox**：为 runtime 镜像和 busybox 各建一个 holder pod，保证 R1 不会冷拉镜像。
- **registry 修复死锁**：planner 在需要 repair 时会跳过规划，而 epoch-controller 的副本数是 0。临时把 epoch-controller 扩到 1，完成 repair 后再缩回 0。

### 5.2 executor / router / node-agent 审计后的修复
- **executor**
  - HTTP 请求加超时；
  - 同一 GPU 的 MIG 操作按 GPU 互斥，并增加 mig-devices / binding 两类资源占用；
  - 按 GPU 预留资源，避免后续动作长期拿不到资源；
  - `delete_instance` 先删路由再删 Deployment；
  - 按 replica 用 PATCH 更新路由，排空时只查询这个 replica；
  - 只在需要时刷新 CDI，`place_instance` 从 3.6–14.7 s 降到 0.3–0.8 s；
  - 按节点注解给 runtime 分配 CPU。
- **router**：带版本号的路由持久化；`PATCH /control/routes`；`?runtimeId=` 过滤；每个 replica 的并发上限；由 replica 主动拉取来组 batch。
- **node-agent**：`clear`、`apply-slots`、`patch-slots` 按每块 GPU 加 flock。
- **Stage 3 的 SW−C 变体**：同时删除 capacity-gate 边和临时容量清理边，动作集合不变。

### 5.3 被取代的 E1 run（已从仓库删除）

| run | 问题 | 修复 |
|---|---|---|
| `20260928T201926`（SliceWise） | router 允许一个 replica 同时跑多个 batch，resnet50 单个 batch 要 587 ms（6.8 rps）；CDI 刷新被串行执行 | 每个 replica 的并发上限；只在需要时刷新 CDI |
| `20260928T205256`（SliceWise）/ `211026`（SW−C） | 发送端的 Python GC 让所有线程停顿，最长约 1 s，越往后越长，vision 最低比值被拉到 0.15–0.34；LLM runtime 每个请求多做一次 prefill，TTFT 被抬高（llama TTFT 几乎全部超 SLO）；runner 记录的 ledger 缺少 profile，变换中容量不更新 | 发送端每秒调用 `gc.freeze()`，最长发送间隔降到约 0.1 s；TTFT 改为在 `generate()` 内部标记；分析时根据路由快照和事件时间重建 ledger |
| `20260928T213056` / `214830` | router 每个 replica 一次只发一个 batch，GPU 在约 1.5 ms 的往返期间空闲；vgg16 稳态只完成 84–98%，被拒请求 20838 个 | vision 流水线（depth 2），runtime 推理串行执行 |
| `20260929T000200` / `002006` | 流水线打开后，小 batch 的 GPU 时间 +43%（vgg16 b1 9.2 → 13.3 ms）。定位到根因：`ThreadingHTTPServer` 为每个请求新建一个线程，推理跑在新线程上，而 PyTorch 在新线程上第一次调用 CUDA 时要建立线程级状态，这段开销落在计时区间内（vgg16 b1 2g：固定线程 4.18 ms，新线程 10.4 ms）。旧 catalog 也因此偏低，小 batch 最多被低估 4.8 倍 | 所有 CUDA 调用放到一个固定线程上执行，并重测 catalog |
| `20260929T023306` / `025230` | catalog 中 LLM 行仍是旧 runtime 的测量值；为验证 catalog 做的补测 | LLM 用新 runtime 重测，建立 `profile/newest`；补测 unfit 的 option 和 vgg16 b1 |

诊断过程中还确认了以下几点：
- **SM 时钟和功耗都不是 +43% 的原因。** ampere-gpu0 满载时约 250 W，出现 SW power cap，SM 时钟为 1320–1350 MHz；关闭流水线时时钟更低，GPU 时间却正常。
- **CPU 频率与此无关。** 两种状态下都是约 2.0 GHz。
- **请求体大小和连接方式无关。** 请求体约 250 B，runtime 是 HTTP/1.0，每个请求一个连接。
- **结论：只有"每个请求一个新线程"会造成这个开销。**

### 5.4 容量余量 h（最终不使用）
- **实现**：`capacityHeadroom` 让 planner 按 (1 + h) × 需求规划。超出 GPU 预算时，用二分查找把 h 降到最大可行值。Stage 3 和发流器仍用原始需求。代码保留，默认关闭。
- **离线扫描**：`headroom_offline_sweep.py`，结果在 `analysis/headroom_offline_sweep.csv`。
  - 固定 h 时，R1 在 h > 2.8% 就需要 4 块 GPU，5% 已经放不下。
  - 按轮自适应降 h 时，5–25% 的 12 轮都能放进 3 块 GPU。R1 实际用 2.8–2.9%，其余各轮用满 h。
- **决定**：不加 h，最终 run 只打开 `conservative3gMu`。

### 5.5 已知局限（最终 run 中仍然存在）
- **ampere 多切片同时运行时，小 batch 比隔离 profile 慢。**
  - 各 option 比 catalog 慢了多少：resnet 1g b1 +25–34%，vit 1g b16 +6–7%，vgg16 +2–4%。同样的 option 在 rtx1 上与 catalog 一致。
  - 原因推测：ampere 是 Dell R750xa，操作系统没有加载 cpufreq 驱动，BIOS 自行管理频率；实测 CPU 一直停在 ≤2.0 GHz，没有 turbo，C6 开启；同时 A100 触及 250 W 功耗上限。满负载下的 CPU 频率还没有测，BIOS 也没有改。
  - 影响：resnet 被放在 ampere 的 1g b1 时（R4、R11），稳态只完成约 81%，这两轮的 resnet 请求有一部分被发送端的 pending 上限拒绝。
- **LLM 的 catalog 值比 `profile/20260929` 低 0–5%**（gpt2 为 −1.6% 到 −5.0%），原因没有进一步分析。
- **SW−C 的风险**：删除容量依赖边后，旧 replica 会在新 replica 就绪前被撤下，出现短暂的 404。这是 SW−C 设计上的结果，也是它作为反例要展示的内容。

## 6. 文件

- `cluster_results/<run>/`
  - `plans/`、`snapshots/`：每轮的 plan 与快照；
  - `requests.csv.gz`：压缩后的请求记录，原始 `requests.jsonl` 没有提交；
  - `analysis/`：`language_transition_table.csv`、`vision_timeseries.csv`（用于画图）、`ledger_capacity_per_second.csv`、`min_ratio.csv`、`s46_*.csv`；
  - `strict_runtime_audit.md`。
- `live_runner_20260926.py`、`live_traffic_20260926.py`：runner 与发流器。
- `e1_analyze.py`、`s46_analyze.py`、`compact_requests.py`：分析脚本。
- `headroom_offline_sweep.py`：离线扫描。
- `e1_smoke_20260929.py`：冒烟测试。
- `profile/newest/`（仓库根目录下）：catalog 的测量数据与生成脚本。

## 7. 本次清理删除的文件

以下文件都可以在 git 历史中找回：

- **被取代的 E1 run（9 个）**：`20260928T201926`、`205256`、`211026`、`213056`、`214830`，以及 `20260929T000200`、`002006`、`023306`、`025230`。第 5.3 节记录了每个 run 的问题。它们本地的原始 `requests.jsonl` 也一并删除了（这部分没有提交过，无法从 git 找回）。
- **中间 catalog**：`catalog_20260929_median_min.csv`、`catalog_20260929_worker.csv`。
- **09-28 的一次性调试脚本与笔记**：`run_ampere_smoke.py`、`ampere_smoke_build_20260928.yaml`、`ampere_*_20260928.md`、`smoke_inputs/`、`capture_live_failure_20260928.py`、`cleanup_live_r1_20260928.py`、`make_direct_batch_smoke_plan.py`、`refinalize_makespan_run.py`、`run_r13_cleanup.py`、`summarize_makespan_audit.py`、`audit_parallelism.py`。
- **已不再部署的镜像的构建清单**：`manifests/build/{cpuexcl-release-20260928,cpuset-release-20260929,e1c-release-20260929,ttft-release-20260929,newest-planner-release-20260929}.yaml`。当前部署的镜像，其构建清单都保留：`e1`、`e1b`、`pipe`、`worker`、`headroom`、`release-batch-state-concurrency`。
- **`planner_engine/exact_planner.py`**：没有任何代码引用，来源不明，之前已在本地被删除。
