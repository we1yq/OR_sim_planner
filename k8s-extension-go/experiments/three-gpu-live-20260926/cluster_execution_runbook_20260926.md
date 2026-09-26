# 三卡集群执行手册：转换承诺与实际容量验证

版本：2026-09-26。执行对象：控制平面上的 agent。本文是执行规格，不表示下述 harness 已实现。

## 0. 任务边界与不可自行修改的事项

1. 集群两个 worker，共三张 A100，分布为 2+1；不得使用其他业务的 GPU。
2. 基准代码提交 e965f1118。先核对 HEAD、工作区差异及运行镜像 digest；有后续修复时记录 diff 并重新离线验证，不自行 reset 用户改动。
3. 本轮测 SliceWise，不运行 baseline，不重跑论文离线实验，不修改正文。
4. 允许 target 使用三张；所有转换过程中实际占用也必须不超过三张。不固定扣除一张备用 GPU。
5. 不修改 catalog、SLO、batch 选项、模型请求形状或某一类 workload 的相对需求。
6. 禁止把离线 pickle 中的占位物理 ID 提交给集群。正式计划必须来自真实观测快照。
7. 先检查集群独占权。发现其他用户的实例或不明占用，停止并报告，不能清空它们。
8. 自动失败重试不得改变统计口径。保存每次失败、重试与恢复，不能只保留成功尝试。

## 1. 输入交接

以下路径均相对于仓库根目录。只拉取 k8s-extension-go 的控制平面未必有 eval 文件，缺失时先从本地工作区传递这几个文件；不得用旧 runner 内置 trace 代替。

- eval/results/three_gpu_screen_20260926_v2/selected_demand.csv
- eval/results/three_gpu_screen_20260926_v2/catalog.csv
- eval/results/three_gpu_screen_20260926_v2/manifest.json
- eval/results/three_gpu_screen_20260926_v2/selected_rounds.csv
- eval/results/three_gpu_screen_20260926_v2/selected_action_coverage.csv
- eval/reports/three_gpu_offline_screen_20260926.md
- 本执行手册。

在控制平面计算并保存所有输入的 SHA256，交接双方核对。不要重新生成或四舍五入 demand。

输入为 §4.3 的 30 分钟窗口峰值需求，七个 workload 统一乘 0.2，连续选择原 trace R4–R15。
selected_demand.csv 的 live_round=1..12 为本次编号；round=4..15 为原 trace 编号。
原窗口为相对时间 [1.5h,7.5h)，实机不等待半小时推进，按本手册的测量阶段推进。
live R1 从空集群初始化，live R2–R12 为 11 次转换。
离线 target GPU 数为 3,3,3,2,1,1,1,1,2,1,2,2；这是参考，不强迫同机以外的等价最优解逐位一致。

workload 必须保留以下七个独立 key，不能合并成 gpt2/llama：

- resnet50_image
- vgg16_image
- vit_base_image
- gpt2_p64_o64
- gpt2_p512_o512
- llama_p1024_o128
- llama_p2048_o64

每个 replica 的预测吞吐按 (workload, 最终物理 profile, batch) 从冻结 catalog 精确查找。
缺失、重复或单位不匹配均停止；不能把逻辑 3g 的 mu 用于物理 4g。

## 2. 先交付运行能力，再执行实验

旧 eval/3gpu_test/real_3gpu_k8s_experiment.py 只能作为实现参考：它含旧 workload 简写和旧流量流程，不能直接当成本协议已实现的入口。
参考正式实现：k8s-extension-go/cmd/transition-executor/main.go、cmd/cluster-state-manager（以仓库实际路径为准）、cmd/model-runtime、planner-engine/app。

执行 agent 先实现或核查以下功能，并生成 readiness.md，列出实际入口命令、API/CRD 名称、日志位置和未满足项：

| 功能 | 必须具备的行为 |
|---|---|
| 计划与执行分离 | 可以先产生/检查 DAG，再批准执行；对照期间不能自动执行待测转换 |
| 实际状态读取 | 查询真实 GPU UUID、MIG UUID、slot、workload、batch、Pod UID、route endpoint、就绪状态 |
| 开环发流 | 按保存的相对时间发送，不等待完成，不因拥塞自动减速 |
| 配对重放 | source 与 transition 使用相同请求内容、相同发送偏移、相同逻辑请求编号 |
| 饱和模式 | 与开环分开，实现第 7 节的并发扫描和固定并发测量 |
| 请求追踪 | 每次尝试有唯一 ID，可关联发送、终态及客户端/服务端事件 |
| 行为日志 | 记录完整 action event，不是只抓最后若干行日志 |
| 队列观测 | 有 router/runtime 的 queued/inflight 则采集；没有则标 unavailable，不用客户端 outstanding 冒充 |
| 执行失败 | 失败停止后续轮次，保存状态；不得在部分执行后盲目重放整个 DAG |

若必要功能缺失，先补 harness 与单元测试，不能跳过后宣称已验证。产出真实可运行的命令，不猜测不存在的 CLI。

## 3. 环境记录、权限和时间

- 记录 namespace、kubectl context、控制平面与 worker CPU/内存/OS/kernel、GPU型号/显存/UUID/MIG模式、驱动、CUDA、Kubernetes、GPU Operator、device plugin、container runtime 版本。
- 记录 planner/controller/executor/router/MIG agent/model runtime 的代码版本和镜像 digest，保存实际 Gurobi 参数。环境变量中的凭据不要写入报告。
- 确认两节点均有七种 workload 所需的镜像和模型文件；确认 runtime 接受对应 prompt/output 长度和 batch。
- 记录设备 UUID 到节点的映射、其他资源约束、当前占用和健康状态。所有请求经正式 router，而非绕过它直连 replica。
- 负载生成器最好在非 GPU worker 的独立主机；若运行控制平面，记录 CPU/内存占用并单独验证它不成为瓶颈。
- 所有主机启用时钟同步，记录开跑前后的 offset；目标绝对 offset <=10 ms。未达到时不做毫秒级跨主机事件先后结论。
- 同进程持续时间用 monotonic clock；跨主机关联使用 UTC 纳秒时间和事件来源，并保留时间误差。
- 正式 solver 使用 Threads=8、Seed=1、MIPGap=0，Stage 1/2 无求解时限，仅 OPTIMAL。不得在本轮与其他规划压测抢 CPU。

## 4. 冒烟与真实三卡预检

依次验证：batch 增大/减小、Partial、In-place、Bridge。每个从已知状态开始，完毕清理本次资源。
batch 检查 Pod UID 是否保持、实际 batch、预测容量更新；不能只看 HTTP 200。
Bridge 至少一个跨节点案例，验证目标路由生效、旧源释放、物理绑定最终正确。
每例记录实际动作覆盖、DAG valid、reached_target、容量前缀审计、峰值物理占用。
若三卡上构造不出某种安全案例，报告未覆盖，不能强行忽略容量保护。

用真实空闲设备清单重新离线规划推荐 12 轮；初始 source 为空实验分配，GPU 库存仍为三张。
每轮 target<=3、全过程峰值<=3、容量前缀审计通过、真实节点约束满足才接受。
如果真实状态产生不同 placement 可以保留，但重新检查覆盖；若计划失败，停止并诊断，不静默换需求/换窗口。
每次实际执行前再次刷新 source；如与计划 source 不一致则该计划作废，重规划并记录。

## 5. 发送器统一规则

### 5.1 请求形状

视觉使用与 profiling 相同预处理及固定有效图片，保存内容 hash。
语言使用固定的对应 prompt 长度、输出长度、tokenizer 与生成参数，校验实际 token 数；保存请求 payload hash。
所有 workload 同时发送，不能逐 workload 分开测来逃避共置影响。

### 5.2 开环模式

三个主重复的 seed 分别为 71、113、197。
固定间隔：workload 速率为 d>0 时，发送偏移为 phi+n/d；phi 从 [0,1/d) 由 seed 与 workload 稳定 hash 派生。
同一轮 control/transition 使用同一个已保存的发送文件，payload、offset 完全相同，attempt ID 不同。
d=0 不发流；不能为了制造请求加最小流量下限。每个 workload 的预计和实际请求数都要报告。
默认配对文件长度 300 秒。transition 只发送到执行结束；对照使用相同时间长度的前缀作分析。

单请求超时 900 秒，计划执行 watchdog 1800 秒。客户端自动请求重试关闭；服务端重试如存在必须追踪。
客户端 outstanding 安全上限每 workload 256、总计 1024；这不是调速并发数。
达到上限、发送线程阻塞或请求丢失时停止新增发送并标 measurement_invalid，记录原因，不悄悄变成闭环。
每个请求记录 scheduled_send、actual_send；任何漏发使该次测量无效。
发送抖动验收：p99 lag <= max(10 ms, min(100 ms, 0.05/d))，每 workload 分开检查。
超过阈值的原始结果保留为发流器异常，不作为干净的配对比较。

### 5.3 不能混淆的计数

N_sent：实际发出的请求尝试数；N_success：成功完成；N_failure：明确错误/超时；N_pending=N_sent-N_success-N_failure。
N_sent-N_success 还包含失败，不能称为排队；N_pending 包含正常执行中的请求，也不能称为 runtime queue。
分别记录 client pending、router queued、runtime queued/inflight；缺失项填 null。
LLM 的第一 token 不算请求完成，完整响应才计入完成数。

## 6. 每次主重复的精确状态机

三个重复串行执行，禁止相互抢资源。每次从空实验分配启动，全程记录事件。

### 6.1 live R1：初始化

从空实验分配规划并执行第一行 demand 的 target。
记录动作和峰值、验证目标，执行第 7 节容量测试；初始化没有非零旧需求，不做转换承诺流量比较。
R1 单列计入执行收敛和容量测试，不混入 11 次非空源转换的统计。

### 6.2 live R2–R12：每轮固定顺序

1. SOURCE_STEADY：在当前 source 上按旧需求开环发送 120 秒。保存完整指标。不能自动扩缩容。
2. RESET_TRAFFIC：停止新增请求，等待客户端 pending 和可观测队列归零，最多 900 秒。未排空则停止本重复并保存状态。
3. SOURCE_CONTROL：保持 source 不变，按 commitment=min(old,new) 重放保存的 300 秒序列。开始计时前确认无遗留请求。
4. RESET_TRAFFIC：停止发送并排空，要求同上；确认 GPU/layout/Pod UID/route/batch 未改变。
5. PLAN_VALIDATE：从当前真实快照生成 target+DAG，记录规划时间，完成容量与三卡峰值检查；不执行。
6. TRANSITION：发流器与 executor 由同一 coordinator barrier 释放。发流器从 offset=0 按同一文件发 commitment；记录 barrier 与首个动作开始时间。承诺在首个动作之前生效，禁止执行后才补发流。
7. TARGET_REACHED：以全部动作完成且独立实际状态核对通过为转换结束。停止 commitment 序列，切换到新需求，持续 120 秒。未完成的转换请求继续追踪，禁止丢弃。
8. RESET_TRAFFIC：停止新增并排空，检查全部请求终态。
9. CAPACITY_TEST：执行第 7 节饱和容量测试，然后排空；配置不得改变。
10. 保存轮末快照、所有产物及本轮质量检查，再进入下一轮。

SOURCE_STEADY → CONTROL 的速率变化与 drain 是测量边界，不属于待测转换。
控制阶段不发旧需求而发 commitment，是为了与转换段严格配对；旧需求已在 SOURCE_STEADY 单独测量。

如果转换超过 300 秒，发流器按相同固定间隔继续发送至结束，不能停在 300 秒。
此时保存完整转换数据，但配对主分析只用前 300 秒，并明确未覆盖的尾部；不把这次失败隐藏或冒充全程有对照。
若转换超过 1800 秒/动作失败，不执行下一轮。停止新请求、保存实际状态，按正常有序方式恢复，不绕过 drain。

## 7. 最终 target 的饱和容量验证

目的：测真实并发共置下的吞吐，分别比较 target demand 和预测容量。与转换流量试验分开。
对每个活跃 workload：C_pred=sum(catalog_mu of final replicas)，D=本轮 demand，n=实际 replica 数。
固定 GPU/MIG/replica/batch，不启用 autoscaler；所有活跃 workload 同时压测。

### 7.1 并发扫描（闭环，只用于容量测试）

使用每 workload 并发 c_i=n_i*2^j，j=0..7。相同 j 下所有 workload 同时运行，每个请求完成后立即补发。
单次请求与前文相同，记录实际 batch 和响应 token。不要在负载生成器做额外 batch 合并。
每级预热 30 秒、测量 60 秒；不同级之间停止新增并排空。记录每级吞吐、延迟、失败、资源使用。
候选平台条件：每个活跃 workload 最近连续两次加倍的吞吐增长均在 [-5%,+5%]，且无请求失败。
达到条件后停止扫描，选当前 j；这只是操作性平台判据，不是数学上的最大值证明。
若 c_i 超过每 workload 256 或总计 1024，则不执行该级，记录上限；若到 j=7 仍无平台，标 plateau_not_confirmed。
遇 OOM、GPU error、runtime 崩溃，保存失败，不将该级当零吞吐后取较优结果隐去。

### 7.2 固定级测量

若平台确认：在选定并发同时测 120 秒，预热 30 秒不计入。按该 120 秒内成功完成请求数/120 得到 C_measured。
若平台未确认：在最大无失败且可执行的并发级同样测 120 秒，标 observed_at_tested_concurrency，不写“最大容量”。
这里的闭环与转换开环是不同模式，在日志中强制标记。
采集每个 workload 完成数、失败、TTFT/TPOT或视觉延迟、实际 batch、queued/inflight、客户端负载。
报 C_measured/D 和 C_measured/C_pred，D=0 的前者为 null。
所有 C_measured 必须来自同一个同时发流测量区间，不能把不同并发级各 workload 的单独最大值拼成容量向量。

延迟/SLO仅作性能校验和边界：饱和闭环 throughput 达到预测不等于 SLO-qualified sustainable capacity。
本实验不声称求出了 SLO 下精确最大请求率；若要这项结论，应另做开环负载扫描，不能混同。
预先规定没有“95%就算达到”的宽限：实际测值>=D 或 >=C_pred 才分别记 observed_met_demand / observed_met_prediction。
三次重复逐次报告，再报中位数与范围；重复值跨阈值时写结果混合，不单凭中位数宣布全部达到。

## 8. 必须保存的数据表

所有表包含 run_id、repeat、live_round、trace_round、phase。时间字段使用 UTC ns，持续时间另存 monotonic ns。

| 文件 | 必需字段 |
|---|---|
| environment.json | 节点/GPU拓扑、版本、commit、镜像digest、输入hash、时钟offset、并发与超时 |
| planned_requests.csv | workload、sequence_id、payload_hash、offset_ns、模式、seed |
| requests.jsonl | attempt_id、sequence_id、workload、scheduled/actual_send、first_token、completed、status、error、token数、字节数 |
| actions.jsonl | plan_id、action_id、type、attempt、depends_on、scheduled/start/end、status、错误、节点、物理/MIG UUID、slot |
| runtime_events.jsonl | replica/Pod UID、workload、profile、batch、ready/unready、route add/remove ack、batch生效、事件发生及观测时间 |
| queues.csv | 每100 ms记录client pending；router/runtime按支持频率且至少每1秒采样，缺失标null |
| gpu_events.jsonl | UUID、allocation owner、获取/释放成功时间、关联action、source/target标识 |
| snapshots/ | 每轮前后完整raw Kubernetes资源、registry、MIG/replica/route/batch及规范化对比结果 |
| plans/ | source、target、demand、DAG、容量审计、实际设备三卡审计 |
| round_summary.csv | reached_target、makespan、规划时间、重试、失败、headroom、动作统计、数据质量标记 |
| capacity_results.csv | 并发级、平台判定、C_pred、D、C_measured、两个比值、请求数、失败、延迟、测量区间 |

动作日志不能只记录一个最终耗时：失败尝试与重试等待分别保存。
makespan = 最后完成/核验事件 - 第一个动作开始；另外报告提交至目标核验的端到端时间，不含本手册稳态/控制/压测阶段。
GPU占用从获取生效到成功return，不能在移出路由、清除logical binding或发送release命令时提前扣除。
headroom=max(0,peak_reserved-max(source_gpu_count,target_gpu_count))。

## 9. 容量账本和请求分析

### 9.1 容量账本不是实测容量

容量账本是已就绪、已路由 replica 的 catalog_mu 之和。
新实例 ready AND route生效后才加；旧实例不再接受新请求时扣除，即使仍在drain。
batch 生效后替换旧mu为新mu，不重复加整份容量。
优先用 runtime/router 实际生效事件。只有请求/ACK时间时保留生效区间：增容取确认后，减容取可能最早时间。
未知时间不能随意填 action end；不确定区间标 unknown，必要时输出上下界，不能宣称已经证明逐时刻实测安全。
ready丢失、endpoint异常退出同样需要扣除；只记录正常动作不够。
对承诺>0的 workload 计算容量比；承诺=0记null。
跨主机时间误差以内的短暂缺口标时序不确定，不自动当违规，也不自动抹掉。

### 9.2 配对流量主指标

以 barrier 为0，截取 source control 与 transition 相同持续时间（最多300秒）。保留两端在途请求终态。
每 workload 报请求数、成功/失败/超时、client pending峰值与面积(request-seconds)、排队峰值（如可得）、请求延迟摘要。
报转换减对照的 pending峰值和面积差值；不要用“pending>0”判断容量不足。
额外pending可能来自服务时长变化；持续增长也需结合发送准时性、稳定对照和恢复情况，不直接等同物理容量违规。
最长完成间隔仅在至少两次成功完成时计算，否则null；同时报样本数与发送间隔，不称其对低请求率不敏感。
固定10秒窗口画发送/完成吞吐作为辅助，保留部分窗口边界，不按结果调整窗口。
低请求率LLM不单独提高需求；完整报告样本数及证据不足，不用catalog账本替代实测证据。

## 10. 补充实验与授权边界

主实验三次完成后，可做Poisson补充：选择首次实际出现Partial的转换和首次Bridge转换。
从各自保存的source重新部署并核验；速率仍为commitment，seed=301和302，每例control/transition使用同一预生成Poisson序列。
各跑一次，只报告探索性结果，不能与三次固定间隔结果混为一个均值。
若状态无法精确恢复，停止该补充而不是换成另一个source。
SW-C会故意降低保护，本轮默认不执行；必须另获用户对独占测试床故障注入的明确授权。
不得用随机重启Pod或禁用其他资源/生命周期保护代替SW-C。

## 11. 数据质量与停止规则

- 前置条件缺失：不开始正式测量。
- 第四张GPU、重复物理占用、计划容量审计失败：不批准执行。
- 动作失败、设备错误、真实target不匹配、请求无法排空：停止本次重复，不开始下一轮。
- 发流器漏发/限流/显著滞后、事件日志丢失：标对应测量无效，保留原始数据；先修工具再做新的带编号尝试。
- C_measured低于预测、负载产生额外积压、请求失败：这是需要报告的结果，不能仅因此删数据或换seed。
- 修正式算法/runtime后必须换版本标签；旧尝试不合并成同版本三次重复。
- 禁止为追求0违规而修改mu、吞吐阈值、输入需求或丢弃慢请求。

## 12. 最终交付

输出目录统一为 cluster_results/<UTC-run-id>/，原始日志不可覆盖。
交付 readiness.md、执行命令与 harness、环境/输入manifest、全部逐轮原始数据、results.md、可重跑的独立绘图脚本。
报告至少包含：12轮初始化/转换区分、33次非空源转换的完成与失败、实际策略/动作覆盖、设备峰值、容量账本证据边界、配对流量结果、每个target的实测/预测/需求比较。
33次是三次重复全部成功时的计划样本数，不是预先填写的成功数。
单独列出In-place与batch/cross-node冒烟，不能混成trace中自然出现的覆盖。
图为独立PDF，不预先拼版；用现有论文风格。原始生成与绘图严格分离。
先完成环境与harness核查，汇报ready/blocker；只有全部必要前置项通过才执行三次正式序列。
