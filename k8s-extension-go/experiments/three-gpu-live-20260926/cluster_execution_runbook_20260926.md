# 三卡集群执行手册 v2：单次连续实验、同口径 profiling、仅采集数据

更新：2026-09-26。本版取代原三 seed 全流程方案；控制平面 agent 以本版为准。
本文是执行规格，不表示 harness 已实现。禁止按旧计划继续安排额外重复或并发扫描。

## 0. 本轮范围和时间预算

- 只跑 seed=71 的一次完整12轮序列：1次初始化、11次非空源转换。
- 不跑 seed=113/197，不做代表性 Partial/Bridge 局部重复，不做 Poisson、SW-C、独立共置扫描。
- seed=71只决定开环发流的起始相位；Gurobi Seed=1始终不变，不改变demand。
- 稳定期沿用原profiling的推理计时方法，取消所有逐档并发搜索，不宣称测得端到端最大吞吐。
- 控制平面只实现/核查采集工具、执行实验、导出原始数据和表格/文字报告。不要画图，不要安装绘图库，不生成PDF/PNG/SVG或绘图脚本。
- 数据回到用户当前主机后，由当前主机另行分析和画图。不要把绘图纳入集群耗时。
- 目标是在环境和harness已就绪的前提下约1–2小时得到首版数据，不是完成保证。
- 已在跑的安全转换正常完成，不强制中断；复用同输入、同代码和相同测量口径的已完成数据，并标记沿用来源。旧并发压测数据不能冒充新版profiling。
- 记录总耗时与每轮耗时；预估将超过2小时及时报告剩余任务，不擅自缩短测量、删工作负载或绕过drain。

预算参考：环境/复用冒烟10–15分钟；11轮source对照11分钟；12轮target稳态6分钟；12轮profiling至少12分钟；预热/排空/规划/真实转换和数据导出占余量。模型加载、慢请求或缺失harness可增加时间，必须如实报告。

## 1. 冻结输入与版本

所有正式输入在本手册所在目录 k8s-extension-go/experiments/three-gpu-live-20260926/：
- selected_demand.csv：唯一正式需求输入，12行，七个workload，单位req/s。
- catalog.csv：84个SLO-qualified多batch选项。
- selected_rounds.csv、selected_action_coverage.csv、selected_physical_prefix_audit.csv：离线核查参考。
- manifest.json：离线溯源，里面的Mac临时路径不是集群执行路径。
- original_30min_demand.csv、three_gpu_offline_screen_20260926.md：来源与筛选记录。
- SHA256SUMS：传输后核对，不自行重新生成demand。

需求已乘0.2，不能再次缩放。selected_demand.csv的live_round=1..12；round=原trace R4..R15。
第一行从空实验分配初始化，不需要先执行原R1–R3。原hour不是实机等待时间。
离线target GPU数为3,3,3,2,1,1,1,1,2,1,2,2，仅作参考，不强制不同机器的等价最优解一致。

保留七个独立workload key：
resnet50_image、vgg16_image、vit_base_image、gpt2_p64_o64、gpt2_p512_o512、llama_p1024_o128、llama_p2048_o64。
不能合并GPT-2或Llama的请求形状，不能改成旧runner的三个模型简写。

正式planner基准commit为e965f1118；实际运行HEAD、diff和镜像digest另行保存，有后续修复则重新核查。
两个worker共三张A100（2+1）；target可用三张，但转换全过程不能用第四张。
所有物理ID必须来自真实观测；不能提交离线pickle中的占位ID。
不修改catalog、SLO、batch菜单、请求形状或需求比例。不得清空其他用户的资源。

## 2. 开跑前的必要核查

1. 确认测试床独占权、namespace、context、真实GPU库存与健康状态。
2. 记录控制平面和两个worker的CPU/内存/OS/kernel、GPU型号/显存/UUID/MIG模式、驱动/CUDA/Kubernetes/GPU Operator/device plugin/container runtime版本。
3. 保存planner、executor、router、MIG agent、runtime镜像digest，核对七种请求类型在两个节点的部署能力。
4. 核查原profiling入口、模型/精度/预处理、batch、token长度、warmup和计时边界。在readiness.md写出实际执行命令和源码位置，不能根据名称猜测一致。
5. 日志需有动作、请求、replica生命周期和路由/batch生效事件，不能只采集日志末尾若干行。
6. 先核查可用的同版本冒烟记录。缺失关键就绪/batch/设备释放证据时补最小安全检查；不为了增加策略覆盖额外构造整套实验。
7. In-place未在离线片段中触发。若无已有验证，报告未覆盖，不为本次新增独立In-place实验；发现运行必要的安全前置条件未满足则停止，而不是冒险执行。
8. 三卡离线预检使用真实设备库存，target<=3、所有拓扑前缀峰值<=3、容量依赖审计通过。每轮实际执行前刷新source，状态不符时作废旧计划并重规划。

readiness.md列出ready/blocker及实际harness入口。旧eval/3gpu_test/real_3gpu_k8s_experiment.py仅供参考，不能默认符合本版协议。
原profiling可能使用独立脚本/Pod；不能直接运行会重配或独占设备的旧profile matrix。应复用计时逻辑，在已部署target的replica内测量，保持布局不变。
缺失必要工具先补齐并测试，时间不足应报告，不静默替代测量方法。

所有主机记录时钟同步offset，目标绝对值<=10ms；跨主机时序保留误差。同进程耗时用monotonic，事件用UTC时间。
Stage1/2 Threads=8、Seed=1、MIPGap=0，仅OPTIMAL；本轮不与其他规划压测并行。无新增求解时限，遇耗时异常报告。
环境/日志不能包含license token、密码或访问密钥。

## 3. 发送与请求规则

转换和稳态服务测试均经正式router，开环按预设时间发送，不等待上一请求完成。
profiling是独立阶段，可在实际replica内部计时，不经过路由的测量必须标明范围。

固定间隔d>0：offset=phi+n/d，phi由seed71与workload稳定hash在[0,1/d)生成。
每轮source control与transition使用同一发送文件、payload、offset，attempt ID区分阶段。
d=0不发请求；不提高LLM最低流量。语言使用与catalog一致的prompt/output长度和tokenizer，视觉输入及预处理一致。
保存payload hash、实际token数；一张图片/一条生成请求为一个逻辑样本，不把一整个batch当成一个样本。

单请求超时900秒，执行watchdog1800秒；它们是异常上限，不是每轮固定等待。
客户端自动重试关闭，服务端/执行器重试需单独记录。
客户端pending安全上限每workload256、总计1024。达到上限停止新增并标measurement_invalid，不静默限速变闭环。
记录scheduled_send/actual_send；漏发即质量异常。p99发送lag需<=max(10ms,min(100ms,0.05/d))；超出则保留结果并标发流器异常。
不因吞吐低、失败或容量不达标而删结果或换seed。

请求计数：
N_pending=N_sent-N_success-N_failure；超时属于failure，完整响应才算LLM success。
N_pending包含正常在途请求，不叫排队。已有router/runtime队列计数可原样保留，但本轮不新增排队/路由算法分析。

## 4. 单次12轮执行状态机

### live R1：初始化

从空实验分配执行第一行target，保存初始化动作和峰值，核验实际target。
目标就绪后按D_new开环运行30秒，停止新请求并排空，再执行第5节profiling。
初始化承诺为0，不计算容量比，不计入11次非空源转换；仍计入收敛、动作和稳定期容量统计。

### live R2–R12：每轮固定顺序

1. SOURCE_CHECK：确认上一轮profiling结束、请求排空、布局及batch未变化。上一轮30秒target稳态已承担旧需求测试，不再额外重复120秒旧需求段。
2. SOURCE_CONTROL：保持source不变，以commitment=min(D_old,D_new)发送60秒。记录相同发送文件的source对照，勿触发规划执行。
3. DRAIN：停止新增，等待本阶段请求终结及可观测队列排空。正常为空时立即继续；超过900秒停止本次运行，记录未完成请求。
4. PLAN_VALIDATE：从真实source规划target/DAG，保存规划时间；检查容量依赖和三卡上限，未经通过不执行。
5. TRANSITION：coordinator同步释放开环发送与executor；承诺发流从首个动作之前/同时开始。保存barrier和首动作时间；已进入该阶段的请求不能丢弃。
6. TARGET_REACHED：全部动作完成且独立状态核对通过才结束转换；立即切换D_new发流30秒。转换遗留请求继续追踪，用phase区分新旧请求。
7. DRAIN：停止新增并排空，保存全部请求终态。
8. PROFILE_TARGET：执行第5节；结束后停止profile worker，确认实际batch/layout未变化、无遗留请求。
9. 保存最终快照和逐轮汇总，再进入下一轮。

SOURCE_CONTROL是配对的单次测量，不是额外Partial/Bridge局部重复；每次转换只执行一次。
若转换超过60秒，承诺发流按相同规则继续到结束。source配对范围仅前60秒，尾部明确标unpaired，不重做转换或恢复source补测。
遇动作失败、目标不符或1800秒watchdog，不进入下一轮。安全停止新请求并保全状态，不能直接重放整个部分执行的DAG。
不存在“跑完再多一个seed”“有余量追加局部重复”的步骤。

## 5. 稳定期：沿用profiling口径验证实际容量

目的：在最终target实际共置条件下，验证各workload的推理容量是否达到catalog预测及目标需求。
不是端到端吞吐极限搜索，不做并发加倍扫描，也不把模型内计时误称HTTP完成吞吐。

### 固定方法

- 保持最终target的GPU、MIG、replica、batch不变；不重新部署profiling专用副本，不遍历未选中的catalog配置。
- 停止开环服务测试，确保遗留请求排空。每个实际replica只启用一个连续benchmark worker；所有replica同时测量。
- 沿用原profiling同样的输入生成、精度、batch、token长度、warmup、CUDA同步/计时方法。将具体参数写入profile_protocol.json。
- 不用旧累计runtime均值直接当本轮样本；重置本轮测量窗口或按原始样本计算。
- 每个replica先执行原profiling规定的warmup次数。全部完成后通过统一barrier开始60秒采样。若原预热参数无法查证，列blocker，不随意声称“同口径”。
- worker连续执行完整batch，中间不主动sleep；只在计时范围内测原profile定义的推理/生成过程，排除客户端排队和路由耗时。
- 60秒到达后不启动新的batch，允许已开始的batch完整结束，保存超出窗口的实际结束时间。所有worker完成后才能进入下一轮。
- 保存每个完整batch的起止、逻辑样本数、纯推理时间、实际profile/batch及错误。不要用低于一个完整batch的截断时间计算吞吐。
- 所有replica必须在共同采样区间活跃，记录起止偏差、设备利用率（可用时）和worker间空隙。空隙显著或worker饥饿则标测量质量问题，不宣称已饱和。
- OOM、GPU错误、runtime退出等原样保存并停止后续轮次，不能换batch隐藏问题。
- 60秒内没有完整样本则填insufficient_samples，不把缺失写0，也不自动加长或换输入。
- runtime无法在不改变target的条件下执行同口径profile时，先修测量接口或报告阻塞，不改用并发扫描替代。

### 计算

mu_measured_replica = sum(完整batch逻辑样本数) / sum(这些batch按原profile边界计的推理秒数)。
对于固定batch，等价于batch_size/平均batch推理时间。原profile若使用其他聚合，先核对并记录差异，不能混用。
每个workload：
C_pred=sum(最终各replica对应catalog_mu)；
C_measured=sum(本轮同时测量各replica的mu_measured_replica)；
D=本轮目标需求。
报告C_measured/D、C_measured/C_pred；分母为0记null。缺失replica样本时整体标不完整，不能只加成功者后称完整容量。
另存每replica完整样本数量、计时总和、观测墙钟跨度、失败数、实际batch，便于主机复算。

这验证的是profile口径的推理容量，不包括网络、路由、排队；不能称“实测端到端最大req/s”。
吞吐>=D和>=C_pred分别记observed_met_demand和observed_met_prediction，没有默认95%宽限。
只跑一次，按逐轮实际值报告，不制造跨运行误差线或三次中位数。LLM样本少必须给出样本数。

## 6. 实例上下线：必须按workload分开记录

GPT-2和Llama的不同请求形状分别统计，不能只按vision/LLM两大类或全部instance混算。
每条记录含workload、family、runtime_id、Pod UID、node、GPU UUID、MIG UUID、slot、physical_profile、batch、round、action_id、attempt。

上线事件：
- deployment_create_started_at
- deployment_created_at
- pod_ready_at
- model_cuda_verified_at
- route_activation_ack_at

下线事件：
- route_stop_accepting_effective_at（未知时同时存请求/ACK区间）
- drain_started_at
- drain_completed_at
- pod_delete_started_at
- pod_gone_confirmed_at

分开导出创建/就绪等待、路由激活、drain、删除，以及整段上线/下线耗时。
整段上线=create_start→route_active；整段下线=stop_accepting→pod_gone。
batch原地更新单列apply/verify耗时，不当作新Pod上线；不适用字段填null。
某动作没有workload（整卡MIG配置等）标GPU-scope；另存受影响workload列表，不能将同一GPU动作伪复制为每workload独立样本。
事件缺失填null并记录原因，不能拿动作结束时间冒充就绪/路由生效时间。
各workload×action汇总样本数、中位数、最小/最大值；这里是本次运行内多实例动作的统计，不是跨重复统计。
所有失败尝试和重试等待保存；成功耗时统计标明仅成功attempt，端到端转换耗时包括实际失败/等待。

## 7. 转换期间容量核算与辅助请求记录

承诺d_transition=min(D_old,D_new)。
账本为已就绪且可接收新请求replica的catalog_mu之和；不是实时实测吞吐。
新replica ready且路由生效后加容量；旧replica停止接收新请求时扣除，即使仍在drain。
batch生效替换旧mu，不重复加整份容量。ready丢失或endpoint异常退出也记录。
优先实际生效事件；只有请求/ACK时间则记录区间，增容按确认后、减容按最早可能时刻构造保守账本。
未知事件标unknown；时间同步误差内的次序无法确定时标不确定，不能随意填充成0违规。
输出每workload最低容量比、低于承诺的区间/持续时间及不确定区间。承诺0的比值为null。

请求只作服务异常的辅助证据：记录发送准时性、成功、失败、超时及遗留请求最终状态。
不将pending>0判为违反承诺，不以无失败单独证明容量安全。
本轮不分析路由选点策略或排队机理；现成queue/inflight数据可保存供排查，不新建这方面实验。
固定source对照用于保留相同发送条件的参考，不输出排队差值主结论。
所有时序数据完整回传，由用户主机决定分析与画图；控制平面不画任何图。

## 8. 必须导出的数据

所有表包含run_id、live_round、trace_round、phase、traffic_seed=71；若保留repeat字段固定为1。
UTC时间含精度和来源；同进程duration保存monotonic值。不要上传凭据。

| 文件 | 必需内容 |
|---|---|
| environment.json | 硬件/软件/镜像/commit/diff、设备拓扑、输入hash、时钟offset、求解参数 |
| readiness.md | 前置核查、已复用冒烟及版本、实际执行命令、工具实现与限制 |
| profile_protocol.json | 原profiling入口/版本、模型精度、请求形状、batch、warmup、CUDA同步、计时与聚合定义 |
| planned_requests.csv | workload、sequence_id、payload_hash、发送offset、rate、seed |
| requests.jsonl | 唯一attempt_id、sequence_id、workload、scheduled/actual_send、first_token、completion、status/error、实际token/样本数 |
| actions.jsonl | plan_id、action_id/type、attempt、依赖、start/end/status、workload或GPU-scope、设备/slot、错误 |
| replica_lifecycle.csv | 第6节全部身份与上线/下线事件、时序缺失标记 |
| action_runtime_by_workload.csv | workload×action的成功/失败样本数、成功中位数和范围；batch更新单列 |
| runtime_events.jsonl | ready/unready、route生效/撤销、batch生效及请求/ACK区间 |
| gpu_events.jsonl | 物理GPU获取/释放实际时间、owner与关联action |
| snapshots/ | 每轮前后原始资源、registry、GPU/MIG/Pod/route/batch和规范化diff |
| plans/ | source/target/demand/DAG、规划时间、容量与三卡审计 |
| profile_samples.csv | 每replica每完整batch的样本数、纯推理耗时、起止、实际配置与错误 |
| capacity_results.csv | 每轮每workload的D、C_pred、C_measured、两个比值、样本数、完整性与单位 |
| capacity_timeline.csv | 实际事件重建的catalog容量、commitment、比值、事件来源、不确定区间 |
| transition_requests_summary.csv | 每轮每workload每阶段的发送/成功/失败/超时/pending、发流质量 |
| round_summary.csv | reached_target、makespan、规划时间、动作/策略计数、GPU峰值/headroom、失败和数据质量 |
| results.md | 单次运行完整结果和限制，不写英文正文，不嵌入新图 |

makespan=首动作开始至全部动作及实际target核验完成；提交至完成的端到端耗时另列，不混入稳态/profile时段。
物理GPU占用从获取生效至return成功，不能在route撤销或clear_binding时提前扣除。
headroom=max(0,peak_reserved-max(source_gpu_count,target_gpu_count))。
保留实际出现的全部动作类型；不能用离线229个动作或16类覆盖填代真实结果。

## 9. 停止规则与结果边界

- 发现他人资源、缺失模型/关键接口/日志、三卡计划不通过：不启动正式执行。
- 动作失败、设备错误、target不符、无法排空：停止下一轮并报告，正常安全收尾，不硬删在途实例。
- 发流器漏发/滞后、测量worker饥饿、事件丢失：标对应数据质量，保留原始记录；不得把异常样本删除后声称成功。
- 性能低于预测/需求本身是结果，不以此重新挑选输入或seed。
- 修改算法或runtime后换版本标签；已采数据不伪装同版本全序列。
- 没有自动追加第二次全流程、局部重复、Poisson、SW-C或共置实验。
- 单次连续实验不声称统计稳健性。全部成功时为12/12执行收敛，其中11次非空源转换；成功数必须来自实际记录。
- 动作覆盖、策略覆盖、故障分支覆盖是不同概念。未触发In-place或某类动作就报告未覆盖。

## 10. 控制平面交付与回传

输出到cluster_results/<UTC-run-id>/，原始日志不能覆盖；保存采集harness和精确运行命令。
生成上述CSV/JSON/JSONL和results.md及SHA256SUMS，完成行数/单位/ID关联/缺失字段检查。
把原始数据和报告打包供当前主机获取；告知实际绝对路径、文件大小、校验和及传输方式。
结果较大时日志压缩，但不能只回传汇总或只回传截图。
控制平面不生成图片、PDF、图形预览或绘图脚本，不安装绘图库，不把画图列为完成条件。
当前主机取得完整数据后另行绘图。本次禁止未经请求push大型结果、实验日志或修改论文。
