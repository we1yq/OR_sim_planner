# Three-GPU Live Experiment Handoff

本目录是独立交接包。控制平面拉取仓库后从这里开始，无需另外复制原 eval 目录。
本包只包含执行规格和冻结输入，不包含已经实现的完整压测 harness，不会自动部署集群。

## 当前执行版本（v2）

- 只跑seed71一次完整12轮，不安排其他seed或Partial/Bridge局部重复。
- 稳定期按原profiling计时口径同时测实际replica，取消逐档并发扫描。
- 实例上下线时长按七个workload分别保存，GPT-2/Llama不同请求形状不合并。
- 控制平面只采集/汇总/回传数据，不生成图片、PDF或绘图脚本；绘图在用户当前主机进行。
- 已就绪环境下以约1–2小时得到首版数据为目标，安全检查和实际加载耗时不能隐去。

## 先读

1. `cluster_execution_runbook_20260926.md`：完整执行计划、测量协议、日志字段和停止规则。
2. `three_gpu_offline_screen_20260926.md`：本地离线筛选结果及边界。
3. 按手册先做环境/harness核查和冒烟；不能直接提交离线占位设备ID。
4. `SW_C_NEGATIVE_CONTROL.md`：SW-C 的显式开关、安全边界和执行命令。

## 正式输入

- `selected_demand.csv`：唯一用于这次实机序列的需求输入，12行、7个workload，单位req/s。
- `live_round`：实机R1–R12；第一行从空实验分配初始化。
- `round`：原trace的R4–R15；`hour`是原trace的相对小时，不是实机等待时长。
- 数值已经统一乘0.2，**不要再次缩放**，不要改成旧runner的三个workload简写。
- `catalog.csv`：84个SLO-qualified多batch serving options；按最终物理profile和batch查mu。
- `original_30min_demand.csv`：原48轮未缩放输入，仅用于核验来源。这里的原始round为0–47；正式输入round已转为1-based。

## 核查与溯源

- `selected_rounds.csv`：12轮离线source/target/峰值/动作汇总。
- `selected_action_coverage.csv`：16类primitive actions覆盖计数。
- `selected_physical_prefix_audit.csv`：所有拓扑前缀的物理GPU占用审计。
- `manifest.json`：离线运行来源、规划器commit e965f1118、solver参数和限制。
- `SHA256SUMS`：上述冻结数据与手册的校验和。

手册内原 `eval/...` 路径表示文件的生成来源；执行时使用本目录的同名文件。
manifest内Mac本地路径仅为溯源，不是控制平面的执行路径。真实规划器位于仓库的 `k8s-extension-go/planner-engine/app`。
原报告中的全量候选、其他scale目录和pickle未打包；不是本次实机运行所需输入。

Linux控制平面可在本目录执行：

```sh
sha256sum -c SHA256SUMS
```

## 三卡边界

两个worker共三张A100（2+1）。允许target用满三张，但全过程不得使用第四张。
参考target序列：3,3,3,2,1,1,1,1,2,1,2,2；末轮转换峰值为3、headroom为1。
真实设备绑定与等价最优解可能不同，需基于观测快照重新规划和审计。
离线覆盖Partial、Bridge及全部16类生成动作，未自然触发In-place；另做In-place冒烟。
本包没有实机通过结果，不能把离线结果写成真实流量或吞吐证据。
