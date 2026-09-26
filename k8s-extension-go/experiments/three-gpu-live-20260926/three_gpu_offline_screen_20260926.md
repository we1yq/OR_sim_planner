# 三卡实机序列离线筛选（2026-09-26）

## 结论

推荐统一缩放 0.2，连续取原 trace R4–R15（相对小时窗口 [1.5,7.5)），导出为实机 R1–R12。
第一轮从空集群初始化，随后 11 次转换；不是先执行原 R1–R3 后再截取。
所有目标和所有拓扑前缀的已占用物理 GPU 数均不超过 3。
三卡可用时允许 target 使用三卡，不固定预留一张。最后一轮 target 为两卡、峰值三卡、headroom 为 1。
12 个计划均 reached_target，容量前缀审计均无 unguarded action。

## 输入与版本

- 正式 planner commit e965f1118，直接调用 k8s-extension-go 内 Stage 1/2/3；未修改正式算法。
- 使用 §4.3 的 30min demand.csv：每个窗口内一分钟请求率的最大值；不是窗口均值。
- 七个 workload 同乘 0.2；不删 workload、不改变顺序、不逐 workload 调参。
- 84 个 SLO-qualified 多 batch options；Threads=8、Seed=1、MIPGap=0，Stage 1/2 仅接受最优。
- 模拟两节点 2+1 共三块同型 MIG GPU，用占位设备标识；无 Kubernetes 连接、无真实部署。
- 每轮从实际模拟执行结果构造下一轮 source，并重新建立完整空闲设备清单与路由快照。
- 为检查 drain 动作覆盖，假设每个旧 replica 有 1 个在途请求、0 个排队请求；不代表实测。

## 筛选方法

扫描固定八个缩放值。每个缩放从空集群开始沿原顺序执行；失败时明确断开链，下一轮作为新的冷启动。
枚举每个成功连续段内长度 10–12 的窗口，不拼接失败两侧。
本次探索后采用：优先覆盖所有 primitive actions 且含 Partial/Bridge，优先 12 轮，再取扫描中最大的缩放和最早窗口。
这是探索性筛选，不声称是事前注册规则；全量候选和失败记录均保存。
上升/下降初筛按需求和判断，实际 selected GPU 也有 3→2→1 和 1→2 的变化。

| 统一缩放 | 有效轮数/48 | 备注 |
|---|---|---|
| 0.025 | 48/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 0.05 | 47/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 0.1 | 44/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 0.15 | 39/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 0.2 | 31/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 0.3 | 18/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 0.5 | 10/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |
| 1.0 | 2/48 | 失败后中断，下一轮重新初始化；不能拼接成完整轨迹 |

0.025 倍可完成全部 48 轮，但许多轮没有任何动作，且负载更低；不为动作覆盖首选。
失败既包含目标最少 GPU 超过三张，也包含有限设备下启发式构造失败。后者不能表述为理论不可达。
初版 three_gpu_screen_20260926 的空闲池/运行时快照沿用不完整；仅 v2 是有效结果。

## 推荐片段逐轮结果

| 原 trace 轮 | 实机轮 | source GPU | target GPU | 峰值 GPU | headroom | 动作数 |
|---|---|---|---|---|---|---|
| 4 | 1 | 0 | 3 | 3 | 0 | 40 |
| 5 | 2 | 3 | 3 | 3 | 0 | 9 |
| 6 | 3 | 3 | 3 | 3 | 0 | 8 |
| 7 | 4 | 3 | 2 | 3 | 0 | 42 |
| 8 | 5 | 2 | 1 | 2 | 0 | 33 |
| 9 | 6 | 1 | 1 | 1 | 0 | 8 |
| 10 | 7 | 1 | 1 | 1 | 0 | 8 |
| 11 | 8 | 1 | 1 | 1 | 0 | 4 |
| 12 | 9 | 1 | 2 | 2 | 0 | 14 |
| 13 | 10 | 2 | 1 | 2 | 0 | 14 |
| 14 | 11 | 1 | 2 | 2 | 0 | 10 |
| 15 | 12 | 2 | 2 | 3 | 1 | 39 |

动作总数 229，包括初始化。峰值以 allocate_gpu 获取到 return_gpu 释放计数，不能在清除绑定时提前释放。
额外用 closed-set LP 枚举式审计所有合法拓扑前缀：每个物理设备占用始终在 [0,1]，总占用最多 3。
容量审计同样检查所有拓扑前缀，使用 catalog 容量及动作效果，不是实测吞吐。

## 动作与策略覆盖

| Primitive action | 次数 |
|---|---|
| activate_instance_route | 45 |
| allocate_gpu | 6 |
| apply_batch | 17 |
| bind_target_gpu | 6 |
| clear_gpu_binding | 4 |
| clear_template | 4 |
| configure_full_template | 6 |
| configure_partial_profile | 3 |
| deactivate_instance_route | 21 |
| delete_instance | 21 |
| patch_batch_config | 17 |
| place_instance | 28 |
| register_mig_devices | 9 |
| return_gpu | 4 |
| verify_batch | 17 |
| wait_instance_drain | 21 |

正式 transition_planner 中静态 _action 调用产生的 16 类 primitive actions 全部覆盖；缺失：[]。
执行器中的兼容别名、no-op、故障重试不纳入这 16 类，不宣称覆盖全部执行器分支。
策略选择计数：{"create_gpu": 5, "instance_diff": 9, "remove_gpu": 3, "partial_reconfiguration": 3, "bridge_reconfiguration": 1}。
Partial 与 Bridge 覆盖，In-place 未覆盖；全扫描也没有选中 In-place。
临时容量注入和所有特殊失败恢复分支不能仅凭 primitive action 覆盖宣称已测试。
若必须展示三类策略，另补独立 In-place 冒烟案例，不改动这条连续 trace。

## 集群执行前仍需确认

两个节点是否都可部署七种请求类型、模型路径/镜像、精确 MIG slot/CDI、batch 更新支持和真实设备健康状态。
这里只限定三张设备库存，未查询实时节点污点、显存外资源或模型缓存，也未测跨节点网络。
selected_demand.csv 与 pkl 是离线输入/诊断产物，含占位物理 ID，不能直接作为真实集群动作清单提交。
实机需从观测快照重新规划，并再次检查峰值；先跑四类冒烟再跑连续实验。
本轮未更改 catalog、正式 k8s 代码或已有论文结果，也未 push。
