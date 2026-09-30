# Stage 3：partial 容量检查的三处补充观察（Go 侧，2026-09-30）

对象：k8s-extension-go/planner-engine，commit 131f3ef（`_partial_effect_feasible` 已改为 eval 的容量检查版本，其余 Stage 3 代码与 eval 一致）。

背景：Go 侧先跟进了 eval 的容量检查版本（131f3ef），又按下面第 1、2 点做了修正（见文末"Go 侧的处理"）。用 E1 的 12 轮输入（catalog_newest_v3.csv，SliceWise，conservative3gMu，gpuBudget=3）离线对比了三个版本：

| 版本 | R5 | R12 | 借 GPU 的轮数 |
|---|---|---|---|
| 严格（c5196ed 之后） | bridge，40 个动作，借 1 块 | bridge，33 个动作，借 1 块 | 2 |
| 旧宽松分支 | partial，27 个动作，不借 | bridge，33 个动作，借 1 块 | 1 |
| 容量检查（eval 现行） | bridge，40 个动作，借 1 块 | partial，16 个动作，不借 | 1 |

其余 10 轮三个版本相同。下面是在对比中看到的问题。

## 1. 原位 slot 改 batch 增加的容量没有计入

容量检查把 target 中与 source 同 `(start, end, profile, workload)` 的实例视为已有，不计入新容量。但如果这个保留的 slot 改了 batch，它的 μ 会变大，而且 capacity gate 生成依赖边时会把这次 `activate_instance_route`（batch 改变后）当作 producer。

E1 R5：旧宽松分支选 partial 能生成计划、审计通过；容量检查因为没有计入这部分增量，判定不可行，退回 bridge，多借 1 块 GPU。

建议：对保留 slot，计入 `max(0, μ_target − μ_source)`。

## 2. 本 GPU 上不重叠的新 MIG 实例，按现在的动作结构保护不了删除

容量检查中，本 GPU 上的新实例只要和被删 slot 不重叠就计入。这里要区分两种新实例：

- 保留的 MIG 实例上换进来的新 workload（`PRESERVE_SLOT_*`）：不依赖 `configure_partial_profile`，可以先于删除就绪。计入是对的。E1 R12 的 partial 靠的就是这种：删 (6,7,1g) resnet 之前，先等 (2,3,1g) 从 gpt2_p512 换成 resnet。
- `create_slots` 里的新 MIG 实例：`_append_partial_reconfiguration_actions` 先对所有 delete_slots 做 deactivate → drain → delete_instance，再用一个 `configure_partial_profile` 一次性创建所有 create_slots。所以即使新实例不和被删 slot 重叠（建在 source 本来就空闲的位置），它也要等所有删除完成后才存在，保护不了删除。

第二种被计入时不会不安全：生成依赖边时 `_capacity_dependency_would_cycle` 会跳过这条会成环的边，删除动作凑不够容量就标记 `blockedByCapacity`，补临时容量。但这正是容量检查想提前排除的情况，可能又要借 GPU，或者规划失败。`_same_physical_capacity_dependency_allowed` 也同样只看是否重叠。

建议二选一：

- 容量检查中，本 GPU 只计入保留 slot 上换进来的 workload（以及第 1 点的 batch 增量），不计入 create_slots。
- 或者改动作结构：把 partial 的 MIG 操作拆成两步，先在不重叠的空闲位置创建新 slot（不依赖删除），再删除旧 slot、创建重叠的新 slot。node-agent 的 `/patch-slots` 已支持只在空隙中创建，同一 GPU 的 MIG 操作由每卡锁串行，不需要新的并行协议；需要同步修改 action_builder、action_simulator，以及 executor 对"只有 create 的 configure_partial_profile"的处理。

E1 的 12 轮里没有出现第二种情况，所以对 E1 结果没有影响。

## 3. （撤回）同一 workload 的多个去激活动作叠加

之前报告的 R5、R12 两处"最坏拓扑序下低于承诺"，是 Go 侧自写审计的错误，不是 planner 的问题。审计在计算最坏情况时减去了另一个去激活动作消耗的容量，却没有加回它必须等待的新 producer。`_add_cumulative_capacity_dependency_edges` 会让后处理的 consumer 继承前面 consumer 选中的 producer，再补上自己需要的，所以任意执行顺序都安全。

改用正确的定义（最坏情况取所有可能已完成的动作集合，即下闭集：一个动作被计入时它的所有祖先也被计入，后代排除）之后，SliceWise 的 E1 计划全部 0 违规；SW−C 在 R3、R4、R5、R12 有违规，符合它去掉 capacity edge 的预期。

## Go 侧的处理（commit 见 git log）

`_partial_effect_feasible` 在容量检查版本上改了两处：

- 第 1 点：保留的实例（同 slot、同 workload）计入 `max(0, μ_target − μ_source)`，其他 GPU 也一样。
- 第 2 点：本 GPU 上只计入 `partial_plan.preserve_slots` 上的新容量（换进来的 workload，或 batch 增加的 μ），不计入 create_slots 的新 MIG 实例，无论是否重叠。其他 GPU 上新建的实例全部计入。

E1 12 轮（SliceWise）结果：

| 版本 | R5 | R12 | 借 GPU 的轮数 | 审计违规 |
|---|---|---|---|---|
| 容量检查（eval 现行） | bridge，40 个动作，借 1 块 | partial，16 个动作 | 1 | 0 |
| Go 修正后 | partial，27 个动作，不借 | partial，16 个动作 | 0 | 0 |

这两处修改使 Go 与 eval 再次有差异，建议 eval 评估后同步。
