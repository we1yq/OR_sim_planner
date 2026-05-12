# Round 2 消融实验结果分析

生成文件目录：`k8s-extension-prototype/reports/ablation-2026-05-11-round2-scenarios/`

## 场景设计

第二轮不再只使用原始 `stage0-stage3` trace，而是加入更有针对性的压力场景：

| 场景 | 目的 | 说明 |
| --- | --- | --- |
| A/bootstrap-balanced | warm-up / bootstrap | 从空集群进入中等混合负载，避免 bootstrap 完全主导后续结果。 |
| B/placement-pressure | placement pressure | LLM/text 需求急剧上升，考察第一阶段模板选择是否会用过多 GPU。 |
| C/preserve-benefit | preserve-benefit | 负载从 LLM/text 转向 vision，考察 target builder 是否能避免不必要 layout churn。 |
| D/drain-reroute | drain/reroute | 全局收缩，考察清理、保留和 drain/reroute 处理。 |
| E/transition-ordering | transition ordering | 收缩后混合反弹，考察 transition ordering 对迭代轮数、峰值 GPU 和硬件估算的影响。 |

## 总览

| variant | completed | max_gpu | algo_s | placement_s | target_s | transition_s | hw_est_s | fine_actions | iterations |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| current_full | True | 12 | 18.26 | 0.34 | 17.40 | 0.08 | 2680 | 206 | 25 |
| placement_milp_original | True | 12 | 30.10 | 1.38 | 28.33 | 0.09 | 2933 | 224 | 26 |
| placement_greedy_two_phase | True | 18 | 100 | 92.41 | 7.56 | 0.10 | 3960 | 240 | 18 |
| placement_simulated_annealing | True | 14 | 72.17 | 71.04 | 0.80 | 0.05 | 3137 | 206 | 14 |
| target_no_preserve | True | 12 | 37.47 | 0.29 | 36.81 | 0.07 | 4076 | 304 | 15 |
| target_beam_preserve | True | 12 | 685 | 0.28 | 685 | 0.10 | 2951 | 244 | 33 |
| target_exact_milp_templates | True | 12 | 0.65 | 0.28 | 0.01 | 0.06 | 4193 | 306 | 11 |
| transition_serial_v0 | True | 12 | 18.31 | 0.29 | 17.47 | 0.23 | 2680 | 206 | 75 |
| transition_drain_v2 | True | 12 | 20.18 | 0.28 | 19.51 | 0.09 | 2803 | 213 | 26 |
| transition_full_plan_v2 | True | 12 | 18.35 | 0.29 | 17.70 | 0.05 | 2904 | 205 | 12 |

## 主要发现

1. 所有变体在第二轮 5 个场景中都完成，说明模块化后的 baseline 都能跑完整链路。
2. `current_full` 的估算硬件时间最低，为 2680s；相比 `target_no_preserve` 和 `target_exact_milp_templates`，preserve-aware target builder 明显减少了不必要重构。
3. placement 变体中，`placement_greedy_two_phase` 使用 GPU 峰值最高，max_gpu=18；这说明 greedy 更倾向用更多 GPU 快速堆出可行容量。
4. `target_beam_preserve` 的 target build 时间最高，为 685s。它适合作为搜索质量 baseline，但不适合在线低延迟路径。
5. transition 变体最终硬件估算接近，但迭代数差异明显：`transition_serial_v0` 需要 75 轮，`transition_full_plan_v2` 只需要 12 轮。这说明当前硬件估算仍然是动作成本累加，尚未把真正并行 makespan 建模进去。

## Placement 场景级结果

| scenario | variant | gpu | placement_s | hw_est_s |
| --- | --- | --- | --- | --- |
| A/bootstrap-balanced | current_full | 4 | 0.10 | 546 |
| A/bootstrap-balanced | placement_milp_original | 4 | 0.15 | 546 |
| A/bootstrap-balanced | placement_greedy_two_phase | 4 | 4.07 | 546 |
| A/bootstrap-balanced | placement_simulated_annealing | 4 | 10.38 | 546 |
| B/placement-pressure | current_full | 12 | 0.06 | 1035 |
| B/placement-pressure | placement_milp_original | 12 | 0.14 | 1040 |
| B/placement-pressure | placement_greedy_two_phase | 18 | 30.30 | 1586 |
| B/placement-pressure | placement_simulated_annealing | 14 | 15.80 | 1132 |
| C/preserve-benefit | current_full | 7 | 0.06 | 338 |
| C/preserve-benefit | placement_milp_original | 7 | 0.81 | 339 |
| C/preserve-benefit | placement_greedy_two_phase | 12 | 28.90 | 234 |
| C/preserve-benefit | placement_simulated_annealing | 8 | 17.99 | 342 |
| D/drain-reroute | current_full | 4 | 0.06 | 180 |
| D/drain-reroute | placement_milp_original | 4 | 0.06 | 301 |
| D/drain-reroute | placement_greedy_two_phase | 4 | 2.53 | 223 |
| D/drain-reroute | placement_simulated_annealing | 4 | 10.29 | 66.60 |
| E/transition-ordering | current_full | 9 | 0.05 | 580 |
| E/transition-ordering | placement_milp_original | 9 | 0.22 | 706 |
| E/transition-ordering | placement_greedy_two_phase | 15 | 26.60 | 1371 |
| E/transition-ordering | placement_simulated_annealing | 11 | 16.57 | 1051 |

## Target Builder 场景级结果

| scenario | variant | target_s | hw_est_s | fine_actions |
| --- | --- | --- | --- | --- |
| A/bootstrap-balanced | current_full | 0.95 | 546 | 30 |
| A/bootstrap-balanced | target_no_preserve | 0.68 | 546 | 30 |
| A/bootstrap-balanced | target_beam_preserve | 48.11 | 546 | 30 |
| A/bootstrap-balanced | target_exact_milp_templates | 0.00 | 546 | 30 |
| B/placement-pressure | current_full | 13.88 | 1035 | 45 |
| B/placement-pressure | target_no_preserve | 32.19 | 1291 | 64 |
| B/placement-pressure | target_beam_preserve | 558 | 1166 | 59 |
| B/placement-pressure | target_exact_milp_templates | 0.00 | 1165 | 56 |
| C/preserve-benefit | current_full | 0.71 | 338 | 61 |
| C/preserve-benefit | target_no_preserve | 1.92 | 849 | 97 |
| C/preserve-benefit | target_beam_preserve | 10.09 | 347 | 70 |
| C/preserve-benefit | target_exact_milp_templates | 0.00 | 849 | 97 |
| D/drain-reroute | current_full | 0.24 | 180 | 36 |
| D/drain-reroute | target_no_preserve | 0.51 | 429 | 52 |
| D/drain-reroute | target_beam_preserve | 14.16 | 188 | 44 |
| D/drain-reroute | target_exact_milp_templates | 0.00 | 555 | 59 |
| E/transition-ordering | current_full | 1.62 | 580 | 34 |
| E/transition-ordering | target_no_preserve | 1.50 | 961 | 61 |
| E/transition-ordering | target_beam_preserve | 54.11 | 704 | 41 |
| E/transition-ordering | target_exact_milp_templates | 0.00 | 1078 | 64 |

## Transition 场景级结果

| scenario | variant | transition_s | peak_gpu | hw_est_s | iters |
| --- | --- | --- | --- | --- | --- |
| A/bootstrap-balanced | current_full | 0.00 | 9 | 546 | 1 |
| A/bootstrap-balanced | transition_serial_v0 | 0.02 | 9 | 546 | 9 |
| A/bootstrap-balanced | transition_drain_v2 | 0.00 | 9 | 546 | 1 |
| A/bootstrap-balanced | transition_full_plan_v2 | 0.00 | 9 | 546 | 1 |
| B/placement-pressure | current_full | 0.01 | 13 | 1035 | 3 |
| B/placement-pressure | transition_serial_v0 | 0.04 | 12 | 1035 | 13 |
| B/placement-pressure | transition_drain_v2 | 0.01 | 13 | 1035 | 3 |
| B/placement-pressure | transition_full_plan_v2 | 0.01 | 13 | 1035 | 3 |
| C/preserve-benefit | current_full | 0.02 | 13 | 338 | 3 |
| C/preserve-benefit | transition_serial_v0 | 0.09 | 12 | 338 | 19 |
| C/preserve-benefit | transition_drain_v2 | 0.02 | 14 | 460 | 2 |
| C/preserve-benefit | transition_full_plan_v2 | 0.02 | 13 | 338 | 3 |
| D/drain-reroute | current_full | 0.02 | 7 | 180 | 8 |
| D/drain-reroute | transition_serial_v0 | 0.04 | 7 | 180 | 18 |
| D/drain-reroute | transition_drain_v2 | 0.02 | 7 | 182 | 9 |
| D/drain-reroute | transition_full_plan_v2 | 0.01 | 8 | 178 | 3 |
| E/transition-ordering | current_full | 0.03 | 9 | 580 | 10 |
| E/transition-ordering | transition_serial_v0 | 0.05 | 9 | 580 | 16 |
| E/transition-ordering | transition_drain_v2 | 0.03 | 9 | 580 | 11 |
| E/transition-ordering | transition_full_plan_v2 | 0.01 | 10 | 807 | 2 |

## 论文图表

- `figures_paper/fig1_placement_stage_time_gpu.svg`: 每个场景的 placement 算法时间和 GPU 数。
- `figures_paper/fig2_target_stage_time_cost.svg`: 每个场景的 target builder 时间、估算硬件时间、细粒度动作数。
- `figures_paper/fig3_transition_stage_time_gpu_hw.svg`: 每个场景的 transition planner 时间、峰值 GPU、估算硬件时间。

## 适合论文中的表述

这轮实验可以作为 ablation study 的第二组 stress scenarios，用来说明：current 方案不仅在原始 trace 上有效，在 LLM spike、vision shift、scale-down 和 rebound 等不同负载形态下也保持较低硬件重构成本。需要注意，`estimated_hardware_sec` 是由单机 MIG benchmark 常数估算得到，不是每个变体都真实执行硬件切换。
