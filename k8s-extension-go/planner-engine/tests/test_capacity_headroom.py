from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from models import PlanningScenario, ScenarioWorkloadDemand  # noqa: E402
from planning.k8s_adapter import capacity_headroom, conservative_3g_option_dataframe  # noqa: E402


def _frame() -> pd.DataFrame:
    return pd.DataFrame([
        {"opt_idx": 0, "w_idx": 0, "workload": "a", "batch": 1, "profile": "3g", "mu": 10.0},
        {"opt_idx": 1, "w_idx": 0, "workload": "a", "batch": 1, "profile": "4g", "mu": 9.0},
        {"opt_idx": 2, "w_idx": 0, "workload": "a", "batch": 4, "profile": "3g", "mu": 20.0},
        {"opt_idx": 3, "w_idx": 0, "workload": "a", "batch": 4, "profile": "4g", "mu": 25.0},
        {"opt_idx": 4, "w_idx": 1, "workload": "b", "batch": 1, "profile": "3g", "mu": 5.0},
    ])


def test_conservative_3g_takes_min_with_same_batch_4g_only():
    out = conservative_3g_option_dataframe(_frame())
    assert list(out["mu"]) == [9.0, 9.0, 20.0, 25.0, 5.0]


def test_conservative_3g_leaves_input_frame_untouched():
    frame = _frame()
    conservative_3g_option_dataframe(frame)
    assert list(frame["mu"]) == [10.0, 9.0, 20.0, 25.0, 5.0]


def _scenario(transition: dict) -> PlanningScenario:
    return PlanningScenario(
        name="t", description="", policy_ref="", mig_rules_ref="", source_state_ref="", target_state_ref="",
        workloads=[ScenarioWorkloadDemand(name="a", source_arrival=0.0, target_arrival=1.0, workload_ref="")],
        transition=transition,
    )


def test_capacity_headroom_defaults_to_zero_and_rejects_negative():
    assert capacity_headroom(_scenario({})) == 0.0
    assert capacity_headroom(_scenario({"capacityHeadroom": 0.1})) == 0.1
    try:
        capacity_headroom(_scenario({"capacityHeadroom": -0.1}))
    except ValueError:
        pass
    else:
        raise AssertionError("negative headroom accepted")
