from __future__ import annotations

from typing import Any

from ..target_builder import build_target_state_from_milp


NAME = "target.no_preserve_greedy"


def build(**kwargs: Any) -> Any:
    kwargs = dict(kwargs)
    kwargs["prev_state"] = None
    target = build_target_state_from_milp(**kwargs)
    target.metadata["target_builder_module"] = NAME
    target.metadata.setdefault("build_metrics", {})["preserve_disabled"] = True
    return target
