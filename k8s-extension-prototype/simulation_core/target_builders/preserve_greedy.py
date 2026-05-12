from __future__ import annotations

from typing import Any

from ..target_builder import build_target_state_from_milp


NAME = "target.preserve_greedy"


def build(**kwargs: Any) -> Any:
    target = build_target_state_from_milp(**kwargs)
    target.metadata["target_builder_module"] = NAME
    return target
