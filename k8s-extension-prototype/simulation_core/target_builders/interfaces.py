from __future__ import annotations

from typing import Any, Callable


TargetBuilder = Callable[..., Any]


def build_target(builder: TargetBuilder, **kwargs: Any) -> Any:
    return builder(**kwargs)
