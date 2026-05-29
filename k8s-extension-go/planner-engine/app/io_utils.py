from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML object in {path}")
    return data


def dump_yaml(data: dict[str, Any]) -> str:
    return yaml.safe_dump(data, sort_keys=False)

