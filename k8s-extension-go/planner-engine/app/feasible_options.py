from __future__ import annotations

from models import ProfileOption, WorkloadRequest


def profile_catalog_from_yaml(obj: dict) -> list[ProfileOption]:
    options = []
    for raw in obj.get("options", []):
        metrics = {
            k: v
            for k, v in raw.items()
            if k not in {"workload", "family", "batch", "profile", "mu", "fit"}
        }
        options.append(
            ProfileOption(
                workload=str(raw["workload"]),
                family=raw.get("family"),
                batch=int(raw["batch"]),
                profile=str(raw["profile"]),
                mu=float(raw["mu"]),
                fit=bool(raw.get("fit", False)),
                metrics=metrics,
            )
        )
    return options


def feasible_options_for_request(
    request: WorkloadRequest,
    catalog: list[ProfileOption],
) -> list[ProfileOption]:
    out = []
    allowed_batches = set(request.allowed_batches)
    for option in catalog:
        if not option.fit:
            continue
        if option.workload not in {request.name, request.model}:
            continue
        if allowed_batches and option.batch not in allowed_batches:
            continue
        if not _slo_matches(request, option):
            continue
        out.append(option)
    return sorted(out, key=lambda x: (x.profile_size, -x.mu, x.batch))


def _slo_matches(request: WorkloadRequest, option: ProfileOption) -> bool:
    slo_to_metric = {
        "e2eMs": "e2eMs",
        "ttftMs": "ttftMs",
        "tpotMs": "tpotMs",
    }
    for slo_key, metric_key in slo_to_metric.items():
        if slo_key not in request.slo:
            continue
        if metric_key not in option.metrics:
            continue
        if float(option.metrics[metric_key]) > float(request.slo[slo_key]):
            return False
    return True

