from __future__ import annotations

import re

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


def apply_runtime_profile_correction(
    catalog: list[ProfileOption],
    correction: dict | None,
) -> tuple[list[ProfileOption], dict]:
    """Apply conservative runtime profile correction to profile options.

    Runtime measurements are not allowed to make profiles more optimistic. They
    can only reduce effective throughput or increase latency-like metrics.
    """

    if not correction:
        return catalog, {"available": False, "appliedCount": 0, "observations": []}
    observations = [
        _normalize_observation(raw)
        for raw in correction.get("observations", [])
        if isinstance(raw, dict)
    ]
    observations = [obs for obs in observations if obs.get("model")]
    if not observations:
        return catalog, {"available": False, "appliedCount": 0, "observations": []}

    applied = []
    corrected = []
    for option in catalog:
        best = _best_observation_for_option(option, observations)
        if best is None:
            corrected.append(option)
            continue
        new_mu = float(option.mu)
        new_metrics = dict(option.metrics)
        changed = False
        observed_mu = best.get("observedMu")
        if observed_mu is not None and float(observed_mu) > 0:
            conservative_mu = min(float(option.mu), float(observed_mu))
            if conservative_mu != float(option.mu):
                new_mu = conservative_mu
                changed = True
        observed_latency = best.get("observedLatencyMs")
        if observed_latency is not None and float(observed_latency) > 0:
            old_latency = float(new_metrics.get("serviceTimeMs", 0.0) or 0.0)
            conservative_latency = max(old_latency, float(observed_latency))
            if conservative_latency != old_latency:
                new_metrics["serviceTimeMs"] = conservative_latency
                changed = True
        if changed:
            new_metrics["runtimeProfileCorrection"] = {
                "source": "runtime-observation",
                "confidence": best.get("confidence"),
                "sampleCount": best.get("sampleCount"),
                "observedMu": best.get("observedMu"),
                "observedLatencyMs": best.get("observedLatencyMs"),
                "originalMu": float(option.mu),
                "effectiveMu": float(new_mu),
            }
            applied.append(
                {
                    "workload": option.workload,
                    "batch": option.batch,
                    "profile": option.profile,
                    "originalMu": float(option.mu),
                    "effectiveMu": float(new_mu),
                    "observedLatencyMs": best.get("observedLatencyMs"),
                    "confidence": best.get("confidence"),
                }
            )
            corrected.append(
                ProfileOption(
                    workload=option.workload,
                    family=option.family,
                    batch=option.batch,
                    profile=option.profile,
                    mu=float(new_mu),
                    fit=option.fit,
                    metrics=new_metrics,
                )
            )
        else:
            corrected.append(option)
    return corrected, {
        "available": True,
        "policy": "runtime-profile-correction/v1",
        "muPolicy": "min(originalMu, observedMu)",
        "latencyPolicy": "max(originalLatencyMs, observedLatencyMs)",
        "appliedCount": len(applied),
        "applied": applied,
    }


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


def _normalize_observation(raw: dict) -> dict:
    obs = dict(raw)
    if "profile" not in obs and obs.get("slotResource"):
        match = re.search(r"-s[0-9]+-[0-9]+-([0-9]g)$", str(obs["slotResource"]))
        if match:
            obs["profile"] = match.group(1)
    for key in ("batch", "sampleCount"):
        if key in obs and obs[key] is not None:
            try:
                obs[key] = int(obs[key])
            except (TypeError, ValueError):
                obs.pop(key, None)
    for key in ("observedMu", "observedLatencyMs"):
        if key in obs and obs[key] is not None:
            try:
                obs[key] = float(obs[key])
            except (TypeError, ValueError):
                obs.pop(key, None)
    return obs


def _best_observation_for_option(option: ProfileOption, observations: list[dict]) -> dict | None:
    matches = []
    for obs in observations:
        if str(obs.get("model")) != option.workload:
            continue
        if obs.get("batch") is not None and int(obs["batch"]) != int(option.batch):
            continue
        if obs.get("profile") is not None and str(obs["profile"]) != str(option.profile):
            continue
        matches.append(obs)
    if not matches:
        return None
    confidence_rank = {"high": 3, "medium": 2, "low": 1, "none": 0}
    return max(
        matches,
        key=lambda obs: (
            confidence_rank.get(str(obs.get("confidence", "none")), 0),
            int(obs.get("sampleCount", 0) or 0),
        ),
    )
