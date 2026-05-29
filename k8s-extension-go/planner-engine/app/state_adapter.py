from __future__ import annotations

from models import GpuMigState, GpuState, MigInstance, WorkloadRequest


def workload_request_from_k8s_object(obj: dict) -> WorkloadRequest:
    metadata = obj.get("metadata", {})
    spec = obj.get("spec", {})
    name = metadata.get("name") or spec.get("model")
    if not name:
        raise ValueError("WorkloadRequest must have metadata.name or spec.model")
    if "model" not in spec:
        raise ValueError("WorkloadRequest spec.model is required")
    if "arrivalRate" not in spec:
        raise ValueError("WorkloadRequest spec.arrivalRate is required")

    return WorkloadRequest(
        name=str(name),
        model=str(spec["model"]),
        family=spec.get("family"),
        arrival_rate=float(spec["arrivalRate"]),
        allowed_batches=[int(x) for x in spec.get("allowedBatches", [])],
        priority=str(spec.get("priority", "normal")),
        slo={k: float(v) for k, v in dict(spec.get("slo", {})).items()},
    )


def gpu_state_from_mock_yaml(obj: dict) -> GpuMigState:
    gpus = []
    for raw_gpu in obj.get("gpus", []):
        instances = [
            MigInstance(
                start=int(raw_inst["start"]),
                end=int(raw_inst["end"]),
                profile=str(raw_inst["profile"]),
                workload=raw_inst.get("workload"),
                batch=(int(raw_inst["batch"]) if raw_inst.get("batch") is not None else None),
            )
            for raw_inst in raw_gpu.get("instances", [])
        ]
        gpus.append(
            GpuState(
                gpu_id=int(raw_gpu["gpuId"]),
                source=str(raw_gpu.get("source", "mock")),
                mig_enabled=bool(raw_gpu.get("migEnabled", True)),
                instances=instances,
            )
        )
    return GpuMigState(gpus=gpus)

