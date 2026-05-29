from __future__ import annotations

from typing import Any

import yaml

from api.k8s_api import KubernetesClient, PythonKubernetesClient
from executors.mig_config_manager import dynamic_config_name


def create_real_transition_smoke_action_plan(
    name: str,
    namespace: str = "or-sim",
    node_name: str = "ampere",
    physical_gpu_id: str = "ampere-gpu0",
    device_index: int = 0,
    logical_gpu_id: int = 0,
    target_template: str = "1g+1g",
    client_: KubernetesClient | None = None,
) -> dict[str, Any]:
    client_ = client_ or PythonKubernetesClient()
    target_instances = _instances_from_template(target_template)
    config_name = dynamic_config_name(
        node_name=node_name,
        targets=[
            {
                "deviceIndex": int(device_index),
                "physicalGpuId": physical_gpu_id,
                "targetTemplate": target_template,
                "targetInstances": target_instances,
            }
        ],
    )
    actions = [
        {
            "actionKey": "bind-target-gpu",
            "type": "bind_target_gpu",
            "gpu_id": int(logical_gpu_id),
            "physical_gpu_id": physical_gpu_id,
        },
        {
            "actionKey": "configure-mig-template",
            "type": "configure_full_template",
            "gpu_id": int(logical_gpu_id),
            "physical_gpu_id": physical_gpu_id,
            "template": target_template,
            "dependsOnActionKeys": ["bind-target-gpu"],
        },
        {
            "actionKey": "register-mig-devices",
            "type": "register_mig_devices",
            "gpu_id": int(logical_gpu_id),
            "physical_gpu_id": physical_gpu_id,
            "dependsOnActionKeys": ["configure-mig-template"],
        },
        {
            "actionKey": "activate-serving-route",
            "type": "activate_serving_route",
            "gpu_id": int(logical_gpu_id),
            "physical_gpu_id": physical_gpu_id,
            "dependsOnActionKeys": ["register-mig-devices"],
        },
    ]
    full_status = {
        "phase": "RealTransitionSmoke",
        "metrics": {"actionCount": len(actions), "gpuCount": 1},
        "planningSummary": {
            "actionCountsByType": {
                "bind_target_gpu": 1,
                "configure_full_template": 1,
                "register_mig_devices": 1,
                "activate_serving_route": 1,
            },
            "chosenTemplates": [target_template],
        },
        "actions": actions,
        "canonicalNextState": {
            "metadata": {
                "physical_id_map": {str(logical_gpu_id): physical_gpu_id},
                "physicalGpuBindings": {
                    physical_gpu_id: {
                        "nodeName": node_name,
                        "deviceIndex": int(device_index),
                    }
                },
                "smokeTest": "real-transition-mig-registry",
            },
            "gpus": [
                {
                    "gpuId": int(logical_gpu_id),
                    "source": "real-smoke",
                    "instances": target_instances,
                }
            ],
        },
    }
    full_plan_name = f"{name}-full-plan"
    client_.apply_configmap(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": full_plan_name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "or-sim-mig-planner",
                    "mig.or-sim.io/state-kind": "full-plan-debug",
                    "mig.or-sim.io/owner-migplan": f"{name}-synthetic-owner",
                    "mig.or-sim.io/real-transition-smoke": "true",
                },
            },
            "data": {"status.yaml": yaml.safe_dump(full_status, sort_keys=False)},
        }
    )
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/real-transition-smoke": "true",
            },
        },
        "spec": {
            "migPlanRef": f"{name}-synthetic-owner",
            "migPlanGeneration": 1,
            "dryRun": False,
            "executor": "transition-executor",
            "phaseGate": "ApprovedForRealExecution",
            "fullPlanConfigMap": full_plan_name,
            "actionCount": len(actions),
            "actionCountsByType": full_status["planningSummary"]["actionCountsByType"],
            "chosenTemplates": [target_template],
            "targetGpuCount": 1,
            "executorPreview": {
                "previewOnly": False,
                "executor": "nvidia-gpu-operator",
                "gpuOperatorLabel": "nvidia.com/mig.config",
                "gpuTargets": [
                    {
                        "logicalGpuId": int(logical_gpu_id),
                        "physicalGpuId": physical_gpu_id,
                        "nodeName": node_name,
                        "deviceIndex": int(device_index),
                        "targetTemplate": target_template,
                        "targetInstances": target_instances,
                    }
                ],
                "migManagerTargetConfigs": [
                    {
                        "nodeName": node_name,
                        "configName": config_name,
                        "gpus": [
                            {
                                "deviceIndex": int(device_index),
                                "physicalGpuId": physical_gpu_id,
                                "targetTemplate": target_template,
                                "targetInstances": target_instances,
                            }
                        ],
                    }
                ],
                "wouldPatchNodeLabels": {
                    node_name: {"nvidia.com/mig.config": config_name},
                },
                "unresolvedPhysicalGpuIds": [],
            },
            "notes": [
                "Synthetic non-dryRun action plan for controlled real Transition Executor smoke testing.",
                "This plan changes real MIG geometry through NVIDIA GPU Operator.",
            ],
        },
        "status": {
            "phase": "ApprovedForRealExecution",
            "approved": True,
            "executed": False,
            "policyRef": "real-transition-smoke",
            "message": "Synthetic real transition smoke plan approved for explicit execution.",
        },
    }
    client_.apply_migactionplan(manifest)
    client_.patch_migactionplan_status(name=name, namespace=namespace, status=manifest["status"])
    return {
        "kind": "RealTransitionSmokeActionPlan",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "name": name,
        "namespace": namespace,
        "nodeName": node_name,
        "physicalGpuId": physical_gpu_id,
        "deviceIndex": int(device_index),
        "logicalGpuId": int(logical_gpu_id),
        "targetTemplate": target_template,
        "fullPlanConfigMap": full_plan_name,
        "migConfigName": config_name,
        "actions": actions,
        "status": manifest["status"],
    }


def _instances_from_template(template: str) -> list[dict[str, Any]]:
    cursor = 0
    instances = []
    for raw_profile in [part for part in template.split("+") if part]:
        profile = raw_profile if raw_profile.endswith("g") else f"{raw_profile}g"
        size = int(profile.removesuffix("g"))
        instances.append(
            {
                "start": cursor,
                "end": cursor + size,
                "profile": profile,
                "workload": None,
                "batch": None,
                "mu": 0.0,
            }
        )
        cursor += size
    return instances
