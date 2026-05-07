from __future__ import annotations

from typing import Any

from reconciler import run_watch_controller_loop, should_reconcile_migplan, upsert_cluster_state_configmap


class FakeKubernetesClient:
    def __init__(self) -> None:
        self.configmaps: dict[tuple[str, str], dict[str, Any]] = {}
        self.migplans: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def get_migplan(self, name: str, namespace: str) -> dict[str, Any]:
        raise NotImplementedError

    def list_migplans(self, namespace: str) -> list[dict[str, Any]]:
        return list(self.migplans)

    def patch_migplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        raise NotImplementedError

    def get_configmap(self, name: str, namespace: str) -> dict[str, Any]:
        return self.configmaps[(namespace, name)]

    def apply_configmap(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.configmaps[(metadata["namespace"], metadata["name"])] = manifest

    def watch_migplans(self, namespace: str, timeout_seconds: int) -> Any:
        yield from self.events


def test_should_reconcile_generation_gap() -> None:
    migplan = {"metadata": {"generation": 2}, "status": {"observedGeneration": 1, "phase": "ReachedTarget"}}
    assert should_reconcile_migplan(migplan)


def test_should_skip_observed_generation() -> None:
    migplan = {"metadata": {"generation": 2}, "status": {"observedGeneration": 2, "phase": "ReachedTarget"}}
    assert not should_reconcile_migplan(migplan)


def test_upsert_cluster_state_configmap() -> None:
    client = FakeKubernetesClient()
    state = {"metadata": {"physical_id_map": {"0": "A"}}, "gpus": []}
    upsert_cluster_state_configmap(
        name="target0-state",
        namespace="or-sim",
        state=state,
        owner_migplan="stage0",
        client=client,
    )

    configmap = client.get_configmap("target0-state", "or-sim")
    assert configmap["metadata"]["labels"]["mig.or-sim.io/state-kind"] == "canonical-next-state"
    assert configmap["metadata"]["labels"]["mig.or-sim.io/owner-migplan"] == "stage0"
    assert "physical_id_map" in configmap["data"]["state.yaml"]


def test_watch_loop_skips_observed_initial_object() -> None:
    client = FakeKubernetesClient()
    client.migplans = [
        {
            "metadata": {"name": "stage0", "generation": 1},
            "status": {"observedGeneration": 1, "phase": "ReachedTarget"},
        }
    ]
    summary = run_watch_controller_loop(
        namespace="or-sim",
        max_events=1,
        client=client,
    )
    assert summary["summary"]["skipped"] == ["stage0"]
    assert summary["summary"]["reconciled"] == []


def main() -> int:
    test_should_reconcile_generation_gap()
    test_should_skip_observed_generation()
    test_upsert_cluster_state_configmap()
    test_watch_loop_skips_observed_initial_object()
    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
