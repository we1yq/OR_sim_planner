package main

import "testing"

func TestRepairActionsDeleteRuntimesBeforeClearingAndReturning(t *testing.T) {
	actions := repairActions([]any{map[string]any{
		"type": "clear_template_before_available", "physicalGpuId": "rtx1-worker-gpu0",
		"node": "rtx1-worker", "gpuIndex": 0,
	}})
	if len(actions) != 4 {
		t.Fatalf("expected delete, clear-binding, clear-template, return; got %#v", actions)
	}
	types := []string{}
	for _, action := range actions {
		types = append(types, asString(action["type"]))
	}
	want := []string{"delete_instance", "clear_gpu_binding", "clear_template", "return_gpu"}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("repair action %d = %q, want %q", i, types[i], want[i])
		}
	}
	for i := 1; i < len(actions); i++ {
		deps, ok := actions[i]["dependsOn"].([]string)
		if !ok || len(deps) != 1 || deps[0] != asString(actions[i-1]["id"]) {
			t.Fatalf("repair action %q must depend on its predecessor: %#v", actions[i]["id"], deps)
		}
	}
	deleteAction := asMap(actions[0]["action"])
	if asString(deleteAction["physical_gpu_id"]) != "rtx1-worker-gpu0" || deleteAction["slot"] != nil || deleteAction["workload"] != nil {
		t.Fatalf("repair delete must target every runtime deployment on the GPU: %#v", deleteAction)
	}
}
