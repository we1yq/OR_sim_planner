package main

import "testing"

func TestObservedRuntimeBatchSizePrefersRuntimeMetrics(t *testing.T) {
	routes := []map[string]any{{
		"slotResource":      "or-sim.io/ampere-gpu0-s0-1-1g",
		"expectedMigUuid":   "MIG-test",
		"runtime.batchSize": 32,
	}}
	batch, source := observedRuntimeBatchSize(routes, "or-sim.io/ampere-gpu0-s0-1-1g", "MIG-test", 1)
	if batch != 32 || source != "runtime-metrics" {
		t.Fatalf("observed batch = (%d, %q), want (32, runtime-metrics)", batch, source)
	}
}

func TestObservedRuntimeBatchSizeFallsBackToPodSpec(t *testing.T) {
	batch, source := observedRuntimeBatchSize(nil, "", "", 16)
	if batch != 16 || source != "pod-spec-fallback" {
		t.Fatalf("observed batch = (%d, %q), want (16, pod-spec-fallback)", batch, source)
	}
}

func TestObservedRuntimeBatchSizeDoesNotBorrowAnotherReplica(t *testing.T) {
	routes := []map[string]any{
		{"slotResource": "slot-a", "runtime.batchSize": 32},
		{"slotResource": "slot-b", "runtime.batchSize": 16},
	}
	batch, source := observedRuntimeBatchSize(routes, "slot-missing", "", 1)
	if batch != 1 || source != "pod-spec-fallback" {
		t.Fatalf("unmatched replica = (%d, %q), want configured fallback", batch, source)
	}
}
