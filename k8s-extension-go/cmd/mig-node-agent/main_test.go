package main

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestMIGSlotsFromObservationOnlyUsesTargetGPUBlock(t *testing.T) {
	smi := `GPU 0: NVIDIA A100-PCIE-40GB (UUID: GPU-0)
GPU 1: NVIDIA A100-PCIE-40GB (UUID: GPU-1)
  MIG 3g.20gb     Device  0: (UUID: MIG-gpu1-3g)`
	instances := []gpuInstance{
		{Name: "MIG 3g.20gb", ProfileID: "9", InstanceID: "2", Start: 4, Size: 4},
	}

	if got := migSlotsFromObservation(smi, instances, "0"); len(got) != 0 {
		t.Fatalf("GPU0 must not inherit GPU1 MIG slots: %#v", got)
	}

	got := migSlotsFromObservation(smi, instances, "1")
	if len(got) != 1 {
		t.Fatalf("expected one GPU1 MIG slot, got %#v", got)
	}
	if got[0].MIGDeviceUUID != "MIG-gpu1-3g" || got[0].SlotStart != 4 || got[0].SlotEnd != 8 {
		t.Fatalf("unexpected slot: %#v", got[0])
	}
}

func TestMIGCapableGPUIndexesExcludesMixedRTXAdapters(t *testing.T) {
	smi := `GPU 0: NVIDIA A100-PCIE-40GB (UUID: GPU-a100)
GPU 1: NVIDIA TITAN RTX (UUID: GPU-titan0)
GPU 2: NVIDIA TITAN RTX (UUID: GPU-titan1)
GPU 3: NVIDIA GeForce RTX 3090 (UUID: GPU-rtx3090)`

	got := migCapableGPUIndexes(smi)
	if len(got) != 1 || got[0] != "0" {
		t.Fatalf("only the A100 may be queried for MIG instances, got %#v", got)
	}
}

func TestMIGCapableGPUIndexesSortsNumericIndexes(t *testing.T) {
	smi := `GPU 10: NVIDIA A100-PCIE-40GB (UUID: GPU-10)
GPU 2: NVIDIA A100-PCIE-40GB (UUID: GPU-2)`
	got := migCapableGPUIndexes(smi)
	if len(got) != 2 || got[0] != "2" || got[1] != "10" {
		t.Fatalf("MIG indexes must be numeric and stable, got %#v", got)
	}
}

func TestValidateSlotPatchAllowsCreateInGap(t *testing.T) {
	create := []slotSpec{{Start: 3, Size: 1, Profile: "1g"}}
	preserve := []slotSpec{
		{Start: 0, Size: 2, Profile: "2g"},
		{Start: 2, Size: 1, Profile: "1g"},
		{Start: 4, Size: 4, Profile: "3g"},
	}
	if err := validateSlotPatch(nil, create, preserve); err != nil {
		t.Fatalf("create in unoccupied gap should be valid: %v", err)
	}
}

func TestValidateSlotPatchRejectsCreateOverPreserve(t *testing.T) {
	create := []slotSpec{{Start: 2, Size: 1, Profile: "1g"}}
	preserve := []slotSpec{{Start: 2, Size: 1, Profile: "1g"}}
	if err := validateSlotPatch(nil, create, preserve); err == nil {
		t.Fatal("create overlapping preserved slot must be rejected")
	}
}

func TestStaleOrSimResourceKeys(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	capacity := map[string]any{
		"or-sim.io/mig-live":          "1",
		"or-sim.io/mig-old":           "0",
		"or-sim.io/mig-in-grace":      "1",
		"or-sim.io/mig-past-grace":    "1",
		"or-sim.io/node-gpu0-s0-4-3g": "0",
		"or-sim.io/mig-busy":          "1",
		"nvidia.com/mig-3g.20gb":      "0",
	}
	allocatable := map[string]any{
		"or-sim.io/mig-live":          "0",
		"or-sim.io/mig-old":           "0",
		"or-sim.io/mig-in-grace":      "0",
		"or-sim.io/mig-past-grace":    "0",
		"or-sim.io/node-gpu0-s0-4-3g": "0",
		"or-sim.io/mig-busy":          "1",
		"nvidia.com/mig-3g.20gb":      "0",
	}
	registered := map[string]bool{"or-sim.io/mig-live": true}
	withdrawnAt := map[string]time.Time{
		"or-sim.io/mig-in-grace":   now.Add(-time.Minute),
		"or-sim.io/mig-past-grace": now.Add(-stalePruneWithdrawGrace),
	}
	got := staleOrSimResourceKeys(capacity, allocatable, registered, withdrawnAt, now)
	want := []string{"or-sim.io/mig-old", "or-sim.io/mig-past-grace", "or-sim.io/node-gpu0-s0-4-3g"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stale keys = %v, want %v", got, want)
	}
}

func TestStaleResourcePatchOpsGuardsEachRemoval(t *testing.T) {
	capacity := map[string]any{"or-sim.io/mig-a": "0"}
	allocatable := map[string]any{"or-sim.io/mig-a": "0"}
	got := staleResourcePatchOps([]string{"or-sim.io/mig-a"}, capacity, allocatable)
	want := []map[string]any{
		{"op": "test", "path": "/status/capacity/or-sim.io~1mig-a", "value": "0"},
		{"op": "remove", "path": "/status/capacity/or-sim.io~1mig-a"},
		{"op": "test", "path": "/status/allocatable/or-sim.io~1mig-a", "value": "0"},
		{"op": "remove", "path": "/status/allocatable/or-sim.io~1mig-a"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("patch ops = %v, want %v", got, want)
	}
}

func TestGPULocksAreIndependentAcrossGPUsAndExclusivePerGPU(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.lock")
	unlock0, err := acquireGPULock(path, "0")
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	go func() {
		unlock, err := acquireGPULock(path, "1")
		if err != nil {
			t.Error(err)
			return
		}
		acquired <- unlock
	}()
	select {
	case unlock1 := <-acquired:
		unlock1()
	case <-time.After(2 * time.Second):
		t.Fatal("GPU1 lock must not wait for GPU0")
	}

	second := make(chan func(), 1)
	go func() {
		unlock, err := acquireGPULock(path, "0")
		if err != nil {
			t.Error(err)
			return
		}
		second <- unlock
	}()
	select {
	case <-second:
		t.Fatal("a second GPU0 lock must wait for the first")
	case <-time.After(200 * time.Millisecond):
	}
	unlock0()
	select {
	case unlock := <-second:
		unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("GPU0 lock must be granted after release")
	}
}

func TestHostLockDoesNotWaitForGPULock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.lock")
	unlockGPU, err := acquireGPULock(path, "0")
	if err != nil {
		t.Fatal(err)
	}
	defer unlockGPU()
	done := make(chan struct{})
	go func() {
		unlock, err := acquireLock(path)
		if err == nil {
			unlock()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CDI refresh (host lock) must not wait for a GPU's MIG change")
	}
}
