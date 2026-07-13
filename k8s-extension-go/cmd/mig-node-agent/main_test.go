package main

import "testing"

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
