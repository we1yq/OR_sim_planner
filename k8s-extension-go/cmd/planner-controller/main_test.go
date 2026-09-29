package main

import "testing"

func TestRequestedPhaseGate(t *testing.T) {
	for _, gate := range []string{"manual", "hold"} {
		if got := requestedPhaseGate(map[string]any{"phaseGate": gate}); got != "manual" {
			t.Fatalf("phaseGate %q normalized to %q, want manual", gate, got)
		}
	}
	for _, gate := range []string{"", "auto", "unexpected"} {
		if got := requestedPhaseGate(map[string]any{"phaseGate": gate}); got != "auto" {
			t.Fatalf("phaseGate %q normalized to %q, want auto", gate, got)
		}
	}
}

func TestPlanningInputForwardsCapacityHeadroomFields(t *testing.T) {
	in := planningInputFromSnapshot(map[string]any{"capacityHeadroom": 0.1, "conservative3gMu": true})
	if in.CapacityHeadroom == nil || *in.CapacityHeadroom != 0.1 || !in.Conservative3gMu {
		t.Fatalf("headroom fields not forwarded: %+v", in)
	}
	if none := planningInputFromSnapshot(map[string]any{}); none.CapacityHeadroom != nil || none.Conservative3gMu {
		t.Fatalf("absent fields must stay unset: %+v", none)
	}
}
