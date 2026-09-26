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
