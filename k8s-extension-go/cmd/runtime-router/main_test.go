package main

import "testing"

func TestRouteHealthRequiresConsecutiveFailures(t *testing.T) {
	state := &routerState{routeGCFailures: map[string]int{}}
	if state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("first health failure must not remove the route")
	}
	if state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("second health failure must not remove the route")
	}
	if !state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("third consecutive health failure must remove the route")
	}
}

func TestRouteHealthSuccessResetsFailureCount(t *testing.T) {
	state := &routerState{routeGCFailures: map[string]int{}}
	state.routeHealthFailed("runtime-a", false, 3)
	state.routeHealthFailed("runtime-a", false, 3)
	if state.routeHealthFailed("runtime-a", true, 3) {
		t.Fatal("successful health check must not remove the route")
	}
	if state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("failure count must restart after a successful health check")
	}
}
