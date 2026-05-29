package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
	"or-sim/k8s-extension-go/internal/system"
)

type state struct {
	epoch        int
	lastArrival  map[string]float64
	lastSnapshot time.Time
}

var registeredSLOMs = map[string]float64{
	"llama":    1500,
	"gpt2":     1000,
	"resnet50": 500,
}

func main() {
	ns := env("NAMESPACE", "or-sim")
	router := env("ROUTER_URL", "http://runtime-router:8080")
	client, err := kube.NewInCluster(ns)
	if err != nil {
		log.Fatal(err)
	}
	st := &state{lastArrival: map[string]float64{}}
	go loop(client, router, st)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "component": "epoch-controller", "epoch": st.epoch})
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loop(client *kube.Client, router string, st *state) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		if err := reconcile(client, router, st); err != nil {
			log.Printf("epoch reconcile failed: %v", err)
		}
		<-tick.C
	}
}

func reconcile(client *kube.Client, router string, st *state) error {
	resp, err := http.Get(router + "/metrics/demand")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var demand system.DemandResponse
	if err := json.NewDecoder(resp.Body).Decode(&demand); err != nil {
		return err
	}
	current := map[string]float64{}
	requests := map[string]int64{}
	for _, model := range demand.Models {
		if model.Requests > 0 {
			current[model.Model] = model.ArrivalRate
			requests[model.Model] = model.Requests
		}
	}
	if len(current) == 0 {
		return nil
	}
	open, reason := shouldOpen(st, current, demand.Models)
	if !open {
		return nil
	}
	st.epoch++
	st.lastSnapshot = time.Now()
	st.lastArrival = current
	name := "runtime-epoch-" + strconv.Itoa(st.epoch)
	body := map[string]any{
		"apiVersion": "mig.or-sim.io/v1alpha1",
		"kind":       "ArrivalSnapshot",
		"metadata": map[string]any{
			"name":      name,
			"namespace": client.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":  "migrant-go",
				"mig.or-sim.io/component": "epoch-controller",
			},
		},
		"spec": map[string]any{
			"source":          "runtime-router",
			"mode":            "observed",
			"epoch":           strconv.Itoa(st.epoch),
			"windowSeconds":   int(demand.WindowSeconds),
			"unit":            "requests_per_second",
			"observedAt":      time.Now().Format(time.RFC3339Nano),
			"triggerReason":   reason,
			"registeredSLOMs": registeredSLOMs,
			"targetArrival":   current,
			"requestCount":    requests,
		},
	}
	if err := client.Upsert(kube.NamespacedResourceName(client.Namespace(), "arrivalsnapshots", name), body, nil); err != nil {
		return err
	}
	_, err = client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "arrivalsnapshots", name)+"/status", map[string]any{
		"status": map[string]any{"phase": "Ready", "message": "created from runtime-router demand"},
	}, nil)
	return err
}

func shouldOpen(st *state, current map[string]float64, models []system.DemandModel) (bool, string) {
	if st.epoch == 0 {
		return true, "initial_observed_demand"
	}
	if time.Since(st.lastSnapshot) < 30*time.Second {
		return false, ""
	}
	for _, model := range models {
		slo := registeredSLOMs[model.Model]
		if slo > 0 && model.AvgLatencyMs > slo {
			return true, "slo_latency_violation:" + model.Model
		}
		if model.ErrorRate > 0 {
			return true, "error_rate_nonzero:" + model.Model
		}
		if model.Queued > 0 {
			return true, "queue_nonempty:" + model.Model
		}
	}
	for model, value := range current {
		prev := st.lastArrival[model]
		if prev == 0 && value > 0 {
			return true, "new_model_demand:" + model
		}
		if prev > 0 && math.Abs(value-prev)/prev >= 0.30 {
			return true, "arrival_drift:" + model
		}
	}
	return false, ""
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
