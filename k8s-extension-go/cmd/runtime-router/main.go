package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
)

type route struct {
	Model    string `json:"model"`
	Endpoint string `json:"endpoint"`
}

type modelMetrics struct {
	mu           sync.Mutex
	Arrivals     []time.Time
	Requests     int64
	Errors       int64
	Inflight     int64
	TotalLatency float64
}

type routerState struct {
	mu      sync.RWMutex
	routes  map[string]string
	metrics map[string]*modelMetrics
	window  time.Duration
	http    *http.Client
	kube    *kube.Client
	store   string
}

func main() {
	var addr string
	var window time.Duration
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.DurationVar(&window, "arrival-window", 60*time.Second, "arrival-rate window")
	flag.Parse()

	ns := strings.TrimSpace(os.Getenv("NAMESPACE"))
	if ns == "" {
		ns = "or-sim"
	}
	store := strings.TrimSpace(os.Getenv("ROUTE_STORE_CONFIGMAP"))
	if store == "" {
		store = "runtime-router-routes"
	}
	kubeClient, err := kube.NewInCluster(ns)
	if err != nil {
		log.Printf("runtime-router route persistence disabled: %v", err)
	}
	routes := initialRoutes()
	for model, endpoint := range loadStoredRoutes(kubeClient, ns, store) {
		routes[model] = endpoint
	}
	state := &routerState{
		routes:  routes,
		metrics: map[string]*modelMetrics{},
		window:  window,
		http:    &http.Client{Timeout: 30 * time.Second},
		kube:    kubeClient,
		store:   store,
	}
	if len(routes) > 0 {
		if err := state.persistRoutes(routes); err != nil {
			log.Printf("runtime-router startup route persist failed: %v", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("/infer/", state.handleInfer)
	mux.HandleFunc("/routes", state.handleRouteSnapshot)
	mux.HandleFunc("/control/routes", state.handleRoutes)
	mux.HandleFunc("/metrics/demand", state.handleDemand)
	mux.HandleFunc("/metrics/profile-observations", state.handleProfileObservations)

	log.Printf("runtime-router listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *routerState) handleInfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "POST required"})
		return
	}
	model := strings.TrimPrefix(r.URL.Path, "/infer/")
	model = strings.Trim(model, "/")
	if model == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing model"})
		return
	}
	endpoint, ok := s.routeFor(model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown model", "model": model})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	metrics := s.metricsFor(model)
	started := time.Now()
	metrics.begin(started)
	status := http.StatusBadGateway
	var responseBody []byte
	defer func() {
		metrics.finish(time.Since(started), status >= 500)
	}()

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint+"/infer", bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "model": model, "endpoint": endpoint})
		return
	}
	defer resp.Body.Close()
	responseBody, err = io.ReadAll(resp.Body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	status = resp.StatusCode
	w.Header().Set("content-type", resp.Header.Get("content-type"))
	if w.Header().Get("content-type") == "" {
		w.Header().Set("content-type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(responseBody)
}

func (s *routerState) handleRouteSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET required"})
		return
	}
	now := time.Now()
	writeJSON(w, http.StatusOK, map[string]any{
		"routes":      s.routeSnapshot(now),
		"generatedAt": now.Format(time.RFC3339Nano),
	})
}

func (s *routerState) handleRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		defer s.mu.RUnlock()
		routes := make([]route, 0, len(s.routes))
		for model, endpoint := range s.routes {
			routes = append(routes, route{Model: model, Endpoint: endpoint})
		}
		writeJSON(w, http.StatusOK, map[string]any{"routes": routes})
	case http.MethodPut, http.MethodPost:
		var input route
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if input.Model == "" || input.Endpoint == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model and endpoint are required"})
			return
		}
		s.mu.Lock()
		s.routes[input.Model] = strings.TrimRight(input.Endpoint, "/")
		routes := s.copyRoutesLocked()
		s.mu.Unlock()
		if err := s.persistRoutes(routes); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, input)
	case http.MethodDelete:
		model := strings.TrimSpace(r.URL.Query().Get("model"))
		if model == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model query parameter is required"})
			return
		}
		s.mu.Lock()
		delete(s.routes, model)
		routes := s.copyRoutesLocked()
		s.mu.Unlock()
		if err := s.persistRoutes(routes); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"model": model, "deleted": true})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET, PUT, POST, or DELETE required"})
	}
}

func (s *routerState) routeSnapshot(now time.Time) []map[string]any {
	metricsByModel := s.snapshotMetrics(now)
	s.mu.RLock()
	routes := make(map[string]string, len(s.routes))
	for model, endpoint := range s.routes {
		routes[model] = endpoint
	}
	s.mu.RUnlock()

	models := make([]string, 0, len(routes))
	for model := range routes {
		models = append(models, model)
	}
	sort.Strings(models)

	out := make([]map[string]any, 0, len(models))
	for _, model := range models {
		endpoint := strings.TrimRight(routes[model], "/")
		row := metricsRow(model, metricsByModel[model], s.window)
		row["endpoint"] = endpoint
		row["active"] = true
		row["acceptingNew"] = true
		for key, value := range s.runtimeMetrics(endpoint) {
			row[key] = value
		}
		out = append(out, row)
	}
	return out
}

func (s *routerState) copyRoutesLocked() map[string]string {
	out := make(map[string]string, len(s.routes))
	for model, endpoint := range s.routes {
		out[model] = endpoint
	}
	return out
}

func (s *routerState) persistRoutes(routes map[string]string) error {
	if s.kube == nil {
		return nil
	}
	raw, err := json.Marshal(routes)
	if err != nil {
		return err
	}
	body := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      s.store,
			"namespace": s.kube.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":      "migrant-runtime-router",
				"migrant.io/routing-state":    "true",
				"app.kubernetes.io/component": "runtime-router",
			},
		},
		"data": map[string]any{
			"routes.json": string(raw),
			"updatedAt":   time.Now().Format(time.RFC3339Nano),
		},
	}
	return s.kube.Upsert(configMapPath(s.kube.Namespace(), s.store), body, nil)
}

func (s *routerState) handleDemand(w http.ResponseWriter, _ *http.Request) {
	now := time.Now()
	models := []map[string]any{}
	for model, metrics := range s.snapshotMetrics(now) {
		models = append(models, metricsRow(model, metrics, s.window))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"windowSeconds": s.window.Seconds(),
		"models":        models,
		"generatedAt":   now.Format(time.RFC3339Nano),
	})
}

func (s *routerState) handleProfileObservations(w http.ResponseWriter, _ *http.Request) {
	now := time.Now()
	items := []map[string]any{}
	for model, metrics := range s.snapshotMetrics(now) {
		row := metricsRow(model, metrics, s.window)
		row["sampleCount"] = metrics.Requests
		row["confidence"] = confidence(metrics.Requests)
		if endpoint, ok := s.routeFor(model); ok {
			row["endpoint"] = endpoint
			for key, value := range s.runtimeMetrics(endpoint) {
				row[key] = value
			}
		}
		items = append(items, row)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"observations": items,
		"generatedAt":  now.Format(time.RFC3339Nano),
	})
}

func (s *routerState) runtimeMetrics(endpoint string) map[string]any {
	ctxClient := http.Client{Timeout: 3 * time.Second}
	resp, err := ctxClient.Get(strings.TrimRight(endpoint, "/") + "/metrics")
	if err != nil {
		return map[string]any{"runtimeMetricsAvailable": false, "runtimeMetricsError": err.Error()}
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return map[string]any{"runtimeMetricsAvailable": false, "runtimeMetricsError": err.Error()}
	}
	out := map[string]any{"runtimeMetricsAvailable": true}
	for _, key := range []string{"batchSize", "migUuid", "slotResource", "avgLatencyMs", "requests", "errors"} {
		if value, ok := payload[key]; ok {
			out["runtime."+key] = value
		}
	}
	return out
}

func (s *routerState) routeFor(model string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	endpoint, ok := s.routes[model]
	return strings.TrimRight(endpoint, "/"), ok
}

func (s *routerState) metricsFor(model string) *modelMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics[model] == nil {
		s.metrics[model] = &modelMetrics{}
	}
	return s.metrics[model]
}

func (s *routerState) snapshotMetrics(now time.Time) map[string]modelMetrics {
	s.mu.RLock()
	models := make([]string, 0, len(s.metrics))
	for model := range s.metrics {
		models = append(models, model)
	}
	s.mu.RUnlock()
	out := map[string]modelMetrics{}
	for _, model := range models {
		m := s.metricsFor(model)
		out[model] = m.snapshot(now, s.window)
	}
	return out
}

func (m *modelMetrics) begin(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Inflight++
	m.Arrivals = append(m.Arrivals, now)
}

func (m *modelMetrics) finish(latency time.Duration, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Inflight--
	m.Requests++
	if failed {
		m.Errors++
	}
	m.TotalLatency += float64(latency.Milliseconds())
}

func (m *modelMetrics) snapshot(now time.Time, window time.Duration) modelMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := now.Add(-window)
	kept := m.Arrivals[:0]
	for _, arrival := range m.Arrivals {
		if arrival.After(cutoff) {
			kept = append(kept, arrival)
		}
	}
	m.Arrivals = kept
	cp := *m
	cp.Arrivals = append([]time.Time(nil), kept...)
	return cp
}

func metricsRow(model string, metrics modelMetrics, window time.Duration) map[string]any {
	avgLatency := 0.0
	if metrics.Requests > 0 {
		avgLatency = metrics.TotalLatency / float64(metrics.Requests)
	}
	errorRate := 0.0
	if metrics.Requests > 0 {
		errorRate = float64(metrics.Errors) / float64(metrics.Requests)
	}
	return map[string]any{
		"model":        model,
		"arrivalRate":  round(float64(len(metrics.Arrivals))/window.Seconds(), 4),
		"requests":     metrics.Requests,
		"errors":       metrics.Errors,
		"errorRate":    round(errorRate, 4),
		"inflight":     metrics.Inflight,
		"queued":       0,
		"avgLatencyMs": round(avgLatency, 3),
	}
}

func confidence(samples int64) string {
	switch {
	case samples >= 100:
		return "high"
	case samples >= 20:
		return "medium"
	case samples > 0:
		return "low"
	default:
		return "none"
	}
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	raw, _ := json.Marshal(payload)
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func envDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return strings.TrimRight(value, "/")
}

func initialRoutes() map[string]string {
	routes := map[string]string{}
	for _, item := range []struct {
		model string
		key   string
	}{
		{model: "gpt2", key: "GPT2_ENDPOINT"},
		{model: "resnet50", key: "RESNET50_ENDPOINT"},
		{model: "llama", key: "LLAMA_ENDPOINT"},
	} {
		value := strings.TrimSpace(os.Getenv(item.key))
		if value != "" {
			routes[item.model] = strings.TrimRight(value, "/")
		}
	}
	return routes
}

func loadStoredRoutes(client *kube.Client, ns, name string) map[string]string {
	if client == nil {
		return map[string]string{}
	}
	var cm map[string]any
	status, err := client.Get(configMapPath(ns, name), &cm)
	if err != nil {
		if status != http.StatusNotFound {
			log.Printf("runtime-router route store load failed: %v", err)
		}
		return map[string]string{}
	}
	raw := strings.TrimSpace(asString(asMap(cm["data"])["routes.json"]))
	if raw == "" {
		return map[string]string{}
	}
	out := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		log.Printf("runtime-router route store decode failed: %v", err)
		return map[string]string{}
	}
	for model, endpoint := range out {
		out[model] = strings.TrimRight(endpoint, "/")
	}
	return out
}

func configMapPath(ns, name string) string {
	return "/api/v1/namespaces/" + ns + "/configmaps/" + name
}

func round(value float64, digits int) float64 {
	pow := math.Pow10(digits)
	return math.Round(value*pow) / pow
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
