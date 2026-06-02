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

type routeEndpoint struct {
	Model           string  `json:"model"`
	RuntimeID       string  `json:"runtimeId"`
	Endpoint        string  `json:"endpoint"`
	Weight          float64 `json:"weight,omitempty"`
	Capacity        float64 `json:"capacity,omitempty"`
	Profile         string  `json:"profile,omitempty"`
	BatchSize       int     `json:"batchSize,omitempty"`
	GPU             string  `json:"gpu,omitempty"`
	SlotResource    string  `json:"slotResource,omitempty"`
	DeviceResource  string  `json:"deviceResource,omitempty"`
	ExpectedMIGUUID string  `json:"expectedMigUuid,omitempty"`
	Active          bool    `json:"active"`
	AcceptingNew    bool    `json:"acceptingNew"`
	Draining        bool    `json:"draining,omitempty"`
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
	mu              sync.RWMutex
	routes          map[string][]routeEndpoint
	metrics         map[string]*modelMetrics
	endpointMetrics map[string]*modelMetrics
	window          time.Duration
	http            *http.Client
	kube            *kube.Client
	store           string
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
	for model, endpoints := range loadStoredRoutes(kubeClient, ns, store) {
		routes[model] = upsertEndpoints(routes[model], endpoints...)
	}
	state := &routerState{
		routes:          routes,
		metrics:         map[string]*modelMetrics{},
		endpointMetrics: map[string]*modelMetrics{},
		window:          window,
		http:            &http.Client{Timeout: 30 * time.Second},
		kube:            kubeClient,
		store:           store,
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
	selected, ok := s.routeFor(model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown model", "model": model})
		return
	}
	endpoint := strings.TrimRight(selected.Endpoint, "/")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	metrics := s.metricsFor(model)
	endpointMetrics := s.metricsForEndpoint(selected.RuntimeID)
	started := time.Now()
	metrics.begin(started)
	endpointMetrics.begin(started)
	status := http.StatusBadGateway
	var responseBody []byte
	defer func() {
		metrics.finish(time.Since(started), status >= 500)
		endpointMetrics.finish(time.Since(started), status >= 500)
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
		routes := make([]routeEndpoint, 0)
		for _, endpoints := range s.routes {
			routes = append(routes, endpoints...)
		}
		writeJSON(w, http.StatusOK, map[string]any{"routes": routes})
	case http.MethodPut, http.MethodPost:
		var input routeEndpoint
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if input.Model == "" || input.Endpoint == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model and endpoint are required"})
			return
		}
		input = normalizeEndpoint(input)
		s.mu.Lock()
		s.routes[input.Model] = upsertEndpoints(s.routes[input.Model], input)
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
		runtimeID := strings.TrimSpace(r.URL.Query().Get("runtimeId"))
		s.mu.Lock()
		if runtimeID == "" {
			delete(s.routes, model)
		} else {
			s.routes[model] = deleteEndpoint(s.routes[model], runtimeID)
			if len(s.routes[model]) == 0 {
				delete(s.routes, model)
			}
		}
		routes := s.copyRoutesLocked()
		s.mu.Unlock()
		if err := s.persistRoutes(routes); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"model": model, "runtimeId": runtimeID, "deleted": true})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET, PUT, POST, or DELETE required"})
	}
}

func (s *routerState) routeSnapshot(now time.Time) []map[string]any {
	metricsByModel := s.snapshotMetrics(now)
	metricsByEndpoint := s.snapshotEndpointMetrics(now)
	s.mu.RLock()
	routes := make(map[string][]routeEndpoint, len(s.routes))
	for model, endpoints := range s.routes {
		routes[model] = append([]routeEndpoint(nil), endpoints...)
	}
	s.mu.RUnlock()

	models := make([]string, 0, len(routes))
	for model := range routes {
		models = append(models, model)
	}
	sort.Strings(models)

	out := make([]map[string]any, 0, len(models))
	for _, model := range models {
		endpoints := append([]routeEndpoint(nil), routes[model]...)
		sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].RuntimeID < endpoints[j].RuntimeID })
		for _, endpoint := range endpoints {
			row := metricsRow(model, metricsByModel[model], s.window)
			endpointMetrics := metricsByEndpoint[endpoint.RuntimeID]
			row["runtimeId"] = endpoint.RuntimeID
			row["endpoint"] = strings.TrimRight(endpoint.Endpoint, "/")
			row["weight"] = effectiveWeight(endpoint)
			row["capacity"] = endpoint.Capacity
			row["profile"] = endpoint.Profile
			row["batchSize"] = endpoint.BatchSize
			row["gpu"] = endpoint.GPU
			row["slotResource"] = endpoint.SlotResource
			row["deviceResource"] = endpoint.DeviceResource
			row["expectedMigUuid"] = endpoint.ExpectedMIGUUID
			row["active"] = endpoint.Active
			row["acceptingNew"] = endpoint.AcceptingNew
			row["draining"] = endpoint.Draining
			row["endpointRequests"] = endpointMetrics.Requests
			row["endpointInflight"] = endpointMetrics.Inflight
			row["endpointAvgLatencyMs"] = round(avgLatency(endpointMetrics), 3)
			for key, value := range s.runtimeMetrics(endpoint.Endpoint) {
				row[key] = value
			}
			if endpointLatency := asFloat(row["endpointAvgLatencyMs"]); endpointLatency > 0 {
				if runtimeLatency := asFloat(row["runtime.runtimeLatencyMs"]); runtimeLatency > 0 {
					row["networkOverheadMs"] = round(math.Max(0, endpointLatency-runtimeLatency), 3)
				}
			}
			out = append(out, row)
		}
	}
	return out
}

func (s *routerState) copyRoutesLocked() map[string][]routeEndpoint {
	out := make(map[string][]routeEndpoint, len(s.routes))
	for model, endpoints := range s.routes {
		out[model] = append([]routeEndpoint(nil), endpoints...)
	}
	return out
}

func (s *routerState) persistRoutes(routes map[string][]routeEndpoint) error {
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
			row["runtimeId"] = endpoint.RuntimeID
			row["endpoint"] = endpoint.Endpoint
			row["profile"] = endpoint.Profile
			row["batchSize"] = endpoint.BatchSize
			row["slotResource"] = endpoint.SlotResource
			row["deviceResource"] = endpoint.DeviceResource
			row["expectedMigUuid"] = endpoint.ExpectedMIGUUID
			for key, value := range s.runtimeMetrics(endpoint.Endpoint) {
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
	for _, key := range []string{
		"model", "runtimeId", "runtimeMode", "torchvisionModel", "weightsMode", "device", "imageSize",
		"batchSize", "migUuid", "slotResource", "deviceResource", "expectedMigUuid",
		"avgLatencyMs", "runtimeLatencyMs", "runtimeThroughput", "lastRuntimeLatencyMs",
		"requests", "errors", "loaded", "loadError",
	} {
		if value, ok := payload[key]; ok {
			out["runtime."+key] = value
		}
	}
	return out
}

func (s *routerState) routeFor(model string) (routeEndpoint, bool) {
	s.mu.RLock()
	endpoints := append([]routeEndpoint(nil), s.routes[model]...)
	s.mu.RUnlock()
	if len(endpoints) == 0 {
		return routeEndpoint{}, false
	}
	var best routeEndpoint
	bestScore := math.Inf(1)
	found := false
	for _, endpoint := range endpoints {
		if !endpoint.Active || !endpoint.AcceptingNew || endpoint.Draining {
			continue
		}
		metrics := s.metricsForEndpoint(endpoint.RuntimeID).snapshot(time.Now(), s.window)
		score := float64(metrics.Inflight) / effectiveWeight(endpoint)
		if !found || score < bestScore || (score == bestScore && endpoint.RuntimeID < best.RuntimeID) {
			best = endpoint
			bestScore = score
			found = true
		}
	}
	if found {
		return best, true
	}
	for _, endpoint := range endpoints {
		if endpoint.Active && !endpoint.Draining {
			return endpoint, true
		}
	}
	return routeEndpoint{}, false
}

func (s *routerState) metricsFor(model string) *modelMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics[model] == nil {
		s.metrics[model] = &modelMetrics{}
	}
	return s.metrics[model]
}

func (s *routerState) metricsForEndpoint(runtimeID string) *modelMetrics {
	if runtimeID == "" {
		runtimeID = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endpointMetrics[runtimeID] == nil {
		s.endpointMetrics[runtimeID] = &modelMetrics{}
	}
	return s.endpointMetrics[runtimeID]
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

func (s *routerState) snapshotEndpointMetrics(now time.Time) map[string]modelMetrics {
	s.mu.RLock()
	ids := make([]string, 0, len(s.endpointMetrics))
	for id := range s.endpointMetrics {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	out := map[string]modelMetrics{}
	for _, id := range ids {
		m := s.metricsForEndpoint(id)
		out[id] = m.snapshot(now, s.window)
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

func initialRoutes() map[string][]routeEndpoint {
	routes := map[string][]routeEndpoint{}
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
			endpoint := normalizeEndpoint(routeEndpoint{Model: item.model, Endpoint: value})
			routes[item.model] = []routeEndpoint{endpoint}
		}
	}
	return routes
}

func loadStoredRoutes(client *kube.Client, ns, name string) map[string][]routeEndpoint {
	if client == nil {
		return map[string][]routeEndpoint{}
	}
	var cm map[string]any
	status, err := client.Get(configMapPath(ns, name), &cm)
	if err != nil {
		if status != http.StatusNotFound {
			log.Printf("runtime-router route store load failed: %v", err)
		}
		return map[string][]routeEndpoint{}
	}
	raw := strings.TrimSpace(asString(asMap(cm["data"])["routes.json"]))
	if raw == "" {
		return map[string][]routeEndpoint{}
	}
	out := map[string][]routeEndpoint{}
	if err := json.Unmarshal([]byte(raw), &out); err == nil {
		for model, endpoints := range out {
			normalized := []routeEndpoint{}
			for _, endpoint := range endpoints {
				endpoint.Model = firstNonEmpty(endpoint.Model, model)
				normalized = append(normalized, normalizeEndpoint(endpoint))
			}
			out[model] = normalized
		}
		return out
	}
	legacy := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		log.Printf("runtime-router route store decode failed: %v", err)
		return map[string][]routeEndpoint{}
	}
	for model, endpoint := range legacy {
		out[model] = []routeEndpoint{normalizeEndpoint(routeEndpoint{Model: model, Endpoint: endpoint})}
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

func avgLatency(metrics modelMetrics) float64 {
	if metrics.Requests == 0 {
		return 0
	}
	return metrics.TotalLatency / float64(metrics.Requests)
}

func normalizeEndpoint(endpoint routeEndpoint) routeEndpoint {
	endpoint.Endpoint = strings.TrimRight(endpoint.Endpoint, "/")
	if endpoint.RuntimeID == "" {
		endpoint.RuntimeID = runtimeIDFromEndpoint(endpoint.Model, endpoint.Endpoint)
	}
	if endpoint.Weight <= 0 {
		endpoint.Weight = endpoint.Capacity
	}
	if endpoint.Weight <= 0 {
		endpoint.Weight = 1
	}
	if !endpoint.Active {
		endpoint.Active = true
	}
	if !endpoint.AcceptingNew && !endpoint.Draining {
		endpoint.AcceptingNew = true
	}
	return endpoint
}

func upsertEndpoints(existing []routeEndpoint, updates ...routeEndpoint) []routeEndpoint {
	out := append([]routeEndpoint(nil), existing...)
	for _, update := range updates {
		update = normalizeEndpoint(update)
		replaced := false
		for idx := range out {
			if out[idx].RuntimeID == update.RuntimeID {
				out[idx] = update
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, update)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuntimeID < out[j].RuntimeID })
	return out
}

func deleteEndpoint(existing []routeEndpoint, runtimeID string) []routeEndpoint {
	out := []routeEndpoint{}
	for _, endpoint := range existing {
		if endpoint.RuntimeID != runtimeID {
			out = append(out, endpoint)
		}
	}
	return out
}

func effectiveWeight(endpoint routeEndpoint) float64 {
	if endpoint.Weight > 0 {
		return endpoint.Weight
	}
	if endpoint.Capacity > 0 {
		return endpoint.Capacity
	}
	return 1
}

func runtimeIDFromEndpoint(model, endpoint string) string {
	raw := strings.ToLower(model + "-" + endpoint)
	replacer := strings.NewReplacer("http://", "", "https://", "", ":", "-", "/", "-", ".", "-")
	raw = replacer.Replace(raw)
	raw = strings.Trim(raw, "-")
	if raw == "" {
		return "runtime"
	}
	return raw
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		n, _ := x.Float64()
		return n
	default:
		return 0
	}
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
