package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

type runtimeState struct {
	mu           sync.Mutex
	model        string
	batchSize    int
	startedAt    time.Time
	requests     int64
	errors       int64
	totalLatency float64
}

func main() {
	var addr string
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.Parse()

	state := &runtimeState{
		model:     envDefault("MODEL_NAME", "gpt2"),
		batchSize: envIntDefault("BATCH_SIZE", 4),
		startedAt: time.Now(),
	}
	state.startCUDAWorker()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", state.healthz)
	mux.HandleFunc("/metrics", state.metrics)
	mux.HandleFunc("/infer", state.infer)
	mux.HandleFunc("/control/batch", state.controlBatch)
	if err := http.ListenAndServe(addr, mux); err != nil {
		panic(err)
	}
}

func (s *runtimeState) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                   true,
		"model":                s.model,
		"cudaWorker":           envDefault("ENABLE_CUDA_SPIN", "true"),
		"nvidiaVisibleDevices": os.Getenv("NVIDIA_VISIBLE_DEVICES"),
		"orSimMIGUUID":         os.Getenv("OR_SIM_MIG_UUID"),
		"orSimSlot":            os.Getenv("OR_SIM_SLOT"),
		"orSimSlotResource":    os.Getenv("OR_SIM_SLOT_RESOURCE"),
		"orSimDeviceResource":  os.Getenv("OR_SIM_DEVICE_RESOURCE"),
		"orSimExpectedMIGUUID": os.Getenv("OR_SIM_EXPECTED_MIG_UUID"),
		"orSimPhysicalGpuID":   os.Getenv("OR_SIM_PHYSICAL_GPU_ID"),
	})
}

func (s *runtimeState) metrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	avg := 0.0
	if s.requests > 0 {
		avg = s.totalLatency / float64(s.requests)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model":          s.model,
		"uptimeSeconds":  time.Since(s.startedAt).Seconds(),
		"requests":       s.requests,
		"errors":         s.errors,
		"avgLatencyMs":   avg,
		"batchSize":      s.batchSize,
		"migUuid":        os.Getenv("OR_SIM_MIG_UUID"),
		"slotResource":   os.Getenv("OR_SIM_SLOT_RESOURCE"),
		"deviceResource": os.Getenv("OR_SIM_DEVICE_RESOURCE"),
	})
}

func (s *runtimeState) startCUDAWorker() {
	if envDefault("ENABLE_CUDA_SPIN", "true") == "false" {
		return
	}
	if _, err := os.Stat("/cuda-spin"); err != nil {
		log.Printf("cuda worker disabled: /cuda-spin not present: %v", err)
		return
	}
	cmd := exec.Command("/cuda-spin", envDefault("CUDA_SPIN_DEVICE", "0"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Printf("cuda worker failed to start: %v", err)
		return
	}
	log.Printf("cuda worker started for model=%s pid=%d visible=%s mig=%s", s.model, cmd.Process.Pid, os.Getenv("NVIDIA_VISIBLE_DEVICES"), os.Getenv("OR_SIM_MIG_UUID"))
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("cuda worker exited: %v", err)
		}
	}()
}

func (s *runtimeState) infer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "POST required"})
		return
	}
	started := time.Now()
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		s.record(time.Since(started), true)
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "model": s.model})
		return
	}

	base := map[string]int{"gpt2": 35, "resnet50": 18, "llama": 120}[s.model]
	if base == 0 {
		base = 50
	}
	sleepMs := float64(base)*float64(max(1, s.batchSize))/4.0 + float64(rand.Intn(max(4, base/5)))
	time.Sleep(time.Duration(sleepMs * float64(time.Millisecond)))
	latency := time.Since(started)
	s.record(latency, false)

	writeJSON(w, http.StatusOK, map[string]any{
		"model":     s.model,
		"batchSize": s.batchSize,
		"latencyMs": float64(latency.Microseconds()) / 1000.0,
		"input":     payload,
		"output":    s.output(payload),
	})
}

func (s *runtimeState) controlBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "POST or PUT required"})
		return
	}
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	next := intValue(payload["batchSize"])
	if next <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "batchSize must be positive"})
		return
	}
	s.mu.Lock()
	previous := s.batchSize
	s.batchSize = next
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"model":             s.model,
		"previousBatchSize": previous,
		"batchSize":         next,
		"applied":           true,
	})
}

func (s *runtimeState) output(payload map[string]any) map[string]any {
	switch s.model {
	case "resnet50":
		return map[string]any{"class": "tabby", "confidence": 0.91}
	case "llama":
		return map[string]any{"text": "llama-runtime cuda-backed completion", "tokens": payload["max_tokens"]}
	default:
		return map[string]any{"text": "gpt2-runtime cuda-backed completion", "tokens": payload["max_tokens"]}
	}
}

func (s *runtimeState) record(latency time.Duration, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	if failed {
		s.errors++
	}
	s.totalLatency += float64(latency.Microseconds()) / 1000.0
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	raw, _ := json.Marshal(payload)
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func envDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envIntDefault(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

func intValue(v any) int {
	switch value := v.(type) {
	case float64:
		return int(value)
	case int:
		return value
	case string:
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return 0
}
