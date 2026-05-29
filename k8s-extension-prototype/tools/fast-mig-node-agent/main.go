package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type result struct {
	Command       string    `json:"command"`
	GPUIndex      string    `json:"gpuIndex"`
	Template      string    `json:"template,omitempty"`
	ProfileIDs    string    `json:"profileIds,omitempty"`
	CreateSeconds float64   `json:"createSeconds,omitempty"`
	DeleteSeconds float64   `json:"deleteSeconds,omitempty"`
	Success       bool      `json:"success"`
	Message       string    `json:"message,omitempty"`
	NvidiaSMIL    string    `json:"nvidiaSmiL,omitempty"`
	MIGSlots      []migSlot `json:"migSlots,omitempty"`
}

type gpuInstance struct {
	Name       string
	ProfileID  string
	InstanceID string
	Start      int
	Size       int
}

type migSlot struct {
	SlotStart     int    `json:"slotStart"`
	SlotEnd       int    `json:"slotEnd"`
	Profile       string `json:"profile"`
	MIGDeviceUUID string `json:"migDeviceUuid,omitempty"`
	GPUInstanceID string `json:"gpuInstanceId,omitempty"`
	ProfileID     string `json:"profileId,omitempty"`
	Source        string `json:"source,omitempty"`
}

type slotSpec struct {
	Start      int
	Size       int
	Profile    string
	MIGUUID    string
	InstanceID string
}

var templateSlotSpecs = map[string]string{
	"7":             "0:8:7g",
	"4+3":           "0:4:4g,4:4:3g",
	"4+2+1":         "0:4:4g,4:2:2g,6:1:1g",
	"4+1+1+1":       "0:4:4g,4:1:1g,5:1:1g,6:1:1g",
	"3+3":           "0:4:3g,4:4:3g",
	"3+2+1":         "0:4:3g,4:2:2g,6:1:1g",
	"3+1+1+1":       "0:4:3g,4:1:1g,5:1:1g,6:1:1g",
	"2+2+3":         "0:2:2g,2:2:2g,4:4:3g",
	"3+2+1+1":       "0:2:2g,2:1:1g,3:1:1g,4:4:3g",
	"3+1+1+1+1":     "0:1:1g,1:1:1g,2:1:1g,3:1:1g,4:4:3g",
	"2+2+2+1":       "0:2:2g,2:2:2g,4:2:2g,6:1:1g",
	"2+2+1+1+1":     "0:2:2g,2:1:1g,3:1:1g,4:2:2g,6:1:1g",
	"2+1+1+1+1+1":   "0:2:2g,2:1:1g,3:1:1g,4:1:1g,5:1:1g,6:1:1g",
	"1+1+1+1+1+1+1": "0:1:1g,1:1:1g,2:1:1g,3:1:1g,4:1:1g,5:1:1g,6:1:1g",
}

var profileToCreateID = map[string]string{
	"1g": "19",
	"2g": "14",
	"3g": "9",
	"4g": "5",
	"7g": "0",
}

var profileToSize = map[string]int{
	"1g": 1,
	"2g": 2,
	"3g": 3,
	"4g": 4,
	"7g": 7,
}

var gpuIndexRe = regexp.MustCompile(`^[0-9]+$`)
var giLineRe = regexp.MustCompile(`^\|\s+([0-9]+)\s+(MIG [0-9]+g\.[0-9]+gb)\s+([0-9]+)\s+([0-9]+)\s+([0-9]+):([0-9]+)\s+\|`)
var migUUIDLineRe = regexp.MustCompile(`^MIG ([0-9]+g\.[0-9]+gb)\s+Device\s+[0-9]+:\s+\(UUID:\s+([^)]+)\)`)
var profileRe = regexp.MustCompile(`([12347]g)`)

func main() {
	var gpuIndex string
	var jsonOut bool
	var lockPath string
	var slotDevicePlugin bool
	var nodeName string
	var pluginDir string
	var scanInterval time.Duration
	flag.StringVar(&gpuIndex, "gpu-index", "0", "A100 GPU index to mutate")
	flag.BoolVar(&jsonOut, "json", false, "emit JSON result")
	flag.StringVar(&lockPath, "lock-file", "/tmp/or-sim-fast-mig-node-agent.lock", "host-local lock file")
	flag.BoolVar(&slotDevicePlugin, "slot-device-plugin", false, "run the or-sim exact MIG slot kubelet device plugin")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "Kubernetes node name used in or-sim slot resource names")
	flag.StringVar(&pluginDir, "device-plugin-dir", deviceplugin.DevicePluginPath, "kubelet device plugin directory")
	flag.DurationVar(&scanInterval, "slot-scan-interval", 5*time.Second, "MIG slot device plugin rescan interval")
	flag.Parse()

	if slotDevicePlugin {
		if nodeName == "" {
			fail(jsonOut, result{Success: false, Message: "--node-name or NODE_NAME is required for --slot-device-plugin"}, 2)
		}
		if err := runSlotDevicePlugin(nodeName, pluginDir, scanInterval); err != nil {
			fail(jsonOut, result{Success: false, Message: err.Error()}, 1)
		}
		return
	}

	if flag.NArg() < 1 {
		fail(jsonOut, result{Success: false, Message: "missing command"}, 2)
	}
	command := flag.Arg(0)
	if !gpuIndexRe.MatchString(gpuIndex) {
		fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "gpu-index must be a non-negative integer"}, 2)
	}

	unlock, err := acquireLock(lockPath)
	if err != nil {
		fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error()}, 1)
	}
	defer unlock()

	switch command {
	case "list":
		out, err := run("nvidia-smi", "-L")
		res := result{Command: command, GPUIndex: gpuIndex, Success: err == nil, NvidiaSMIL: out}
		if err != nil {
			res.Message = err.Error()
		}
		printResult(jsonOut, res)
		if err != nil {
			os.Exit(1)
		}
	case "clear":
		clearMIG(gpuIndex)
		out, err := run("nvidia-smi", "-L")
		res := result{Command: command, GPUIndex: gpuIndex, Success: err == nil, NvidiaSMIL: out}
		if err != nil {
			res.Message = err.Error()
		}
		printResult(jsonOut, res)
		if err != nil {
			os.Exit(1)
		}
	case "apply":
		res := result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "apply TEMPLATE has been removed; use apply-slots CREATE_SPEC"}
		printResult(jsonOut, res)
		os.Exit(2)
	case "apply-slots":
		if flag.NArg() != 2 {
			fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "apply-slots requires exactly one CREATE_SPEC"}, 2)
		}
		res := applySlots(gpuIndex, flag.Arg(1))
		printResult(jsonOut, res)
		if !res.Success {
			os.Exit(1)
		}
	case "benchmark":
		names := templateNames()
		for _, name := range names {
			res := benchmarkTemplate(gpuIndex, name)
			printResult(jsonOut, res)
			if !res.Success {
				clearMIG(gpuIndex)
			}
		}
		clearMIG(gpuIndex)
	case "partial-3g-to-2+1":
		if flag.NArg() != 1 {
			fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "partial-3g-to-2+1 does not accept positional arguments"}, 2)
		}
		res := partial3gTo2Plus1(gpuIndex)
		printResult(jsonOut, res)
		if !res.Success {
			os.Exit(1)
		}
	case "patch-slots":
		if flag.NArg() != 4 {
			fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "patch-slots requires DELETE_SPEC CREATE_SPEC PRESERVE_SPEC"}, 2)
		}
		res := patchSlots(gpuIndex, flag.Arg(1), flag.Arg(2), flag.Arg(3))
		printResult(jsonOut, res)
		if !res.Success {
			os.Exit(1)
		}
	case "refresh-cdi":
		if flag.NArg() != 1 {
			fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "refresh-cdi does not accept positional arguments"}, 2)
		}
		res := refreshCDI(gpuIndex)
		printResult(jsonOut, res)
		if !res.Success {
			os.Exit(1)
		}
	default:
		fail(jsonOut, result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "unknown command: " + command}, 2)
	}
}

func applySlots(gpuIndex, createArg string) result {
	command := "apply-slots"
	createSpecs, err := parseSlotSpecs(createArg, false, true)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "invalid create spec: " + err.Error()}
	}
	if len(createSpecs) == 0 {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "apply-slots requires at least one create slot"}
	}
	if err := rejectOverlaps("create", createSpecs); err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error()}
	}
	clearMIG(gpuIndex)
	createSpec := createSpecArg(createSpecs)
	start := time.Now()
	out, err := run("nvidia-smi", "mig", "-cgi", createSpec, "-C", "-i", gpuIndex)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		clearMIG(gpuIndex)
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, CreateSeconds: elapsed, Success: false, Message: err.Error() + "\n" + out}
	}
	smi, smiErr := run("nvidia-smi", "-L")
	msg := strings.TrimSpace(out)
	if smiErr != nil {
		msg = msg + "\npost-apply nvidia-smi -L failed: " + smiErr.Error()
	}
	instances, rawGI, giErr := listGPUInstances(gpuIndex)
	if giErr != nil {
		msg = msg + "\npost-apply nvidia-smi mig -lgi failed: " + giErr.Error() + "\n" + rawGI
	}
	if giErr == nil {
		for _, spec := range createSpecs {
			if findInstanceBySlot(instances, spec) == nil {
				return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, CreateSeconds: elapsed, Success: false, NvidiaSMIL: smi, MIGSlots: migSlotsFromObservation(smi, instances, gpuIndex), Message: "created slot does not exist after apply: " + formatSlot(spec)}
			}
		}
	}
	return result{
		Command:       command,
		GPUIndex:      gpuIndex,
		ProfileIDs:    createSpec,
		CreateSeconds: elapsed,
		Success:       smiErr == nil && giErr == nil,
		Message:       msg,
		NvidiaSMIL:    smi,
		MIGSlots:      migSlotsFromObservation(smi, instances, gpuIndex),
	}
}

func benchmarkTemplate(gpuIndex, template string) result {
	createArg, ok := templateSlotSpecs[template]
	if !ok {
		return result{Command: "benchmark", GPUIndex: gpuIndex, Template: template, Success: false, Message: "unknown template"}
	}
	createSpecs, err := parseSlotSpecs(createArg, false, true)
	if err != nil {
		return result{Command: "benchmark", GPUIndex: gpuIndex, Template: template, Success: false, Message: "invalid template slot spec: " + err.Error()}
	}
	ids := createSpecArg(createSpecs)
	clearMIG(gpuIndex)
	createStart := time.Now()
	out, err := run("nvidia-smi", "mig", "-cgi", ids, "-C", "-i", gpuIndex)
	createElapsed := time.Since(createStart).Seconds()
	if err != nil {
		clearMIG(gpuIndex)
		return result{Command: "benchmark", GPUIndex: gpuIndex, Template: template, ProfileIDs: ids, CreateSeconds: createElapsed, Success: false, Message: err.Error() + "\n" + out}
	}
	deleteStart := time.Now()
	deleteErr := destroyMIG(gpuIndex)
	deleteElapsed := time.Since(deleteStart).Seconds()
	if deleteErr != nil {
		clearMIG(gpuIndex)
		return result{Command: "benchmark", GPUIndex: gpuIndex, Template: template, ProfileIDs: ids, CreateSeconds: createElapsed, DeleteSeconds: deleteElapsed, Success: false, Message: deleteErr.Error()}
	}
	return result{Command: "benchmark", GPUIndex: gpuIndex, Template: template, ProfileIDs: ids, CreateSeconds: createElapsed, DeleteSeconds: deleteElapsed, Success: true}
}

func partial3gTo2Plus1(gpuIndex string) result {
	command := "partial-3g-to-2+1"
	before, err := run("nvidia-smi", "-L")
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error() + "\n" + before}
	}
	beforeUUIDs := migUUIDsByProfile(before, gpuIndex)
	before4g := beforeUUIDs["4g.20gb"]
	if len(before4g) != 1 {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: fmt.Sprintf("expected exactly one preserved 4g.20gb instance, got %d", len(before4g))}
	}
	before3g := beforeUUIDs["3g.20gb"]
	if len(before3g) != 1 {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: fmt.Sprintf("expected exactly one replaceable 3g.20gb instance, got %d", len(before3g))}
	}

	activeByUUID, rawApps, err := activeComputeProcessesByUUID()
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "failed to check active compute processes: " + err.Error() + "\n" + rawApps}
	}
	if processes := activeByUUID[before3g[0]]; len(processes) > 0 {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: fmt.Sprintf("refusing to replace busy 3g.20gb %s; active processes: %s", before3g[0], strings.Join(processes, "; "))}
	}

	instances, rawGI, err := listGPUInstances(gpuIndex)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error() + "\n" + rawGI}
	}
	var target *gpuInstance
	for idx := range instances {
		inst := instances[idx]
		if inst.ProfileID == "9" && inst.Name == "MIG 3g.20gb" {
			if target != nil {
				return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "expected one 3g.20gb instance to replace, found multiple"}
			}
			target = &inst
		}
	}
	if target == nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "no 3g.20gb GPU instance found"}
	}
	if target.Size < 3 {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: fmt.Sprintf("3g placement is too small for 2+1 replacement: %d:%d", target.Start, target.Size)}
	}

	deleteStart := time.Now()
	if out, err := destroyGPUInstance(gpuIndex, target.InstanceID); err != nil {
		return result{Command: command, GPUIndex: gpuIndex, DeleteSeconds: time.Since(deleteStart).Seconds(), Success: false, Message: err.Error() + "\n" + out}
	}
	deleteElapsed := time.Since(deleteStart).Seconds()

	createSpec := fmt.Sprintf("14:%d,19:%d", target.Start, target.Start+2)
	createStart := time.Now()
	createOut, createErr := run("nvidia-smi", "mig", "-cgi", createSpec, "-C", "-i", gpuIndex)
	createElapsed := time.Since(createStart).Seconds()
	if createErr != nil {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, Message: createErr.Error() + "\n" + createOut}
	}

	after, err := run("nvidia-smi", "-L")
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, Message: err.Error() + "\n" + after}
	}
	afterUUIDs := migUUIDsByProfile(after, gpuIndex)
	after4g := afterUUIDs["4g.20gb"]
	if len(after4g) != 1 || after4g[0] != before4g[0] {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: fmt.Sprintf("preserved 4g UUID changed: before=%v after=%v", before4g, after4g)}
	}
	if len(afterUUIDs["2g.10gb"]) != 1 || len(afterUUIDs["1g.5gb"]) != 1 {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: "expected one 2g.10gb and one 1g.5gb after partial replacement"}
	}
	afterInstances, rawAfterGI, err := listGPUInstances(gpuIndex)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: err.Error() + "\n" + rawAfterGI}
	}
	return result{
		Command:       command,
		GPUIndex:      gpuIndex,
		Template:      "4+2+1",
		ProfileIDs:    createSpec,
		CreateSeconds: createElapsed,
		DeleteSeconds: deleteElapsed,
		Success:       true,
		Message:       fmt.Sprintf("preserved 4g UUID %s; replaced idle 3g UUID %s / GI %s at placement %d:%d", before4g[0], before3g[0], target.InstanceID, target.Start, target.Size),
		NvidiaSMIL:    after,
		MIGSlots:      migSlotsFromObservation(after, afterInstances, gpuIndex),
	}
}

func patchSlots(gpuIndex, deleteArg, createArg, preserveArg string) result {
	command := "patch-slots"
	deleteSpecs, err := parseSlotSpecs(deleteArg, true, true)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "invalid delete spec: " + err.Error()}
	}
	createSpecs, err := parseSlotSpecs(createArg, false, true)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "invalid create spec: " + err.Error()}
	}
	preserveSpecs, err := parseSlotSpecs(preserveArg, true, true)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "invalid preserve spec: " + err.Error()}
	}
	if len(deleteSpecs) == 0 && len(createSpecs) == 0 {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: "patch must delete or create at least one slot"}
	}
	if err := validateSlotPatch(deleteSpecs, createSpecs, preserveSpecs); err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error()}
	}

	before, err := run("nvidia-smi", "-L")
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error() + "\n" + before}
	}
	instances, rawGI, err := listGPUInstances(gpuIndex)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, Message: err.Error() + "\n" + rawGI}
	}
	for idx := range deleteSpecs {
		match := findInstanceBySlot(instances, deleteSpecs[idx])
		if match == nil {
			return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "delete slot does not exist: " + formatSlot(deleteSpecs[idx])}
		}
		deleteSpecs[idx].InstanceID = match.InstanceID
	}
	for _, spec := range preserveSpecs {
		if findInstanceBySlot(instances, spec) == nil {
			return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "preserve slot does not exist before patch: " + formatSlot(spec)}
		}
		if spec.MIGUUID != "" && !strings.Contains(before, spec.MIGUUID) {
			return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "preserve MIG UUID not found before patch: " + spec.MIGUUID}
		}
	}

	activeByUUID, rawApps, err := activeComputeProcessesByUUID()
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "failed to check active compute processes: " + err.Error() + "\n" + rawApps}
	}
	if len(activeByUUID) > 0 {
		for _, spec := range deleteSpecs {
			if spec.MIGUUID == "" {
				return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: "active compute processes exist; delete slots must include MIG UUIDs for safe busy checks"}
			}
			if processes := activeByUUID[spec.MIGUUID]; len(processes) > 0 {
				return result{Command: command, GPUIndex: gpuIndex, Success: false, NvidiaSMIL: before, Message: fmt.Sprintf("refusing to delete busy slot %s; active processes: %s", formatSlot(spec), strings.Join(processes, "; "))}
			}
		}
	}

	deleteStart := time.Now()
	for _, spec := range deleteSpecs {
		if out, err := destroyGPUInstance(gpuIndex, spec.InstanceID); err != nil {
			return result{Command: command, GPUIndex: gpuIndex, DeleteSeconds: time.Since(deleteStart).Seconds(), Success: false, Message: err.Error() + "\n" + out}
		}
	}
	deleteElapsed := time.Since(deleteStart).Seconds()

	createSpec := createSpecArg(createSpecs)
	createStart := time.Now()
	if createSpec != "" {
		createOut, createErr := run("nvidia-smi", "mig", "-cgi", createSpec, "-C", "-i", gpuIndex)
		if createErr != nil {
			return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: time.Since(createStart).Seconds(), Success: false, Message: createErr.Error() + "\n" + createOut}
		}
	}
	createElapsed := time.Since(createStart).Seconds()

	after, err := run("nvidia-smi", "-L")
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, Message: err.Error() + "\n" + after}
	}
	afterInstances, rawAfterGI, err := listGPUInstances(gpuIndex)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: err.Error() + "\n" + rawAfterGI}
	}
	for _, spec := range deleteSpecs {
		if findInstanceBySlot(afterInstances, spec) != nil {
			return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: "delete slot still exists after patch: " + formatSlot(spec)}
		}
	}
	for _, spec := range createSpecs {
		if findInstanceBySlot(afterInstances, spec) == nil {
			return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: "create slot missing after patch: " + formatSlot(spec)}
		}
	}
	for _, spec := range preserveSpecs {
		if findInstanceBySlot(afterInstances, spec) == nil {
			return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: "preserve slot missing after patch: " + formatSlot(spec)}
		}
		if spec.MIGUUID != "" && !strings.Contains(after, spec.MIGUUID) {
			return result{Command: command, GPUIndex: gpuIndex, ProfileIDs: createSpec, DeleteSeconds: deleteElapsed, CreateSeconds: createElapsed, Success: false, NvidiaSMIL: after, Message: "preserve MIG UUID missing after patch: " + spec.MIGUUID}
		}
	}
	msg := fmt.Sprintf("deleted [%s]; created [%s]; preserved [%s]", joinSlots(deleteSpecs), joinSlots(createSpecs), joinSlots(preserveSpecs))
	return result{
		Command:       command,
		GPUIndex:      gpuIndex,
		ProfileIDs:    createSpec,
		DeleteSeconds: deleteElapsed,
		CreateSeconds: createElapsed,
		Success:       true,
		Message:       msg,
		NvidiaSMIL:    after,
		MIGSlots:      migSlotsFromObservation(after, afterInstances, gpuIndex),
	}
}

func clearMIG(gpuIndex string) {
	_ = destroyMIG(gpuIndex)
}

func refreshCDI(gpuIndex string) result {
	command := "refresh-cdi"
	start := time.Now()
	tmpPath := "/tmp/or-sim-management.nvidia.com-gpu.yaml"
	outputPath := "/host/var/run/cdi/management.nvidia.com-gpu.yaml"
	out, err := run(
		"/host/usr/bin/nvidia-ctk",
		"cdi",
		"generate",
		"--driver-root=/host",
		"--dev-root=/host",
		"--vendor=management.nvidia.com",
		"--class=gpu",
		"--device-name-strategy=uuid",
		"--format=yaml",
		"--output="+tmpPath,
	)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, CreateSeconds: elapsed, Success: false, Message: err.Error() + "\n" + out}
	}
	spec, err := os.ReadFile(tmpPath)
	if err != nil {
		return result{Command: command, GPUIndex: gpuIndex, CreateSeconds: elapsed, Success: false, Message: err.Error()}
	}
	text := string(spec)
	for _, pair := range [][2]string{
		{"hostPath: /host/dev", "hostPath: /dev"},
		{"hostPath: /host/usr", "hostPath: /usr"},
		{"hostPath: /host/run", "hostPath: /run"},
		{"hostPath: /host/lib", "hostPath: /lib"},
	} {
		text = strings.ReplaceAll(text, pair[0], pair[1])
	}
	if err := os.MkdirAll("/host/var/run/cdi", 0755); err != nil {
		return result{Command: command, GPUIndex: gpuIndex, CreateSeconds: elapsed, Success: false, Message: err.Error()}
	}
	if err := os.WriteFile(outputPath, []byte(text), 0644); err != nil {
		return result{Command: command, GPUIndex: gpuIndex, CreateSeconds: elapsed, Success: false, Message: err.Error()}
	}
	elapsed = time.Since(start).Seconds()
	return result{Command: command, GPUIndex: gpuIndex, CreateSeconds: elapsed, Success: true, Message: "refreshed " + outputPath}
}

func destroyMIG(gpuIndex string) error {
	// nvidia-smi can return non-zero for one of the destroy subcommands even
	// when the final GPU state is empty. The actuator contract cares about the
	// postcondition, so verify with nvidia-smi -L after best-effort deletion.
	_, dciErr := run("nvidia-smi", "mig", "-dci", "-i", gpuIndex)
	_, dgiErr := run("nvidia-smi", "mig", "-dgi", "-i", gpuIndex)
	listOutput, listErr := run("nvidia-smi", "-L")
	if listErr != nil {
		return listErr
	}
	if gpuBlockHasMIG(listOutput, gpuIndex) {
		errs := []string{"target GPU still has MIG devices after destroy"}
		if dciErr != nil {
			errs = append(errs, dciErr.Error())
		}
		if dgiErr != nil {
			errs = append(errs, dgiErr.Error())
		}
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func destroyGPUInstance(gpuIndex, instanceID string) (string, error) {
	_, _ = run("nvidia-smi", "mig", "-dci", "-i", gpuIndex, "-gi", instanceID)
	var out string
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		out, err = run("nvidia-smi", "mig", "-dgi", "-i", gpuIndex, "-gi", instanceID)
		if err == nil {
			return out, nil
		}
		if !strings.Contains(out, "In use by another client") {
			return out, err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return out, err
}

func gpuBlockHasMIG(nvidiaSMIL, gpuIndex string) bool {
	lines := strings.Split(nvidiaSMIL, "\n")
	inTarget := false
	prefix := "GPU " + gpuIndex + ":"
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "GPU ") {
			inTarget = strings.HasPrefix(trimmed, prefix)
			continue
		}
		if inTarget && strings.HasPrefix(trimmed, "MIG ") {
			return true
		}
	}
	return false
}

func run(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	out := stdout.String() + stderr.String()
	if err != nil {
		return out, fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
	}
	return out, nil
}

func activeComputeProcessesByUUID() (map[string][]string, string, error) {
	out, err := run("nvidia-smi", "--query-compute-apps=gpu_uuid,pid,process_name", "--format=csv,noheader")
	if err != nil {
		if strings.Contains(out, "No running processes found") {
			return map[string][]string{}, out, nil
		}
		return nil, out, err
	}
	processes := map[string][]string{}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "No running processes found") {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 3 {
			continue
		}
		uuid := strings.TrimSpace(parts[0])
		pid := strings.TrimSpace(parts[1])
		name := strings.TrimSpace(strings.Join(parts[2:], ","))
		if uuid == "" || uuid == "[Not Supported]" {
			continue
		}
		processes[uuid] = append(processes[uuid], pid+" "+name)
	}
	return processes, out, nil
}

func listGPUInstances(gpuIndex string) ([]gpuInstance, string, error) {
	out, err := run("nvidia-smi", "mig", "-lgi", "-i", gpuIndex)
	if err != nil {
		return nil, out, err
	}
	instances := []gpuInstance{}
	for _, line := range strings.Split(out, "\n") {
		match := giLineRe.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		if match[1] != gpuIndex {
			continue
		}
		start := atoi(match[5])
		size := atoi(match[6])
		instances = append(instances, gpuInstance{
			Name:       match[2],
			ProfileID:  match[3],
			InstanceID: match[4],
			Start:      start,
			Size:       size,
		})
	}
	return instances, out, nil
}

func parseSlotSpecs(value string, allowUUID bool, allowPlacementSize bool) ([]slotSpec, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" || value == "none" {
		return []slotSpec{}, nil
	}
	out := []slotSpec{}
	for _, raw := range strings.Split(value, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parts := strings.Split(raw, ":")
		if len(parts) < 3 || len(parts) > 4 {
			return nil, fmt.Errorf("slot %q must be start:size:profile[:migUuid]", raw)
		}
		start := atoiStrict(parts[0])
		size := atoiStrict(parts[1])
		profile := canonicalProfile(parts[2])
		expectedSize, ok := profileToSize[profile]
		if !ok {
			return nil, fmt.Errorf("slot %q uses unsupported profile %q", raw, parts[2])
		}
		maxEnd := 7
		if allowPlacementSize {
			maxEnd = 8
		}
		if start < 0 || size <= 0 || start+size > maxEnd {
			return nil, fmt.Errorf("slot %q is outside A100 placement range 0..%d", raw, maxEnd)
		}
		if size != expectedSize && !(allowPlacementSize && size > expectedSize) {
			return nil, fmt.Errorf("slot %q size does not match profile %s", raw, profile)
		}
		spec := slotSpec{Start: start, Size: size, Profile: profile}
		if len(parts) == 4 {
			if !allowUUID {
				return nil, fmt.Errorf("slot %q must not include MIG UUID", raw)
			}
			spec.MIGUUID = strings.TrimSpace(parts[3])
			if spec.MIGUUID != "" && !strings.HasPrefix(spec.MIGUUID, "MIG-") {
				return nil, fmt.Errorf("slot %q has invalid MIG UUID", raw)
			}
		}
		out = append(out, spec)
	}
	return out, nil
}

func validateSlotPatch(deleteSpecs, createSpecs, preserveSpecs []slotSpec) error {
	if err := rejectOverlaps("delete", deleteSpecs); err != nil {
		return err
	}
	if err := rejectOverlaps("create", createSpecs); err != nil {
		return err
	}
	if err := rejectOverlaps("preserve", preserveSpecs); err != nil {
		return err
	}
	for _, create := range createSpecs {
		if !slotCoveredByUnion(create, deleteSpecs) {
			return fmt.Errorf("create slot %s is not fully covered by deleted space", formatSlot(create))
		}
	}
	for _, preserve := range preserveSpecs {
		for _, del := range deleteSpecs {
			if slotsOverlap(preserve, del) {
				return fmt.Errorf("preserve slot %s overlaps delete slot %s", formatSlot(preserve), formatSlot(del))
			}
		}
	}
	return nil
}

func slotCoveredByUnion(slot slotSpec, covering []slotSpec) bool {
	for slice := slot.Start; slice < slot.Start+slot.Size; slice++ {
		covered := false
		for _, candidate := range covering {
			if slice >= candidate.Start && slice < candidate.Start+candidate.Size {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func rejectOverlaps(name string, specs []slotSpec) error {
	for i := range specs {
		for j := i + 1; j < len(specs); j++ {
			if slotsOverlap(specs[i], specs[j]) {
				return fmt.Errorf("%s slots overlap: %s and %s", name, formatSlot(specs[i]), formatSlot(specs[j]))
			}
		}
	}
	return nil
}

func slotsOverlap(a, b slotSpec) bool {
	return a.Start < b.Start+b.Size && b.Start < a.Start+a.Size
}

func findInstanceBySlot(instances []gpuInstance, spec slotSpec) *gpuInstance {
	for idx := range instances {
		inst := instances[idx]
		if inst.Start == spec.Start && inst.Size == spec.Size && canonicalProfile(inst.Name) == spec.Profile {
			return &instances[idx]
		}
	}
	return nil
}

func createSpecArg(specs []slotSpec) string {
	parts := []string{}
	for _, spec := range specs {
		parts = append(parts, profileToCreateID[spec.Profile]+":"+fmt.Sprint(spec.Start))
	}
	return strings.Join(parts, ",")
}

func joinSlots(specs []slotSpec) string {
	parts := []string{}
	for _, spec := range specs {
		parts = append(parts, formatSlot(spec))
	}
	return strings.Join(parts, ",")
}

func formatSlot(spec slotSpec) string {
	text := fmt.Sprintf("%d:%d:%s", spec.Start, spec.Size, spec.Profile)
	if spec.MIGUUID != "" {
		text += ":" + spec.MIGUUID
	}
	return text
}

func canonicalProfile(value string) string {
	match := profileRe.FindStringSubmatch(value)
	if match == nil {
		return strings.TrimSpace(value)
	}
	return match[1]
}

func migUUIDsByProfile(nvidiaSMIL, gpuIndex string) map[string][]string {
	out := map[string][]string{}
	lines := strings.Split(nvidiaSMIL, "\n")
	inTarget := false
	prefix := "GPU " + gpuIndex + ":"
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "GPU ") {
			inTarget = strings.HasPrefix(trimmed, prefix)
			continue
		}
		if !inTarget {
			continue
		}
		match := migUUIDLineRe.FindStringSubmatch(trimmed)
		if match == nil {
			continue
		}
		out[match[1]] = append(out[match[1]], match[2])
	}
	return out
}

func migSlotsFromObservation(nvidiaSMIL string, instances []gpuInstance, gpuIndex string) []migSlot {
	uuidsByProfile := migUUIDsByProfile(nvidiaSMIL, gpuIndex)
	sortedInstances := append([]gpuInstance{}, instances...)
	sort.Slice(sortedInstances, func(i, j int) bool {
		if sortedInstances[i].Start != sortedInstances[j].Start {
			return sortedInstances[i].Start < sortedInstances[j].Start
		}
		if sortedInstances[i].Size != sortedInstances[j].Size {
			return sortedInstances[i].Size < sortedInstances[j].Size
		}
		return sortedInstances[i].InstanceID < sortedInstances[j].InstanceID
	})
	out := []migSlot{}
	usedByProfile := map[string]int{}
	for _, inst := range sortedInstances {
		fullProfile := strings.TrimPrefix(inst.Name, "MIG ")
		shortProfile := canonicalProfile(inst.Name)
		uuids := uuidsByProfile[fullProfile]
		used := usedByProfile[fullProfile]
		uuid := ""
		if used < len(uuids) {
			uuid = uuids[used]
		}
		usedByProfile[fullProfile] = used + 1
		out = append(out, migSlot{
			SlotStart:     inst.Start,
			SlotEnd:       inst.Start + inst.Size,
			Profile:       shortProfile,
			MIGDeviceUUID: uuid,
			GPUInstanceID: inst.InstanceID,
			ProfileID:     inst.ProfileID,
			Source:        "fast-mig-node-agent-provisional",
		})
	}
	return out
}

func atoi(value string) int {
	out := 0
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return out
		}
		out = out*10 + int(ch-'0')
	}
	return out
}

func atoiStrict(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return -1
	}
	out := 0
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return -1
		}
		out = out*10 + int(ch-'0')
	}
	return out
}

func acquireLock(path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

func templateNames() []string {
	names := make([]string, 0, len(templateSlotSpecs))
	for name := range templateSlotSpecs {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return templateRank(names[i]) < templateRank(names[j])
	})
	return names
}

func templateRank(name string) int {
	order := []string{"7", "4+3", "4+2+1", "4+1+1+1", "3+3", "3+2+1", "3+1+1+1", "2+2+3", "3+2+1+1", "3+1+1+1+1", "2+2+2+1", "2+2+1+1+1", "2+1+1+1+1+1", "1+1+1+1+1+1+1"}
	for idx, item := range order {
		if item == name {
			return idx
		}
	}
	return len(order)
}

func printResult(jsonOut bool, res result) {
	if jsonOut {
		encoded, _ := json.Marshal(res)
		fmt.Println(string(encoded))
		return
	}
	if res.Command == "benchmark" {
		fmt.Printf("RESULT|%s|%s|%s|%.3f|%.3f|%t\n", res.Command, res.Template, res.ProfileIDs, res.CreateSeconds, res.DeleteSeconds, res.Success)
		return
	}
	fmt.Printf("RESULT|%s|%s|%s|%s|%.3f|%t\n", res.Command, res.GPUIndex, res.Template, res.ProfileIDs, res.CreateSeconds, res.Success)
	if strings.TrimSpace(res.Message) != "" {
		fmt.Println(strings.TrimSpace(res.Message))
	}
	if strings.TrimSpace(res.NvidiaSMIL) != "" {
		fmt.Println(strings.TrimSpace(res.NvidiaSMIL))
	}
}

func fail(jsonOut bool, res result, code int) {
	printResult(jsonOut, res)
	os.Exit(code)
}

const (
	orSimResourceDomain = "or-sim.io"
	nvidiaMIGCDIPrefix  = "k8s.device-plugin.nvidia.com/gpu="
)

type slotDevice struct {
	ResourceName  string
	SocketName    string
	PhysicalGPUID string
	GPUIndex      string
	SlotStart     int
	SlotEnd       int
	Profile       string
	MIGUUID       string
}

type slotDevicePluginServer struct {
	deviceplugin.UnimplementedDevicePluginServer
	mu     sync.RWMutex
	device slotDevice
}

func runSlotDevicePlugin(nodeName, pluginDir string, scanInterval time.Duration) error {
	manager := map[string]*slotResourceServer{}
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		slots, err := discoverSlotDevices(nodeName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "or-sim slot device discovery failed: %v\n", err)
		} else {
			seen := map[string]slotDevice{}
			for _, slot := range slots {
				seen[slot.ResourceName] = slot
				server, ok := manager[slot.ResourceName]
				if !ok {
					server = newSlotResourceServer(pluginDir, slot)
					if err := server.start(); err != nil {
						return err
					}
					manager[slot.ResourceName] = server
				}
				server.update(slot)
				if err := server.register(); err != nil {
					fmt.Fprintf(os.Stderr, "or-sim slot device plugin register %s failed: %v\n", slot.ResourceName, err)
				}
			}
			for resourceName, server := range manager {
				if _, ok := seen[resourceName]; ok {
					continue
				}
				server.stop()
				delete(manager, resourceName)
			}
		}
		<-ticker.C
	}
}

type slotResourceServer struct {
	pluginDir string
	socket    string
	grpc     *grpc.Server
	service  *slotDevicePluginServer
}

func newSlotResourceServer(pluginDir string, device slotDevice) *slotResourceServer {
	return &slotResourceServer{
		pluginDir: pluginDir,
		socket:    filepath.Join(pluginDir, device.SocketName),
		service:   &slotDevicePluginServer{device: device},
	}
}

func (s *slotResourceServer) start() error {
	if err := os.MkdirAll(s.pluginDir, 0755); err != nil {
		return err
	}
	_ = os.Remove(s.socket)
	listener, err := net.Listen("unix", s.socket)
	if err != nil {
		return err
	}
	s.grpc = grpc.NewServer()
	deviceplugin.RegisterDevicePluginServer(s.grpc, s.service)
	go func() {
		if err := s.grpc.Serve(listener); err != nil {
			fmt.Fprintf(os.Stderr, "or-sim slot device plugin grpc stopped: %v\n", err)
		}
	}()
	if err := waitForSocket(s.socket, 5*time.Second); err != nil {
		return err
	}
	return s.register()
}

func (s *slotResourceServer) register() error {
	return registerSlotResource(s.pluginDir, s.service.current().ResourceName, filepath.Base(s.socket))
}

func (s *slotResourceServer) update(device slotDevice) {
	s.service.set(device)
}

func (s *slotResourceServer) stop() {
	if s.grpc != nil {
		s.grpc.Stop()
	}
	_ = os.Remove(s.socket)
}

func (s *slotDevicePluginServer) set(device slotDevice) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.device = device
}

func (s *slotDevicePluginServer) current() slotDevice {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.device
}

func (s *slotDevicePluginServer) GetDevicePluginOptions(context.Context, *deviceplugin.Empty) (*deviceplugin.DevicePluginOptions, error) {
	return &deviceplugin.DevicePluginOptions{}, nil
}

func (s *slotDevicePluginServer) ListAndWatch(_ *deviceplugin.Empty, stream deviceplugin.DevicePlugin_ListAndWatchServer) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		device := s.current()
		resp := &deviceplugin.ListAndWatchResponse{
			Devices: []*deviceplugin.Device{{
				ID:     device.MIGUUID,
				Health: deviceplugin.Healthy,
			}},
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		<-ticker.C
	}
}

func (s *slotDevicePluginServer) Allocate(_ context.Context, req *deviceplugin.AllocateRequest) (*deviceplugin.AllocateResponse, error) {
	device := s.current()
	responses := make([]*deviceplugin.ContainerAllocateResponse, 0, len(req.ContainerRequests))
	for _, containerReq := range req.ContainerRequests {
		if len(containerReq.DevicesIDs) != 1 || containerReq.DevicesIDs[0] != device.MIGUUID {
			return nil, fmt.Errorf("or-sim slot %s expected allocation for %s, got %v", device.ResourceName, device.MIGUUID, containerReq.DevicesIDs)
		}
		responses = append(responses, &deviceplugin.ContainerAllocateResponse{
			CDIDevices: []*deviceplugin.CDIDevice{{Name: nvidiaMIGCDIPrefix + device.MIGUUID}},
			Envs: map[string]string{
				"OR_SIM_PHYSICAL_GPU_ID": device.PhysicalGPUID,
				"OR_SIM_SLOT":            fmt.Sprintf("%d-%d-%s", device.SlotStart, device.SlotEnd, device.Profile),
				"OR_SIM_MIG_UUID":        device.MIGUUID,
			},
		})
	}
	return &deviceplugin.AllocateResponse{ContainerResponses: responses}, nil
}

func (s *slotDevicePluginServer) PreStartContainer(context.Context, *deviceplugin.PreStartContainerRequest) (*deviceplugin.PreStartContainerResponse, error) {
	return &deviceplugin.PreStartContainerResponse{}, nil
}

func (s *slotDevicePluginServer) GetPreferredAllocation(context.Context, *deviceplugin.PreferredAllocationRequest) (*deviceplugin.PreferredAllocationResponse, error) {
	return &deviceplugin.PreferredAllocationResponse{}, nil
}

func registerSlotResource(pluginDir, resourceName, endpoint string) error {
	conn, err := grpc.Dial(
		"unix://"+filepath.Join(pluginDir, "kubelet.sock"),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := deviceplugin.NewRegistrationClient(conn)
	_, err = client.Register(context.Background(), &deviceplugin.RegisterRequest{
		Version:      deviceplugin.Version,
		Endpoint:     endpoint,
		ResourceName: resourceName,
	})
	return err
}

func discoverSlotDevices(nodeName string) ([]slotDevice, error) {
	smi, err := run("nvidia-smi", "-L")
	if err != nil {
		return nil, err
	}
	gpuUUIDs := parseGPUUUIDs(smi)
	slots := []slotDevice{}
	for gpuIndex := range gpuUUIDs {
		instances, _, err := listGPUInstances(gpuIndex)
		if err != nil {
			return nil, err
		}
		for _, slot := range migSlotsFromObservation(smi, instances, gpuIndex) {
			if slot.MIGDeviceUUID == "" {
				continue
			}
			physicalID := fmt.Sprintf("%s-gpu%s", nodeName, gpuIndex)
			resourceName := slotResourceName(physicalID, slot.SlotStart, slot.SlotEnd, slot.Profile)
			slots = append(slots, slotDevice{
				ResourceName:  resourceName,
				SocketName:    socketNameForResource(resourceName),
				PhysicalGPUID: physicalID,
				GPUIndex:      gpuIndex,
				SlotStart:     slot.SlotStart,
				SlotEnd:       slot.SlotEnd,
				Profile:       slot.Profile,
				MIGUUID:       slot.MIGDeviceUUID,
			})
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].ResourceName < slots[j].ResourceName })
	return slots, nil
}

func parseGPUUUIDs(smi string) map[string]string {
	re := regexp.MustCompile(`^GPU ([0-9]+): .* \(UUID: ([^)]+)\)$`)
	out := map[string]string{}
	for _, line := range strings.Split(smi, "\n") {
		match := re.FindStringSubmatch(strings.TrimSpace(line))
		if len(match) == 3 {
			out[match[1]] = match[2]
		}
	}
	return out
}

func slotResourceName(physicalGPUID string, start, end int, profile string) string {
	return fmt.Sprintf("%s/%s-s%d-%d-%s", orSimResourceDomain, resourceToken(physicalGPUID), start, end, resourceToken(profile))
}

func resourceToken(value string) string {
	value = strings.ToLower(value)
	re := regexp.MustCompile(`[^a-z0-9.-]+`)
	value = re.ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-")
	if len(value) > 63 {
		value = strings.Trim(value[:63], ".-")
	}
	if value == "" {
		return "slot"
	}
	return value
}

func socketNameForResource(resourceName string) string {
	name := strings.ReplaceAll(resourceName, "/", "-")
	name = strings.ReplaceAll(name, ".", "-")
	return resourceToken(name) + ".sock"
}

func waitForSocket(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for socket %s", path)
}
