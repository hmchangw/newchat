package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"
)

type failureEventType string

const (
	failureEventBaselineStarted         failureEventType = "baseline_started"
	failureEventFaultStarted            failureEventType = "fault_started"
	failureEventFaultRemoved            failureEventType = "fault_removed"
	failureEventRecoveryObserved        failureEventType = "recovery_observed"
	failureEventSettleStarted           failureEventType = "settle_started"
	failureEventReconciliationStarted   failureEventType = "reconciliation_started"
	failureEventReconciliationCompleted failureEventType = "reconciliation_completed"
	failureEventRunAborted              failureEventType = "run_aborted"
	failureEventNote                    failureEventType = "note"
)

var failureEventRegistry = map[failureEventType]struct{}{
	failureEventBaselineStarted: {}, failureEventFaultStarted: {}, failureEventFaultRemoved: {},
	failureEventRecoveryObserved: {}, failureEventSettleStarted: {}, failureEventReconciliationStarted: {},
	failureEventReconciliationCompleted: {}, failureEventRunAborted: {}, failureEventNote: {},
}

var failureTargetKindRegistry = map[string]struct{}{
	"nats_server": {}, "nats_cluster": {}, "jetstream_stream": {},
	"jetstream_consumer": {}, "network_link": {}, "loadgen_observer": {},
}

var failureFaultKindRegistry = map[string]struct{}{
	"process_stop": {}, "process_kill": {}, "network_partition": {},
	"network_latency": {}, "leader_stepdown": {}, "stream_unavailable": {},
	"consumer_pause": {}, "observer_disconnect": {},
}

type failureFaultEvent struct {
	SchemaVersion int               `json:"schemaVersion"`
	RunID         string            `json:"runId"`
	Sequence      uint64            `json:"sequence"`
	Type          failureEventType  `json:"type"`
	ScenarioID    string            `json:"scenarioId"`
	TargetKind    string            `json:"targetKind,omitempty"`
	Target        string            `json:"target,omitempty"`
	FaultKind     string            `json:"faultKind,omitempty"`
	At            time.Time         `json:"at"`
	Actor         string            `json:"actor"`
	Details       map[string]string `json:"details,omitempty"`
}

func validateFailureTimeline(manifest *failureManifest, input []failureFaultEvent) ([]failureFaultEvent, error) {
	if err := validateFailureManifest(manifest); err != nil {
		return nil, err
	}
	events := append([]failureFaultEvent(nil), input...)
	slices.SortFunc(events, func(a, b failureFaultEvent) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	active := make(map[string]struct{})
	var previous uint64
	for index := range events {
		event := &events[index]
		if event.SchemaVersion != 1 || event.RunID != manifest.RunID || event.ScenarioID != manifest.ScenarioID || event.Sequence == 0 || event.At.IsZero() || event.At.Location() != time.UTC || event.Actor == "" {
			return nil, fmt.Errorf("invalid failure event at index %d", index)
		}
		if _, ok := failureEventRegistry[event.Type]; !ok {
			return nil, fmt.Errorf("unsupported failure event type %q", event.Type)
		}
		if event.Sequence <= previous {
			return nil, fmt.Errorf("failure event sequence %d is not strictly increasing", event.Sequence)
		}
		previous = event.Sequence
		switch event.Type {
		case failureEventFaultStarted, failureEventFaultRemoved:
			if event.TargetKind == "" || event.Target == "" || event.FaultKind == "" {
				return nil, fmt.Errorf("failure event sequence %d requires target and fault kind", event.Sequence)
			}
			if _, known := failureTargetKindRegistry[event.TargetKind]; !known {
				return nil, fmt.Errorf("unsupported failure target kind %q", event.TargetKind)
			}
			if _, known := failureFaultKindRegistry[event.FaultKind]; !known {
				return nil, fmt.Errorf("unsupported failure fault kind %q", event.FaultKind)
			}
			key := event.TargetKind + "\x00" + event.Target + "\x00" + event.FaultKind
			if event.Type == failureEventFaultStarted {
				if len(active) > 0 && !manifest.FaultPolicy.AllowCombined {
					return nil, fmt.Errorf("overlapping faults require combined-failure policy")
				}
				if _, exists := active[key]; exists {
					return nil, fmt.Errorf("fault %q is already active", event.Target)
				}
				active[key] = struct{}{}
			} else {
				if _, exists := active[key]; !exists {
					return nil, fmt.Errorf("fault removal has no matching active fault")
				}
				delete(active, key)
			}
		default:
			// The remaining registered events do not change active fault state.
		}
	}
	return events, nil
}

type failureTimeline struct {
	mu       sync.Mutex
	manifest *failureManifest
	path     string
	file     *os.File
	events   []failureFaultEvent
}

func openFailureTimeline(path string, manifest *failureManifest) (*failureTimeline, error) {
	if err := validateFailureManifest(manifest); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open failure timeline: %w", err)
	}
	timeline := &failureTimeline{manifest: manifest, path: path, file: file}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event failureFaultEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("decode failure timeline: %w", err)
		}
		timeline.events = append(timeline.events, event)
	}
	if err := scanner.Err(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("read failure timeline: %w", err)
	}
	if _, err := validateFailureTimeline(manifest, timeline.events); err != nil {
		_ = file.Close()
		return nil, err
	}
	return timeline, nil
}

func (t *failureTimeline) Append(event *failureFaultEvent) error {
	if t == nil || event == nil {
		return fmt.Errorf("failure timeline event is required")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	candidate := append(append([]failureFaultEvent(nil), t.events...), *event)
	ordered, err := validateFailureTimeline(t.manifest, candidate)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode failure timeline event: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := t.file.Write(encoded); err != nil {
		return fmt.Errorf("append failure timeline event: %w", err)
	}
	if err := t.file.Sync(); err != nil {
		return fmt.Errorf("sync failure timeline event: %w", err)
	}
	t.events = ordered
	return nil
}

func (t *failureTimeline) Events() []failureFaultEvent {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]failureFaultEvent(nil), t.events...)
}

func (t *failureTimeline) Refresh() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return fmt.Errorf("failure timeline is closed")
	}
	if err := t.file.Sync(); err != nil {
		return fmt.Errorf("sync failure timeline before refresh: %w", err)
	}
	if _, err := t.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek failure timeline: %w", err)
	}
	events := make([]failureFaultEvent, 0, len(t.events))
	scanner := bufio.NewScanner(t.file)
	for scanner.Scan() {
		var event failureFaultEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode refreshed failure timeline: %w", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read refreshed failure timeline: %w", err)
	}
	ordered, err := validateFailureTimeline(t.manifest, events)
	if err != nil {
		return err
	}
	t.events = ordered
	return nil
}

func (t *failureTimeline) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return nil
	}
	if err := t.file.Close(); err != nil {
		return fmt.Errorf("close failure timeline: %w", err)
	}
	t.file = nil
	return nil
}
