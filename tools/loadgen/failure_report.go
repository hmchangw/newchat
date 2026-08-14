package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

type failureReportWindow struct {
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
}

type failureTerminalCounts struct {
	Good                 uint64 `json:"good"`
	Bad                  uint64 `json:"bad"`
	Unverified           uint64 `json:"unverified"`
	NotSent              uint64 `json:"notSent"`
	MissingAfterDeadline uint64 `json:"missingAfterDeadline"`
}

func (c failureTerminalCounts) Total() uint64 {
	return c.Good + c.Bad + c.Unverified + c.NotSent + c.MissingAfterDeadline
}

type failureLatencyReport struct {
	P50 string `json:"p50"`
	P95 string `json:"p95"`
	P99 string `json:"p99"`
	Max string `json:"max"`
}

type failureLaneReport struct {
	Scenario  string                `json:"scenario"`
	Lane      string                `json:"lane"`
	Offered   uint64                `json:"offered"`
	Eligible  uint64                `json:"eligible"`
	Terminals failureTerminalCounts `json:"terminals"`
	Latency   failureLatencyReport  `json:"latency"`
}

type failureObservationCounts struct {
	Good                 uint64 `json:"good"`
	Bad                  uint64 `json:"bad"`
	Unverified           uint64 `json:"unverified"`
	MissingAfterDeadline uint64 `json:"missingAfterDeadline"`
}

type failureObserverReport struct {
	Observer  failureObserver          `json:"observer"`
	Outcomes  failureObservationCounts `json:"outcomes"`
	Health    []failureHealthInterval  `json:"health,omitempty"`
	LastValid *time.Time               `json:"lastValid,omitempty"`
}

type failureConsumerReport struct {
	Stream          string  `json:"stream"`
	Durable         string  `json:"durable"`
	Up              bool    `json:"up"`
	SampleErrors    uint64  `json:"sampleErrors"`
	BaselinePending uint64  `json:"baselinePending"`
	PeakPending     uint64  `json:"peakPending"`
	FinalPending    uint64  `json:"finalPending"`
	RecoveryPending *uint64 `json:"recoveryPending,omitempty"`
	OldestAge       string  `json:"oldestAge,omitempty"`
}

type failureSidecarReference struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Count  int    `json:"count"`
}

type failureEvidenceDigest struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type failureEvidenceReport struct {
	SchemaVersion    int                             `json:"schemaVersion"`
	ManifestDigest   string                          `json:"manifestDigest"`
	Window           failureReportWindow             `json:"window"`
	Timeline         []failureFaultEvent             `json:"timeline,omitempty"`
	Lanes            []failureLaneReport             `json:"lanes,omitempty"`
	Observers        []failureObserverReport         `json:"observers,omitempty"`
	ObserverHealth   []failureObserverHealthSnapshot `json:"observerHealth,omitempty"`
	Consumers        []failureConsumerReport         `json:"consumers,omitempty"`
	TopologyChanges  []string                        `json:"topologyChanges,omitempty"`
	InvalidIntervals []failureHealthInterval         `json:"invalidIntervals,omitempty"`
	Gates            []failureGate                   `json:"gates"`
	Sidecars         []failureSidecarReference       `json:"sidecars,omitempty"`
	EvidenceDigests  []failureEvidenceDigest         `json:"evidenceDigests,omitempty"`
	Verdict          failureVerdict                  `json:"verdict"`
	Reasons          []string                        `json:"reasons"`
}

//nolint:gocritic // The value copy lets canonical sorting avoid mutating the caller's report.
func marshalFailureEvidenceReport(report failureEvidenceReport) ([]byte, error) {
	report.Reasons = append([]string(nil), report.Reasons...)
	report.TopologyChanges = append([]string(nil), report.TopologyChanges...)
	report.Gates = append([]failureGate(nil), report.Gates...)
	report.Timeline = append([]failureFaultEvent(nil), report.Timeline...)
	report.Lanes = append([]failureLaneReport(nil), report.Lanes...)
	report.Observers = append([]failureObserverReport(nil), report.Observers...)
	for index := range report.Observers {
		report.Observers[index].Health = append([]failureHealthInterval(nil), report.Observers[index].Health...)
	}
	report.ObserverHealth = append([]failureObserverHealthSnapshot(nil), report.ObserverHealth...)
	for index := range report.ObserverHealth {
		report.ObserverHealth[index].Intervals = append(
			[]failureHealthInterval(nil), report.ObserverHealth[index].Intervals...,
		)
	}
	report.Consumers = append([]failureConsumerReport(nil), report.Consumers...)
	report.Sidecars = append([]failureSidecarReference(nil), report.Sidecars...)
	report.EvidenceDigests = append([]failureEvidenceDigest(nil), report.EvidenceDigests...)
	report.InvalidIntervals = append([]failureHealthInterval(nil), report.InvalidIntervals...)
	for index := range report.Lanes {
		lane := &report.Lanes[index]
		if lane.Eligible != lane.Terminals.Total() {
			return nil, fmt.Errorf(
				"eligible operation accounting for %s/%s: eligible=%d terminals=%d",
				lane.Scenario,
				lane.Lane,
				lane.Eligible,
				lane.Terminals.Total(),
			)
		}
	}
	slices.Sort(report.Reasons)
	slices.Sort(report.TopologyChanges)
	slices.SortFunc(report.Gates, func(a, b failureGate) int { return strings.Compare(a.ID, b.ID) })
	slices.SortFunc(report.Timeline, func(a, b failureFaultEvent) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	slices.SortFunc(report.Lanes, func(a, b failureLaneReport) int {
		if a.Scenario != b.Scenario {
			return strings.Compare(a.Scenario, b.Scenario)
		}
		return strings.Compare(a.Lane, b.Lane)
	})
	slices.SortFunc(report.Observers, func(a, b failureObserverReport) int {
		return strings.Compare(string(a.Observer), string(b.Observer))
	})
	for i := range report.Observers {
		slices.SortFunc(report.Observers[i].Health, func(a, b failureHealthInterval) int {
			return a.Start.Compare(b.Start)
		})
	}
	slices.SortFunc(report.ObserverHealth, func(a, b failureObserverHealthSnapshot) int {
		return strings.Compare(string(a.Observer), string(b.Observer))
	})
	for index := range report.ObserverHealth {
		slices.SortFunc(report.ObserverHealth[index].Intervals, func(a, b failureHealthInterval) int {
			return a.Start.Compare(b.Start)
		})
	}
	slices.SortFunc(report.Consumers, func(a, b failureConsumerReport) int {
		if a.Stream != b.Stream {
			return strings.Compare(a.Stream, b.Stream)
		}
		return strings.Compare(a.Durable, b.Durable)
	})
	slices.SortFunc(report.Sidecars, func(a, b failureSidecarReference) int {
		return strings.Compare(a.Path, b.Path)
	})
	slices.SortFunc(report.EvidenceDigests, func(a, b failureEvidenceDigest) int {
		return strings.Compare(a.Path, b.Path)
	})
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode failure evidence report: %w", err)
	}
	return append(encoded, '\n'), nil
}

func writeFailureSidecar(directory, name string, identifiers []string) (failureSidecarReference, error) {
	if filepath.Base(name) != name || name == "." || name == "" {
		return failureSidecarReference{}, fmt.Errorf("failure sidecar name must be a file component")
	}
	values := append([]string(nil), identifiers...)
	slices.Sort(values)
	values = slices.Compact(values)
	encoded, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return failureSidecarReference{}, fmt.Errorf("encode failure sidecar: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := writeFailureArtifactAtomic(directory, name, encoded); err != nil {
		return failureSidecarReference{}, err
	}
	digest := sha256.Sum256(encoded)
	return failureSidecarReference{Path: name, SHA256: fmt.Sprintf("%x", digest[:]), Count: len(values)}, nil
}

func writeFailureEvidenceArtifacts(directory string, report *failureEvidenceReport) error {
	if report == nil {
		return fmt.Errorf("failure evidence report is required")
	}
	encoded, err := marshalFailureEvidenceReport(*report)
	if err != nil {
		return err
	}
	if err := writeFailureArtifactAtomic(directory, "report.json", encoded); err != nil {
		return fmt.Errorf("write failure report: %w", err)
	}
	summary := new(strings.Builder)
	fmt.Fprintf(summary, "# Failure evidence report: %s\n\n", report.Verdict)
	fmt.Fprintf(summary, "Manifest digest: `%s`\n\n", report.ManifestDigest)
	if len(report.Reasons) > 0 {
		summary.WriteString("## Reasons\n\n")
		reasons := append([]string(nil), report.Reasons...)
		slices.Sort(reasons)
		for _, reason := range reasons {
			fmt.Fprintf(summary, "- %s\n", reason)
		}
	}
	if err := writeFailureArtifactAtomic(directory, "summary.md", []byte(summary.String())); err != nil {
		return fmt.Errorf("write failure summary: %w", err)
	}
	return nil
}

func writeFailureArtifactAtomic(directory, name string, data []byte) error {
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create failure artifact directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+name+".tmp-")
	if err != nil {
		return fmt.Errorf("create temporary failure artifact: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect temporary failure artifact: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary failure artifact: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary failure artifact: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary failure artifact: %w", err)
	}
	targetPath := filepath.Join(directory, name)
	backupPath := targetPath + ".bak"
	hadTarget := false
	if _, err := os.Stat(targetPath); err == nil {
		hadTarget = true
		_ = os.Remove(backupPath)
		if err := os.Rename(targetPath, backupPath); err != nil {
			return fmt.Errorf("backup prior failure artifact: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat prior failure artifact: %w", err)
	}
	if err := os.Rename(temporaryPath, targetPath); err != nil {
		if hadTarget {
			_ = os.Rename(backupPath, targetPath)
		}
		return fmt.Errorf("install failure artifact: %w", err)
	}
	removeTemporary = false
	if err := syncFailureDirectory(directory); err != nil {
		if hadTarget {
			_ = os.Remove(targetPath)
			_ = os.Rename(backupPath, targetPath)
		}
		return err
	}
	if hadTarget {
		if err := os.Remove(backupPath); err != nil {
			return fmt.Errorf("remove prior failure artifact backup: %w", err)
		}
	}
	return nil
}

func syncFailureDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		// Windows does not expose directory fsync through os.File.Sync. The
		// file itself is flushed above; Linux production volumes also flush the
		// containing directory to make the rename durable.
		return nil
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open failure artifact directory for sync: %w", err)
	}
	defer directoryFile.Close()
	if err := directoryFile.Sync(); err != nil {
		return fmt.Errorf("sync failure artifact directory: %w", err)
	}
	return nil
}

func buildSoakFailureEvidenceReport(
	run *soakFailureEvidenceRun,
	ledger failureLedgerSnapshot,
	soak *soakCollectorSnapshot,
	recipientHealth *failureObserverHealthSnapshot,
	consumers []ConsumerStat,
	sidecars []failureSidecarReference,
	endedAt time.Time,
) failureEvidenceReport {
	report := failureEvidenceReport{SchemaVersion: 1, Verdict: failureVerdictInconclusive}
	if run == nil || run.Manifest == nil {
		report.Gates = []failureGate{{
			ID: "01_manifest", Verdict: failureVerdictInconclusive,
			Reason: "failure manifest is unavailable",
		}}
		result := evaluateFailureVerdict(report.Gates)
		report.Verdict, report.Reasons = result.Verdict, result.Reasons
		return report
	}
	health := failureObserverHealthSnapshot{}
	if recipientHealth != nil {
		health = *recipientHealth
	}
	report.ManifestDigest = run.ManifestDigest
	report.Sidecars = append([]failureSidecarReference(nil), sidecars...)
	report.Window = failureReportWindow{StartedAt: run.StartedAt.UTC(), EndedAt: endedAt.UTC()}
	report.Timeline = run.Timeline.Events()
	report.EvidenceDigests = append(report.EvidenceDigests, failureEvidenceDigest{
		Path: filepath.Base(run.ManifestPath), SHA256: run.ManifestDigest,
	})

	results := ledger.Results
	terminals := failureTerminalCounts{
		Good: results[failureResultGood], Bad: results[failureResultBad],
		Unverified: results[failureResultUnverified], NotSent: results[failureResultNotSent],
		MissingAfterDeadline: results[failureResultMissingAfterDeadline],
	}
	lane := failureLaneReport{
		Scenario: run.Manifest.ScenarioID, Lane: soakFailureLaneMessageSend,
		Eligible: terminals.Total(), Terminals: terminals,
	}
	if soak != nil {
		stats := soak.Actions[soakRPCSend]
		lane.Offered = stats.Attempted + soak.Actions[soakRPCThreadReply].Attempted +
			soak.WarmupActions[soakRPCSend] + soak.WarmupActions[soakRPCThreadReply]
		lane.Latency = failureLatencyReport{
			P50: stats.Latency.P50.String(), P95: stats.Latency.P95.String(),
			P99: stats.Latency.P99.String(), Max: stats.Latency.Max.String(),
		}
	}
	report.Lanes = []failureLaneReport{lane}
	for _, observer := range []failureObserver{
		failureObserverAdmission,
		failureObserverHistory,
		failureObserverRecipient,
	} {
		counts := ledger.Observations[observer]
		observerReport := failureObserverReport{
			Observer: observer,
			Outcomes: failureObservationCounts{
				Good: counts[failureObservationGood], Bad: counts[failureObservationBad],
				Unverified:           counts[failureObservationUnverified],
				MissingAfterDeadline: counts[failureObservationMissingAfterDeadline],
			},
		}
		if observer == failureObserverRecipient {
			observerReport.Health = append([]failureHealthInterval(nil), health.Intervals...)
			if !health.LastSuccess.IsZero() {
				lastSuccess := health.LastSuccess.UTC()
				observerReport.LastValid = &lastSuccess
			}
		}
		report.Observers = append(report.Observers, observerReport)
	}
	report.ObserverHealth = []failureObserverHealthSnapshot{health}
	for _, consumer := range consumers {
		report.Consumers = append(report.Consumers, failureConsumerReport{
			Stream: consumer.Stream, Durable: consumer.Durable,
			Up:              consumer.HasSample && consumer.SampleErrors == 0,
			SampleErrors:    consumer.SampleErrors,
			BaselinePending: consumer.BaselinePending,
			PeakPending:     consumer.PeakPending,
			FinalPending:    consumer.FinalPending,
		})
	}

	gates := []failureGate{
		{ID: "01_manifest", Verdict: failureVerdictPass},
		{
			ID: "02_topology", Verdict: failureVerdictInconclusive,
			Reason: "runtime NATS topology proof is unavailable",
		},
	}
	hasBaseline := false
	for index := range report.Timeline {
		event := &report.Timeline[index]
		if event.Type == failureEventBaselineStarted {
			hasBaseline = true
			break
		}
	}
	if hasBaseline {
		gates = append(gates, failureGate{ID: "03_baseline", Verdict: failureVerdictPass})
	} else {
		gates = append(gates, failureGate{
			ID: "03_baseline", Verdict: failureVerdictInconclusive,
			Reason: "baseline_started is absent from the authoritative fault timeline",
		})
	}
	switch {
	case ledger.InvalidReason != "":
		gates = append(gates, failureGate{
			ID: "04_ledger", Verdict: failureVerdictInconclusive,
			Reason: "failure ledger was invalidated: " + ledger.InvalidReason,
		})
		report.InvalidIntervals = append(report.InvalidIntervals, failureHealthInterval{
			Start: run.StartedAt.UTC(), End: endedAt.UTC(), Up: false,
			Reason: ledger.InvalidReason,
		})
	case ledger.Recovered > 0:
		gates = append(gates, failureGate{
			ID: "04_ledger", Verdict: failureVerdictInconclusive,
			Reason: fmt.Sprintf("%d operations were recovered after a process interruption", ledger.Recovered),
		})
	case ledger.Active > 0:
		gates = append(gates, failureGate{
			ID: "04_ledger", Verdict: failureVerdictInconclusive,
			Reason: fmt.Sprintf("%d failure operations remain unreconciled", ledger.Active),
		})
	default:
		gates = append(gates, failureGate{ID: "04_ledger", Verdict: failureVerdictPass})
	}
	if lane.Offered == lane.Eligible {
		gates = append(gates, failureGate{ID: "04_accounting", Verdict: failureVerdictPass})
	} else {
		gates = append(gates, failureGate{
			ID: "04_accounting", Verdict: failureVerdictInconclusive,
			Reason: fmt.Sprintf("offered operations do not match terminal accounting: offered=%d eligible=%d", lane.Offered, lane.Eligible),
		})
	}
	requiredConsumers := map[string]bool{"message-worker": false, "broadcast-worker": false}
	consumerHealthy := true
	for _, consumer := range consumers {
		if _, required := requiredConsumers[consumer.Durable]; required {
			requiredConsumers[consumer.Durable] = true
			if !consumer.HasSample || consumer.SampleErrors > 0 {
				consumerHealthy = false
			}
		}
	}
	for _, present := range requiredConsumers {
		consumerHealthy = consumerHealthy && present
	}
	if consumerHealthy {
		gates = append(gates, failureGate{ID: "05_consumer_health", Verdict: failureVerdictPass})
	} else {
		gates = append(gates, failureGate{
			ID: "05_consumer_health", Verdict: failureVerdictInconclusive,
			Reason: "required consumer samples are missing or failed",
		})
	}
	if failureHealthSnapshotCovers(&health, run.StartedAt, endedAt) {
		gates = append(gates, failureGate{ID: "05_observer_health", Verdict: failureVerdictPass})
	} else {
		gates = append(gates, failureGate{
			ID: "05_observer_health", Verdict: failureVerdictInconclusive,
			Reason: "recipient observer did not cover the full evaluation window",
		})
	}
	switch {
	case terminals.MissingAfterDeadline > 0:
		gates = append(gates, failureGate{
			ID: "06_correctness", Verdict: failureVerdictFail,
			Reason: fmt.Sprintf("%d accepted effects were authoritatively missing after deadline", terminals.MissingAfterDeadline),
		})
	case terminals.Bad > 0:
		gates = append(gates, failureGate{
			ID: "06_correctness", Verdict: failureVerdictFail,
			Reason: fmt.Sprintf("%d operations had mismatched or unexpected effects", terminals.Bad),
		})
	case terminals.Unverified > 0:
		gates = append(gates, failureGate{
			ID: "06_correctness", Verdict: failureVerdictInconclusive,
			Reason: fmt.Sprintf("%d operations could not be verified", terminals.Unverified),
		})
	default:
		gates = append(gates, failureGate{ID: "06_correctness", Verdict: failureVerdictPass})
	}
	// Application-service advisory metrics and canonical recording-rule inputs
	// are owned by the later workstream. Their absence must remain explicit so
	// this loadgen-only implementation cannot claim campaign readiness.
	gates = append(gates, failureGate{
		ID: "07_external_contracts", Verdict: failureVerdictInconclusive,
		Reason: "required application metrics and advisory evidence are external to Workstream A",
	})
	for _, sidecar := range sidecars {
		switch sidecar.Path {
		case "recipient-duplicate.json", "recipient-unexpected.json", "recipient-mismatch.json":
			if sidecar.Count > 0 {
				gates = append(gates, failureGate{
					ID:      "06_recipient_anomaly_" + sidecar.Path,
					Verdict: failureVerdictFail,
					Reason:  fmt.Sprintf("%d recipient anomalies retained in %s", sidecar.Count, sidecar.Path),
				})
			}
		case "recipient-untracked.json", "recipient-unverified.json":
			if sidecar.Count > 0 {
				gates = append(gates, failureGate{
					ID:      "05_recipient_evidence_" + sidecar.Path,
					Verdict: failureVerdictInconclusive,
					Reason:  fmt.Sprintf("%d recipient evidence gaps retained in %s", sidecar.Count, sidecar.Path),
				})
			}
		}
	}
	verdict := evaluateFailureVerdict(gates)
	report.Gates, report.Verdict, report.Reasons = verdict.Gates, verdict.Verdict, verdict.Reasons
	return report
}

func finalizeFailureRecipientSidecars(directory string) ([]failureSidecarReference, error) {
	kinds := []string{"missing", "unexpected", "duplicate", "mismatch", "unverified", "untracked"}
	references := make([]failureSidecarReference, 0, len(kinds))
	for _, kind := range kinds {
		rawPath := filepath.Join(directory, ".recipient-"+kind+".raw.jsonl")
		file, err := os.Open(rawPath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("open recipient evidence journal: %w", err)
		}
		identifiers := make([]string, 0)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record failureRecipientRawEvidence
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("decode recipient evidence journal: %w", err)
			}
			identifier := record.OperationID
			if record.Recipient != "" {
				identifier += "\t" + record.Recipient
			}
			identifiers = append(identifiers, identifier)
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read recipient evidence journal: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close recipient evidence journal: %w", err)
		}
		reference, err := writeFailureSidecar(directory, "recipient-"+kind+".json", identifiers)
		if err != nil {
			return nil, err
		}
		references = append(references, reference)
	}
	return references, nil
}

func failureHealthSnapshotCovers(snapshot *failureObserverHealthSnapshot, start, end time.Time) bool {
	if snapshot == nil {
		return false
	}
	if snapshot.HistoryTruncated && start.Before(snapshot.HistoryAvailableFrom) {
		return false
	}
	if len(snapshot.Intervals) == 0 {
		return false
	}
	coveredUntil := start
	for _, interval := range snapshot.Intervals {
		if !interval.End.After(start) || !interval.Start.Before(end) {
			continue
		}
		if !interval.Up || interval.Start.After(coveredUntil) {
			return false
		}
		if interval.End.After(coveredUntil) {
			coveredUntil = interval.End
		}
	}
	return !coveredUntil.Before(end)
}
