package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type soakFailureEvidenceRun struct {
	Manifest       *failureManifest
	ManifestPath   string
	ManifestDigest string
	Timeline       *failureTimeline
	ReportDir      string
	StartedAt      time.Time
}

func openSoakFailureEvidence(
	cfg *soakConfig,
	metrics *Metrics,
	startedAt time.Time,
) (*soakFailureEvidenceRun, error) {
	if cfg == nil || cfg.FailureManifestPath == "" {
		return nil, fmt.Errorf("SOAK_FAILURE_MANIFEST_PATH is required for a formal failure-evidence run")
	}
	encoded, err := os.ReadFile(cfg.FailureManifestPath)
	if err != nil {
		return nil, fmt.Errorf("read configured failure manifest: %w", err)
	}
	manifest, err := parseFailureManifest(encoded)
	if err != nil {
		return nil, err
	}
	if manifest.RunID != cfg.RunID {
		return nil, fmt.Errorf(
			"failure manifest run ID %q does not match SOAK_RUN_ID %q",
			manifest.RunID,
			cfg.RunID,
		)
	}
	manifestPath, digest, err := ensureFailureManifest(cfg.LedgerDir, manifest)
	if err != nil {
		return nil, err
	}
	timelinePath := cfg.FailureTimelinePath
	if timelinePath == "" {
		timelinePath = filepath.Join(cfg.LedgerDir, cfg.RunID+".fault-events.jsonl")
	}
	if !failurePathWithin(cfg.LedgerDir, timelinePath) {
		return nil, fmt.Errorf("failure timeline must be stored under SOAK_LEDGER_DIR")
	}
	timeline, err := openFailureTimeline(timelinePath, manifest)
	if err != nil {
		return nil, err
	}
	reportDirectory := cfg.FailureReportDir
	if reportDirectory == "" {
		reportDirectory = cfg.LedgerDir
	}
	if !failurePathWithin(cfg.LedgerDir, reportDirectory) {
		_ = timeline.Close()
		return nil, fmt.Errorf("failure report must be stored under SOAK_LEDGER_DIR")
	}
	if metrics != nil {
		metrics.RunInfo.WithLabelValues(
			manifest.Environment,
			manifest.ScenarioID,
			manifest.TrafficProfile,
		).Set(1)
	}
	return &soakFailureEvidenceRun{
		Manifest: manifest, ManifestPath: manifestPath, ManifestDigest: digest,
		Timeline: timeline, ReportDir: reportDirectory, StartedAt: startedAt.UTC(),
	}, nil
}

func failurePathWithin(root, path string) bool {
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	targetPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(rootPath, targetPath)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (r *soakFailureEvidenceRun) Close() error {
	if r == nil || r.Timeline == nil {
		return nil
	}
	return r.Timeline.Close()
}
