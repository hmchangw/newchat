package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

const failureManifestSchemaVersion = 1

var failureRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type failureTopology struct {
	NATSServers                int    `json:"natsServers"`
	StreamReplicas             int    `json:"streamReplicas"`
	MongoDBMembers             int    `json:"mongodbMembers,omitempty"`
	CassandraReplicationFactor int    `json:"cassandraReplicationFactor,omitempty"`
	CassandraConsistency       string `json:"cassandraConsistency,omitempty"`
}

type failureFaultPolicy struct {
	AllowCombined          bool `json:"allowCombined,omitempty"`
	AllowDeliveryDuplicate bool `json:"allowDeliveryDuplicate,omitempty"`
}

type failureManifest struct {
	SchemaVersion           int                `json:"schemaVersion"`
	RunID                   string             `json:"runId"`
	ScenarioID              string             `json:"scenarioId"`
	GitSHA                  string             `json:"gitSha"`
	Images                  map[string]string  `json:"images,omitempty"`
	Environment             string             `json:"environment"`
	Sites                   []string           `json:"sites,omitempty"`
	Seed                    int64              `json:"seed"`
	TrafficProfile          string             `json:"trafficProfile"`
	TrafficParameters       map[string]float64 `json:"trafficParameters,omitempty"`
	Topology                failureTopology    `json:"topology"`
	RequiredObservers       []failureObserver  `json:"requiredObservers"`
	RequiredMetricContracts []string           `json:"requiredMetricContracts"`
	FaultPolicy             failureFaultPolicy `json:"faultPolicy,omitempty"`
	CreatedAt               time.Time          `json:"createdAt"`
}

var failureMetricContractRegistry = map[string]struct{}{
	"nats-core-v1": {}, "loadgen-health-v1": {},
}

var failureScenarioRegistry = map[string]struct{}{
	"F01": {}, "F02": {}, "F03": {}, "F04": {}, "F07a": {}, "F07b": {},
}

var failureEnvironmentRegistry = map[string]struct{}{
	"local": {}, "development": {}, "staging": {}, "production": {},
}

var failureTrafficProfileRegistry = map[string]struct{}{
	"nats-core-message-v1": {},
}

func parseFailureManifest(data []byte) (*failureManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest failureManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode failure manifest: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode failure manifest: %w", err)
	}
	if err := validateFailureManifest(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("unexpected trailing JSON value")
}

func validateFailureManifest(manifest *failureManifest) error {
	if manifest == nil {
		return fmt.Errorf("failure manifest is required")
	}
	if manifest.SchemaVersion != failureManifestSchemaVersion {
		return fmt.Errorf("unsupported failure manifest schema version %d", manifest.SchemaVersion)
	}
	if !failureRunIDPattern.MatchString(manifest.RunID) || manifest.RunID == "." || manifest.RunID == ".." {
		return fmt.Errorf("failure manifest run ID is not a safe file component")
	}
	if _, ok := failureScenarioRegistry[manifest.ScenarioID]; !ok {
		return fmt.Errorf("unsupported failure scenario %q", manifest.ScenarioID)
	}
	if strings.TrimSpace(manifest.GitSHA) == "" || strings.TrimSpace(manifest.Environment) == "" || strings.TrimSpace(manifest.TrafficProfile) == "" {
		return fmt.Errorf("failure manifest requires git SHA, environment, and traffic profile")
	}
	if _, ok := failureEnvironmentRegistry[manifest.Environment]; !ok {
		return fmt.Errorf("unsupported failure environment %q", manifest.Environment)
	}
	if _, ok := failureTrafficProfileRegistry[manifest.TrafficProfile]; !ok {
		return fmt.Errorf("unsupported failure traffic profile %q", manifest.TrafficProfile)
	}
	if manifest.CreatedAt.IsZero() || manifest.CreatedAt.Location() != time.UTC {
		return fmt.Errorf("failure manifest createdAt must be UTC RFC3339")
	}
	if len(manifest.RequiredObservers) == 0 || len(manifest.RequiredMetricContracts) == 0 {
		return fmt.Errorf("failure manifest requires observers and metric contracts")
	}
	if err := validateRegisteredObservers(manifest.RequiredObservers); err != nil {
		return err
	}
	seenContracts := make(map[string]struct{}, len(manifest.RequiredMetricContracts))
	for _, contract := range manifest.RequiredMetricContracts {
		if _, ok := failureMetricContractRegistry[contract]; !ok {
			return fmt.Errorf("unsupported metric contract %q", contract)
		}
		if _, duplicate := seenContracts[contract]; duplicate {
			return fmt.Errorf("duplicate metric contract %q", contract)
		}
		seenContracts[contract] = struct{}{}
	}
	if manifest.TrafficProfile == "nats-core-message-v1" {
		if manifest.Topology.NATSServers < 3 || manifest.Topology.StreamReplicas < 3 {
			return fmt.Errorf("nats core campaign requires at least three NATS servers and stream replicas")
		}
		for _, required := range []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRecipient} {
			if !slices.Contains(manifest.RequiredObservers, required) {
				return fmt.Errorf("nats core campaign requires observer %q", required)
			}
		}
	}
	return nil
}

func createFailureManifest(directory string, manifest *failureManifest) (string, string, error) {
	if err := validateFailureManifest(manifest); err != nil {
		return "", "", err
	}
	if directory == "" {
		return "", "", fmt.Errorf("failure evidence directory is required")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return "", "", fmt.Errorf("create failure evidence directory: %w", err)
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("encode failure manifest: %w", err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(directory, manifest.RunID+".manifest.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", "", fmt.Errorf("create immutable failure manifest: %w", err)
	}
	closeFile := true
	removeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
		if removeFile {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(encoded); err != nil {
		return "", "", fmt.Errorf("write failure manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		return "", "", fmt.Errorf("sync failure manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", "", fmt.Errorf("close failure manifest: %w", err)
	}
	closeFile = false
	if err := syncFailureDirectory(directory); err != nil {
		return "", "", fmt.Errorf("sync failure manifest directory: %w", err)
	}
	removeFile = false
	digest := sha256.Sum256(encoded)
	return path, fmt.Sprintf("%x", digest[:]), nil
}

func ensureFailureManifest(directory string, manifest *failureManifest) (string, string, error) {
	if err := validateFailureManifest(manifest); err != nil {
		return "", "", err
	}
	path := filepath.Join(directory, manifest.RunID+".manifest.json")
	retained, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return createFailureManifest(directory, manifest)
	}
	if err != nil {
		return "", "", fmt.Errorf("read retained failure manifest: %w", err)
	}
	parsed, err := parseFailureManifest(retained)
	if err != nil {
		return "", "", fmt.Errorf("validate retained failure manifest: %w", err)
	}
	expectedCanonical, err := json.Marshal(manifest)
	if err != nil {
		return "", "", fmt.Errorf("encode expected failure manifest: %w", err)
	}
	retainedCanonical, err := json.Marshal(parsed)
	if err != nil {
		return "", "", fmt.Errorf("encode retained failure manifest: %w", err)
	}
	if !bytes.Equal(expectedCanonical, retainedCanonical) {
		return "", "", fmt.Errorf("configured manifest does not match retained manifest %q", path)
	}
	digest := sha256.Sum256(retained)
	return path, fmt.Sprintf("%x", digest[:]), nil
}
