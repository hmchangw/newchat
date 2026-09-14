package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
)

// Root aliases keep failure-aware adapters and cross-package tests compiling.
// Production seed, workload, and teardown entry points compose package run
// directly; the remaining aliases leave with the failure runtime.
type soakManifest = run.Manifest

const (
	soakManifestSeeding   = run.StateSeeding
	soakManifestSeeded    = run.StateSeeded
	soakManifestRunning   = run.StateRunning
	soakManifestStopped   = run.StateStopped
	soakManifestCompleted = run.StateCompleted
	soakManifestCleaned   = run.StateCleaned

	soakManifestCollection  = run.ManifestCollection
	soakOwnershipCollection = run.OwnershipCollection
	soakOwnershipChunkSize  = run.OwnershipChunkSize
)

func digestSoakConfig(cfg *soakConfig) string {
	data, err := json.Marshal(cfg)
	if err != nil {
		panic("marshal soak config: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var soakCreatedRoomPrefix = run.CreatedRoomPrefix
