package main

import (
	"math/rand"
	"time"

	soakuserread "github.com/hmchangw/chat/tools/loadgen/internal/soak/userread"
)

// These aliases keep the runtime call sites stable while userread is
// extracted. The adapter translates the lane-owned sample into the shared
// metrics sample used by the remaining main-package read lanes.
type soakUserReadConfig = soakuserread.Config
type soakUserReader = soakuserread.Reader

type soakUserReadRecorderAdapter struct {
	recorder soakReadSampleRecorder
}

func (a soakUserReadRecorderAdapter) Record(sample *soakuserread.Sample) {
	if a.recorder == nil {
		return
	}
	a.recorder.Record(&soakReadSample{
		Action: sample.Action, Latency: sample.Latency,
		Messages: sample.Messages, RowsCounted: sample.RowsCounted,
		ReplyBytes: sample.ReplyBytes, ErrorClass: sample.ErrorClass,
		ErrorReason: sample.ErrorReason, Retries: sample.Retries,
		Skipped: sample.Skipped,
	})
}

func newSoakUserReader(
	cfg soakUserReadConfig,
	topology *soakTopology,
	rpcClient *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*soakUserReader, error) {
	return soakuserread.New(
		cfg, topology, rpcClient,
		soakUserReadRecorderAdapter{recorder: recorder},
		rng, now,
	)
}
