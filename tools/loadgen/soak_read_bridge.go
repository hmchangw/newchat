package main

import (
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"

	soakread "github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
)

type soakReadConfig = soakread.Config
type soakReadSample = soakread.Sample
type soakReadSampleRecorder = soakread.SampleRecorder
type soakReadOutcome = soakread.Outcome
type soakReader = soakread.Reader

func newSoakReader(
	cfg soakReadConfig,
	topology *soakTopology,
	catalog *soakCatalog,
	rpc *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) *soakReader {
	return soakread.NewReader(
		cfg,
		topology,
		catalog,
		rpc,
		recorder,
		rng,
		now,
	)
}

type soakVerifyClass = soakread.VerifyClass

const (
	soakVerifyOK        = soakread.VerifyOK
	soakVerifySkipped   = soakread.VerifySkipped
	soakVerifyMissing   = soakread.VerifyMissing
	soakVerifyMismatch  = soakread.VerifyMismatch
	soakVerifyMalformed = soakread.VerifyMalformed
	soakVerifyRetryable = soakread.VerifyRetryable
	soakVerifyRPCError  = soakread.VerifyRPCError
)

type soakVerifyField = soakread.VerifyField

const (
	soakVerifyFieldNone       = soakread.VerifyFieldNone
	soakVerifyFieldMessageID  = soakread.VerifyFieldMessageID
	soakVerifyFieldRoomID     = soakread.VerifyFieldRoomID
	soakVerifyFieldAuthor     = soakread.VerifyFieldAuthor
	soakVerifyFieldDeleted    = soakread.VerifyFieldDeleted
	soakVerifyFieldContent    = soakread.VerifyFieldContent
	soakVerifyFieldEditedAt   = soakread.VerifyFieldEditedAt
	soakVerifyFieldPagination = soakread.VerifyFieldPagination
)

type soakVerifyConfig = soakread.VerifyConfig
type soakVerifyMessage = soakread.VerifyMessage
type soakVerifyResult = soakread.VerifyResult
type soakVerifyResultRecorder = soakread.VerifyResultRecorder
type soakVerifier = soakread.Verifier

func newSoakVerifier(
	cfg *soakVerifyConfig,
	catalog *soakCatalog,
	rpc *soakRPCClient,
	recorder soakVerifyResultRecorder,
	now func() time.Time,
) *soakVerifier {
	return soakread.NewVerifier(cfg, catalog, rpc, recorder, now)
}

func compareSoakVerifiedMessage(
	result *soakVerifyResult,
	expected *soakCatalogMessage,
	actual *soakVerifyMessage,
) {
	soakread.CompareVerifiedMessage(result, expected, actual)
}

func classifySoakVerifyRPCError(
	result *soakVerifyResult,
	class soakErrorClass,
	reason soakErrorReason,
) {
	soakread.ClassifyRPCError(result, class, reason)
}
