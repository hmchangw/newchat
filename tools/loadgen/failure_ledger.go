package main

import failuremodel "github.com/hmchangw/chat/tools/loadgen/internal/failure"

var (
	errFailureLedgerCapacity     = failuremodel.ErrLedgerCapacity
	errFailureOperationNotActive = failuremodel.ErrOperationNotActive
	errFailureLedgerClosed       = failuremodel.ErrLedgerClosed
)

const (
	invalidReasonCapacity          = failuremodel.InvalidReasonCapacity
	invalidReasonWAL               = failuremodel.InvalidReasonWAL
	invalidReasonReconcileCapacity = failuremodel.InvalidReasonReconcileCapacity
	invalidReasonReconcileLagRange = failuremodel.InvalidReasonReconcileLagRange
	invalidReasonLeaseAbort        = failuremodel.InvalidReasonLeaseAbort
)

var failureInvalidationReasonRegistry = map[string]struct{}{
	invalidReasonCapacity: {}, invalidReasonWAL: {}, "accounting_invariant": {},
	"observer_queue": {}, invalidReasonReconcileCapacity: {},
	invalidReasonReconcileLagRange: {}, invalidReasonLeaseAbort: {},
	"observer_malformed": {}, "recipient_recovery": {}, "recipient_observer": {},
	"timeline": {}, "other": {}, "sidecar": {},
}

type failureLedgerConfig = failuremodel.LedgerConfig
type failureLedgerRecorder = failuremodel.LedgerRecorder
type failureLedgerSnapshot = failuremodel.LedgerSnapshot
type failureLedger = failuremodel.Ledger

func newFailureLedger(cfg *failureLedgerConfig) (*failureLedger, error) {
	return failuremodel.NewLedger(cfg)
}
