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

type failureLedgerConfig = failuremodel.LedgerConfig
type failureLedger = failuremodel.Ledger

func newFailureLedger(cfg *failureLedgerConfig) (*failureLedger, error) {
	return failuremodel.NewLedger(cfg)
}
