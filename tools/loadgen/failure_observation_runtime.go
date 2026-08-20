package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

var failureRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func openSoakFailureObservationLedger(
	cfg *soakConfig,
	metrics *Metrics,
	now func() time.Time,
) (*failureLedger, bool, error) {
	ledger, err := openSoakFailureLedger(cfg, metrics, now)
	if err == nil {
		return ledger, false, nil
	}
	if errors.Is(err, errFailureObserverContractMismatch) || cfg == nil {
		return nil, false, err
	}
	fallback, fallbackErr := newFailureLedger(&failureLedgerConfig{
		Capacity: cfg.LedgerCapacity,
		Now:      now,
		Recorder: newFailureLedgerPromRecorder(metrics),
	})
	if fallbackErr != nil {
		return nil, false, errors.Join(err, fallbackErr)
	}
	fallback.Invalidate("wal")
	recordFailureObserverConfiguration(metrics, newFailureObserverContract(
		cfg.RecipientObserverEnabled, cfg.SearchObserverEnabled,
	))
	return fallback, true, nil
}

// soakFailureObservationRuntime owns only the optional recipient-observation
// goroutine and subscriptions. Fault injection, campaign windows, and verdicts
// remain external to the load generator.
type soakFailureObservationRuntime struct {
	recipient     *failureRecipientObserver
	subscriptions *failureRecipientSubscriptions
	stop          context.CancelFunc
	closeOnce     sync.Once
	closeErr      error
}

func newSoakFailureObservationRuntime(
	enabled bool,
	ledger *failureLedger,
	metrics *Metrics,
	queue int,
	evidenceDirectory string,
	now func() time.Time,
) *soakFailureObservationRuntime {
	runtime := &soakFailureObservationRuntime{}
	if !enabled {
		return runtime
	}
	options := make([]failureRecipientObserverOption, 0, 1)
	if evidenceDirectory != "" {
		options = append(options, withFailureRecipientEvidenceDir(evidenceDirectory))
	}
	runtime.recipient = newFailureRecipientObserver(ledger, metrics, queue, now, options...)
	return runtime
}

func (r *soakFailureObservationRuntime) StartRecipient(
	source failureRecipientSource,
	topology *soakTopology,
	active []failureOperation,
) error {
	if r == nil || r.recipient == nil {
		return nil
	}
	if err := r.recipient.Recover(active); err != nil {
		return fmt.Errorf("recover recipient observer expectations: %w", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	r.stop = stop
	r.recipient.Run(ctx)
	subscriptions, err := startFailureRecipientSubscriptions(source, topology, r.recipient)
	if err != nil {
		stop()
		r.recipient.Drain()
		return fmt.Errorf("start recipient observer subscriptions: %w", err)
	}
	r.subscriptions = subscriptions
	return nil
}

// Evidence exposes the recipient expectation map so the failure sweep can bound
// it by the same deadline the ledger uses. nil when the observer is off, which
// ExpireBefore handles.
func (r *soakFailureObservationRuntime) Evidence() *recipientEvidence {
	if r == nil || r.recipient == nil {
		return nil
	}
	return r.recipient.evidence
}

func (r *soakFailureObservationRuntime) Recipient() *failureRecipientObserver {
	if r == nil {
		return nil
	}
	return r.recipient
}

func (r *soakFailureObservationRuntime) Close() error {
	if r == nil || r.recipient == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.closeErr = shutdownFailureRecipientObserver(r.subscriptions, r.stop, r.recipient)
	})
	return r.closeErr
}
