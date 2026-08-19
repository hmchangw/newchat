package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type bufferedFailureJournal interface {
	failureJournal
	AppendBuffered(*failureLedgerEvent) error
	Sync() error
}

type failureJournalGroupRequest struct {
	event      *failureLedgerEvent
	compact    []failureLedgerEvent
	compactSet bool
	response   chan error
	close      bool
}

type failureJournalGroupCommit struct {
	journal      bufferedFailureJournal
	maxDelay     time.Duration
	maxBatchSize int
	requests     chan failureJournalGroupRequest
	done         chan struct{}
	metrics      *Metrics
	stateMu      sync.RWMutex
	closed       bool
}

func newFailureJournalGroupCommit(
	journal bufferedFailureJournal,
	maxDelay time.Duration,
	maxBatchSize int,
	metricSets ...*Metrics,
) *failureJournalGroupCommit {
	if maxDelay <= 0 {
		maxDelay = 10 * time.Millisecond
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 256
	}
	group := &failureJournalGroupCommit{
		journal: journal, maxDelay: maxDelay, maxBatchSize: maxBatchSize,
		requests: make(chan failureJournalGroupRequest, maxBatchSize), done: make(chan struct{}),
	}
	if len(metricSets) > 0 {
		group.metrics = metricSets[0]
	}
	go group.run()
	return group
}

func (g *failureJournalGroupCommit) Replay() ([]failureLedgerEvent, error) {
	return g.journal.Replay()
}

// ReplayEach forwards the streaming recovery path when the wrapped journal has
// one; a wrapper that swallowed it would silently reinstate buffering replay.
func (g *failureJournalGroupCommit) ReplayEach(emit func(*failureLedgerEvent) error) error {
	if streaming, ok := g.journal.(streamingFailureJournal); ok {
		return streaming.ReplayEach(emit)
	}
	// A journal that cannot stream still has to recover; buffering here keeps
	// the old behaviour for it rather than failing the run.
	events, err := g.journal.Replay()
	if err != nil {
		return err
	}
	for i := range events {
		if err := emit(&events[i]); err != nil {
			return err
		}
	}
	return nil
}

func (g *failureJournalGroupCommit) Append(event *failureLedgerEvent) error {
	if event == nil {
		return fmt.Errorf("failure journal event is required")
	}
	response := make(chan error, 1)
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	if g.closed {
		return fmt.Errorf("failure journal is closed")
	}
	g.requests <- failureJournalGroupRequest{event: event, response: response}
	return <-response
}

func (g *failureJournalGroupCommit) Compact(events []failureLedgerEvent) error {
	response := make(chan error, 1)
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	if g.closed {
		return fmt.Errorf("failure journal is closed")
	}
	g.requests <- failureJournalGroupRequest{
		compact: append([]failureLedgerEvent(nil), events...), compactSet: true, response: response,
	}
	return <-response
}

func (g *failureJournalGroupCommit) Size() int64 { return g.journal.Size() }

func (g *failureJournalGroupCommit) Close() error {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	if g.closed {
		return nil
	}
	g.closed = true
	response := make(chan error, 1)
	g.requests <- failureJournalGroupRequest{close: true, response: response}
	err := <-response
	<-g.done
	return err
}

func (g *failureJournalGroupCommit) ConfigureObserverContract(
	contract failureObserverContract,
	active []failureOperation,
) error {
	configurer, ok := g.journal.(interface {
		ConfigureObserverContract(failureObserverContract, []failureOperation) error
	})
	if !ok {
		return nil
	}
	return configurer.ConfigureObserverContract(contract, active)
}

func (g *failureJournalGroupCommit) NeedsUpgrade() bool {
	upgrade, ok := g.journal.(interface{ NeedsUpgrade() bool })
	return ok && upgrade.NeedsUpgrade()
}

func (g *failureJournalGroupCommit) run() {
	defer close(g.done)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	var timerC <-chan time.Time
	dirty := false
	batchSize := 0
	var stickyErr error
	barriers := make([]chan error, 0, g.maxBatchSize)

	stopTimer := func() {
		if timerC == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	flush := func() error {
		stopTimer()
		if !dirty {
			return stickyErr
		}
		flushBatchSize := batchSize
		startedAt := time.Now()
		err := g.journal.Sync()
		if g.metrics != nil {
			result := "success"
			if err != nil {
				result = "error"
			}
			g.metrics.FailureWALFlushDuration.WithLabelValues(result).Observe(time.Since(startedAt).Seconds())
			g.metrics.FailureWALFlushBatchSize.WithLabelValues(result).Observe(float64(flushBatchSize))
		}
		if err != nil && stickyErr == nil {
			stickyErr = fmt.Errorf("sync grouped failure journal: %w", err)
		}
		dirty = false
		batchSize = 0
		for _, response := range barriers {
			response <- stickyErr
		}
		barriers = barriers[:0]
		return stickyErr
	}
	armTimer := func() {
		if timerC != nil {
			return
		}
		timer.Reset(g.maxDelay)
		timerC = timer.C
	}

	for {
		select {
		case <-timerC:
			timerC = nil
			_ = flush()
		case request := <-g.requests:
			switch {
			case request.close:
				flushErr := flush()
				request.response <- errors.Join(flushErr, g.journal.Close())
				return
			case request.compactSet:
				if err := flush(); err != nil {
					request.response <- err
					continue
				}
				err := g.journal.Compact(request.compact)
				if err != nil {
					stickyErr = fmt.Errorf("compact grouped failure journal: %w", err)
				}
				request.response <- err
			case request.event != nil:
				if stickyErr != nil {
					request.response <- stickyErr
					continue
				}
				if err := g.journal.AppendBuffered(request.event); err != nil {
					stickyErr = fmt.Errorf("append grouped failure journal: %w", err)
					request.response <- stickyErr
					for _, response := range barriers {
						response <- stickyErr
					}
					barriers = barriers[:0]
					continue
				}
				dirty = true
				batchSize++
				armTimer()
				if request.event.Type == failureLedgerEventStarted {
					barriers = append(barriers, request.response)
				} else {
					request.response <- nil
				}
				if batchSize >= g.maxBatchSize {
					_ = flush()
				}
			}
		}
	}
}
