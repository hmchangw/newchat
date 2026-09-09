package failure

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type groupRequest struct {
	event      *Event
	compact    []Event
	compactSet bool
	response   chan error
	close      bool
}

type GroupCommit struct {
	journal      BufferedJournal
	maxDelay     time.Duration
	maxBatchSize int
	requests     chan groupRequest
	done         chan struct{}
	recorder     WALFlushRecorder
	stateMu      sync.RWMutex
	closed       bool
}

func NewGroupCommit(
	journal BufferedJournal,
	maxDelay time.Duration,
	maxBatchSize int,
	recorders ...WALFlushRecorder,
) *GroupCommit {
	if maxDelay <= 0 {
		maxDelay = 10 * time.Millisecond
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 256
	}
	group := &GroupCommit{
		journal: journal, maxDelay: maxDelay, maxBatchSize: maxBatchSize,
		requests: make(chan groupRequest, maxBatchSize), done: make(chan struct{}),
	}
	if len(recorders) > 0 {
		group.recorder = recorders[0]
	}
	go group.run()
	return group
}

func (g *GroupCommit) Replay() ([]Event, error) {
	return g.journal.Replay()
}

// ReplayEach forwards streaming recovery when the wrapped journal supports it.
func (g *GroupCommit) ReplayEach(emit func(*Event) error) error {
	if streaming, ok := g.journal.(StreamingJournal); ok {
		return streaming.ReplayEach(emit)
	}
	events, err := g.journal.Replay()
	if err != nil {
		return fmt.Errorf("replay group-commit failure journal: %w", err)
	}
	for index := range events {
		if err := emit(&events[index]); err != nil {
			return fmt.Errorf("apply group-commit failure journal event %d: %w", index, err)
		}
	}
	return nil
}

func (g *GroupCommit) Append(event *Event) error {
	if event == nil {
		return fmt.Errorf("failure journal event is required")
	}
	response := make(chan error, 1)
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	if g.closed {
		return fmt.Errorf("failure journal is closed")
	}
	g.requests <- groupRequest{event: event, response: response}
	return <-response
}

func (g *GroupCommit) Compact(events []Event) error {
	response := make(chan error, 1)
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	if g.closed {
		return fmt.Errorf("failure journal is closed")
	}
	g.requests <- groupRequest{
		compact: append([]Event(nil), events...), compactSet: true, response: response,
	}
	return <-response
}

func (g *GroupCommit) Size() int64 { return g.journal.Size() }

func (g *GroupCommit) Close() error {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	if g.closed {
		return nil
	}
	g.closed = true
	response := make(chan error, 1)
	g.requests <- groupRequest{close: true, response: response}
	err := <-response
	<-g.done
	return err
}

func (g *GroupCommit) ConfigureObserverContract(contract ObserverContract, active []Operation) error {
	configurer, ok := g.journal.(ObserverContractJournal)
	if !ok {
		return nil
	}
	return configurer.ConfigureObserverContract(contract, active)
}

func (g *GroupCommit) NeedsUpgrade() bool {
	upgrade, ok := g.journal.(UpgradeJournal)
	return ok && upgrade.NeedsUpgrade()
}

func (g *GroupCommit) run() {
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
		if g.recorder != nil {
			g.recorder.RecordWALFlush(time.Since(startedAt), flushBatchSize, err)
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
				// Intent and invalidation records are not accepted until the
				// complete batch crosses the durability barrier.
				if request.event.Type == EventStarted || request.event.Type == EventInvalidated {
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
