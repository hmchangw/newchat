package natsmetrics

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// Iterator is the context-aware pull iterator surface used by the o11y NATS
// wrapper. It is intentionally small so loop failure behavior is unit-testable.
type Iterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
}

type ClassifyEvent func(jetstream.Msg) EventType
type ProcessMessage func(context.Context, *Message)

// Start registers the consumer loop with wg and then runs Consume in its own
// goroutine. Registering before returning is the point: shutdown stops the
// iterator and then waits on wg, so a loop counted only once it dispatches a
// message lets that wait pass through while a message Next already returned is
// still on its way to a worker.
func Start(ctx context.Context, iter Iterator, consumer *Consumer, maxWorkers, maxDeliver int, wg *sync.WaitGroup, classify ClassifyEvent, process ProcessMessage) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		Consume(ctx, iter, consumer, maxWorkers, maxDeliver, wg, classify, process)
	}()
}

// Consume runs the existing bounded concurrent pull pattern. Callers mark the
// loop started only after iterator creation succeeds; Consume marks it stopped
// before returning from a terminal Next failure.
//
// classify runs inside the worker goroutine, not on the dispatch path: a
// classifier that parses the payload would otherwise serialize the whole
// consumer behind one message at a time.
func Consume(ctx context.Context, iter Iterator, consumer *Consumer, maxWorkers, maxDeliver int, wg *sync.WaitGroup, classify ClassifyEvent, process ProcessMessage) {
	// A zero maxWorkers would make sem unbuffered and park the dispatch send
	// forever — the loop stops consuming with no error and no terminal metric,
	// while the loop-up gauge still reads 1. A negative one panics in make.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	sem := make(chan struct{}, maxWorkers)
	for {
		msgCtx, msg, err := iter.Next()
		if err != nil {
			consumer.LoopFailed(ctx, err)
			return
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(msgCtx context.Context, msg jetstream.Msg) {
			eventType := EventUnknown
			if classify != nil {
				eventType = NormalizeEventType(string(classify(msg)))
			}
			tracked := consumer.Track(msgCtx, msg, eventType, maxDeliver)
			defer func() {
				tracked.Finish(msgCtx)
				<-sem
				wg.Done()
			}()
			process(tracked.Context(msgCtx), tracked)
		}(msgCtx, msg)
	}
}
