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

// Consume runs the existing bounded concurrent pull pattern. Callers mark the
// loop started only after iterator creation succeeds; Consume marks it stopped
// before returning from a terminal Next failure.
//
// classify runs inside the worker goroutine, not on the dispatch path: a
// classifier that parses the payload would otherwise serialize the whole
// consumer behind one message at a time.
func Consume(ctx context.Context, iter Iterator, consumer *Consumer, maxWorkers, maxDeliver int, wg *sync.WaitGroup, classify ClassifyEvent, process ProcessMessage) {
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
