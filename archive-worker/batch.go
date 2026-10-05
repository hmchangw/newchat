package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type locatable interface {
	SetLocation(segmentKey string, frameOffset int64)
}

type docSpec struct {
	Index string
	ID    string
	Doc   locatable
}

type item struct {
	ctx   context.Context
	msg   jetstream.Msg
	seq   uint64
	frame []byte
	docs  []docSpec
}

type batcher struct {
	mu        sync.Mutex
	items     []item
	bytes     int
	firstAt   time.Time
	maxEvents int
	maxBytes  int
	fill      time.Duration
}

func newBatcher(maxEvents, maxBytes int, fill time.Duration) *batcher {
	return &batcher{maxEvents: maxEvents, maxBytes: maxBytes, fill: fill}
}

// add appends and reports whether a count or byte bound tripped.
func (b *batcher) add(it item, now time.Time) bool { //nolint:gocritic // hugeParam: value param is the brief's interface; items are moved into the batch
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		b.firstAt = now
	}
	b.items = append(b.items, it)
	b.bytes += len(it.frame)
	return len(b.items) >= b.maxEvents || b.bytes >= b.maxBytes
}

func (b *batcher) due(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items) > 0 && !now.Before(b.firstAt.Add(b.fill))
}

func (b *batcher) take() []item {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		return nil
	}
	out := b.items
	b.items, b.bytes = nil, 0
	return out
}

func (b *batcher) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

type sealed struct {
	key     string
	body    []byte
	actions []searchengine.BulkAction
	items   []item
}

// seal writes the segment for items in delivery order, points every document
// at its frame, and returns the create actions in the same order as the
// documents so flush can map results back to items.
func seal(site, lane string, items []item, now time.Time) (*sealed, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("seal: no items")
	}
	first, last := items[0].seq, items[0].seq
	frames := make([][]byte, len(items))
	for i, it := range items {
		frames[i] = it.frame
		first, last = min(first, it.seq), max(last, it.seq)
	}
	key := auditarchive.SegmentKey(site, lane, now, first, last)
	var buf bytes.Buffer
	offsets, _, err := auditarchive.WriteSegment(&buf, auditarchive.Header{Site: site, Lane: lane, FirstSeq: first, LastSeq: last, Count: uint32(len(frames))}, frames)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	var actions []searchengine.BulkAction
	for i, it := range items {
		for _, d := range it.docs {
			d.Doc.SetLocation(key, offsets[i])
			body, err := json.Marshal(d.Doc)
			if err != nil {
				return nil, fmt.Errorf("seal: marshal doc %s: %w", d.ID, err)
			}
			actions = append(actions, searchengine.BulkAction{Action: searchengine.ActionCreate, Index: d.Index, DocID: d.ID, Doc: body})
		}
	}
	return &sealed{key: key, body: buf.Bytes(), actions: actions, items: items}, nil
}
