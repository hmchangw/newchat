package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	ctx     context.Context
	msg     jetstream.Msg
	seq     uint64
	frame   []byte
	hash    string    // keyed record digest every doc of the item carries as contentHash
	eventAt time.Time // the record's event time, for the lag metric
	docs    []docSpec
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

// seal writes the segment for items in delivery order, stamped with the key
// id of the DEK that sealed the frames, points every document at its frame,
// and returns the create actions in the same order as the documents so flush
// can map results back to items.
func seal(site, lane, keyID string, items []item, now time.Time) (*sealed, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("seal: no items")
	}
	if uint64(len(items)) > math.MaxUint32 {
		return nil, fmt.Errorf("seal: %d items exceed the segment count field", len(items))
	}
	first, last := items[0].seq, items[0].seq
	frames := make([][]byte, len(items))
	for i := range items {
		frames[i] = items[i].frame
		first, last = min(first, items[i].seq), max(last, items[i].seq)
	}
	key := auditarchive.SegmentKey(site, lane, now, first, last)
	// #nosec G115 -- len(frames) == len(items) <= math.MaxUint32, checked above
	count := uint32(len(frames))
	var buf bytes.Buffer
	offsets, _, err := auditarchive.WriteSegment(&buf, auditarchive.Header{Site: site, Lane: lane, KeyID: keyID, FirstSeq: first, LastSeq: last, Count: count}, frames)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	var actions []searchengine.BulkAction
	for i := range items {
		for _, d := range items[i].docs {
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
