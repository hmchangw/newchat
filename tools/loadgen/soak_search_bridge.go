package main

import (
	"context"
	"math/rand"
	"time"

	soaksearch "github.com/hmchangw/chat/tools/loadgen/internal/soak/search"
)

type soakSearchIndexResult = soaksearch.IndexResult

const (
	soakSearchIndexFound    = soaksearch.IndexFound
	soakSearchIndexMissing  = soaksearch.IndexMissing
	soakSearchIndexUnknown  = soaksearch.IndexUnknown
	soakSearchIndexTooEarly = soaksearch.IndexTooEarly
)

type soakSearchConfig = soaksearch.Config
type soakSearchReader = soaksearch.Reader

func newSoakSearchReader(
	cfg soakSearchConfig,
	topology *soakTopology,
	rpc *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*soakSearchReader, error) {
	return soaksearch.New(cfg, topology, rpc, recorder, rng, now)
}

// soakSearchIndexProbe adapts the root failure ledger operation to the search
// package. The failure types stay at the root until the later failure refactor.
type soakSearchIndexProbe struct {
	reader  *soakSearchReader
	catalog *soakCatalog
}

func newSoakSearchIndexProbe(
	reader *soakSearchReader,
	catalog *soakCatalog,
) *soakSearchIndexProbe {
	return &soakSearchIndexProbe{reader: reader, catalog: catalog}
}

// Indexed reports the verdict and whether it issued a query. Every path that
// answers from local state returns false so the reconciler can refund the read
// allowance when no request reached search-service.
func (p *soakSearchIndexProbe) Indexed(
	ctx context.Context,
	operation *failureOperation,
) (soakSearchIndexResult, bool, error) {
	if p == nil || p.reader == nil || p.catalog == nil || operation == nil {
		return soakSearchIndexUnknown, false, nil
	}
	roomID := operation.Targets["roomId"]
	messageID := operation.Targets["messageId"]
	account := operation.Attributes[soakFailureAttributeAccount]
	if roomID == "" || messageID == "" || account == "" {
		return soakSearchIndexUnknown, false, nil
	}
	message, known := p.catalog.Get(roomID, messageID)
	if !known || message.SearchTerm == "" {
		return soakSearchIndexUnknown, false, nil
	}
	return p.reader.IndexedAt(
		ctx, account, roomID, messageID, message.SearchTerm, operation.StartedAt,
	)
}

func (p *soakSearchIndexProbe) SettleBoundary(publishedAt time.Time) time.Time {
	if p == nil || p.reader == nil {
		return publishedAt
	}
	return p.reader.SettleBoundary(publishedAt)
}
