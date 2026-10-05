package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// loadOrCreateDEK returns the site's plaintext DEK. On an empty keys index it
// generates one, publishes the wrapped form with op_type create, and on a
// create conflict (another replica won) re-reads and unwraps that one.
func loadOrCreateDEK(ctx context.Context, idx indexStore, wrapper atrest.KeyWrapper, site string, now func() time.Time) ([]byte, error) {
	index := auditarchive.KeysIndex(site)
	if dek, found, err := readDEK(ctx, idx, wrapper, index); err != nil || found {
		return dek, err
	}
	plain, wrapped, err := wrapper.GenerateDataKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("generate archive DEK: %w", err)
	}
	doc, err := json.Marshal(auditarchive.KeyDoc{SiteID: site, WrappedDek: wrapped, CreatedAt: now()})
	if err != nil {
		return nil, fmt.Errorf("marshal key doc: %w", err)
	}
	results, err := idx.Bulk(ctx, []searchengine.BulkAction{{Action: searchengine.ActionCreate, Index: index, DocID: auditarchive.KeyDocID, Doc: doc}})
	if err != nil {
		return nil, fmt.Errorf("create key doc: %w", err)
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("create key doc: %d results for 1 action", len(results))
	}
	switch {
	case results[0].Status >= 200 && results[0].Status < 300:
		slog.Info("archive DEK created", "site", site, "index", index)
		return plain, nil
	case results[0].Status == 409:
		slog.Info("archive DEK already created by a peer, re-reading", "site", site)
		dek, found, err := readDEK(ctx, idx, wrapper, index)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("create key doc: conflict but no document found")
		}
		return dek, nil
	default:
		return nil, fmt.Errorf("create key doc: status %d %s", results[0].Status, results[0].ErrorType)
	}
}

func readDEK(ctx context.Context, idx indexStore, wrapper atrest.KeyWrapper, index string) ([]byte, bool, error) {
	raw, found, err := idx.GetDoc(ctx, index, auditarchive.KeyDocID)
	if err != nil {
		return nil, false, fmt.Errorf("read key doc: %w", err)
	}
	if !found {
		return nil, false, nil
	}
	// GetDoc returns the whole hit; the document is under _source.
	var hit struct {
		Source auditarchive.KeyDoc `json:"_source"`
	}
	if err := json.Unmarshal(raw, &hit); err != nil {
		return nil, false, fmt.Errorf("decode key doc: %w", err)
	}
	dek, err := wrapper.Unwrap(ctx, hit.Source.WrappedDek)
	if err != nil {
		return nil, false, fmt.Errorf("unwrap archive DEK: %w", err)
	}
	if len(dek) != auditarchive.DEKSize {
		return nil, false, fmt.Errorf("unwrapped DEK is %d bytes, want %d", len(dek), auditarchive.DEKSize)
	}
	return dek, true, nil
}
