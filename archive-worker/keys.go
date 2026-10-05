package main

import (
	"bytes"
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

// currentKeyObject is the bucket escrow copy of the site's KeyDoc. The bucket
// holds the wrapped DEK beside the data it protects; the index copy is the
// fast path every reader uses.
func currentKeyObject(site string) string { return site + "/keys/current.json" }

// keyObject is the escrow copy named by key id, which segment and blob
// headers carry, so a reader can find the key for any object.
func keyObject(site, keyID string) string { return site + "/keys/" + keyID + ".json" }

// loadOrCreateDEK returns the site's plaintext DEK and its key id.
//
//   - Index doc present: unwrap it, and escrow it in the bucket if
//     current.json is missing there.
//   - Index doc missing but current.json present: refuse. Minting would orphan
//     every object sealed under the escrowed key; an operator restores the
//     index document from the bucket copy instead.
//   - Neither: mint, escrow both bucket objects, then create the index doc. A
//     create conflict (another replica won) re-reads the winner and escrows it
//     again, so the newest current.json always names the key the index holds.
func loadOrCreateDEK(ctx context.Context, idx indexStore, objects objectStore, wrapper atrest.KeyWrapper, site string, now func() time.Time) ([]byte, string, error) {
	index := auditarchive.KeysIndex(site)
	doc, found, err := readKeyDoc(ctx, idx, index)
	if err != nil {
		return nil, "", err
	}
	if found {
		dek, err := unwrapDEK(ctx, wrapper, doc)
		if err != nil {
			return nil, "", err
		}
		escrowed, err := objects.Stat(ctx, currentKeyObject(site))
		if err != nil {
			return nil, "", fmt.Errorf("check key escrow: %w", err)
		}
		if !escrowed {
			if err := escrowKeyDoc(ctx, objects, site, doc); err != nil {
				return nil, "", err
			}
			slog.Info("archive DEK escrowed to the bucket", "site", site, "keyId", auditarchive.KeyID(doc.WrappedDek))
		}
		return dek, auditarchive.KeyID(doc.WrappedDek), nil
	}

	escrowed, err := objects.Stat(ctx, currentKeyObject(site))
	if err != nil {
		return nil, "", fmt.Errorf("check key escrow: %w", err)
	}
	if escrowed {
		return nil, "", fmt.Errorf("archive: audit-keys doc missing but bucket holds %s — restore the index document from it before starting", currentKeyObject(site))
	}

	plain, wrapped, err := wrapper.GenerateDataKey(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("generate archive DEK: %w", err)
	}
	minted := auditarchive.KeyDoc{SiteID: site, WrappedDek: wrapped, CreatedAt: now()}
	if err := escrowKeyDoc(ctx, objects, site, minted); err != nil {
		return nil, "", err
	}
	body, err := json.Marshal(minted)
	if err != nil {
		return nil, "", fmt.Errorf("marshal key doc: %w", err)
	}
	results, err := idx.Bulk(ctx, []searchengine.BulkAction{{Action: searchengine.ActionCreate, Index: index, DocID: auditarchive.KeyDocID, Doc: body}})
	if err != nil {
		return nil, "", fmt.Errorf("create key doc: %w", err)
	}
	if len(results) != 1 {
		return nil, "", fmt.Errorf("create key doc: %d results for 1 action", len(results))
	}
	switch {
	case results[0].Status >= 200 && results[0].Status < 300:
		slog.Info("archive DEK created", "site", site, "index", index, "keyId", auditarchive.KeyID(wrapped))
		return plain, auditarchive.KeyID(wrapped), nil
	case results[0].Status == 409:
		slog.Info("archive DEK already created by a peer, re-reading", "site", site)
		winner, found, err := readKeyDoc(ctx, idx, index)
		if err != nil {
			return nil, "", err
		}
		if !found {
			return nil, "", errors.New("create key doc: conflict but no document found")
		}
		dek, err := unwrapDEK(ctx, wrapper, winner)
		if err != nil {
			return nil, "", err
		}
		if err := escrowKeyDoc(ctx, objects, site, winner); err != nil {
			return nil, "", err
		}
		return dek, auditarchive.KeyID(winner.WrappedDek), nil
	default:
		return nil, "", fmt.Errorf("create key doc: status %d %s", results[0].Status, results[0].ErrorType)
	}
}

// escrowKeyDoc writes doc to the bucket under its key id, then as current.json.
func escrowKeyDoc(ctx context.Context, objects objectStore, site string, doc auditarchive.KeyDoc) error { //nolint:gocritic // hugeParam: a small value written once at startup
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal key doc: %w", err)
	}
	for _, key := range []string{keyObject(site, auditarchive.KeyID(doc.WrappedDek)), currentKeyObject(site)} {
		if err := objects.Put(ctx, key, bytes.NewReader(body), int64(len(body)), "application/json"); err != nil {
			return fmt.Errorf("escrow key doc to %s: %w", key, err)
		}
	}
	return nil
}

func readKeyDoc(ctx context.Context, idx indexStore, index string) (auditarchive.KeyDoc, bool, error) {
	raw, found, err := idx.GetDoc(ctx, index, auditarchive.KeyDocID)
	if err != nil {
		return auditarchive.KeyDoc{}, false, fmt.Errorf("read key doc: %w", err)
	}
	if !found {
		return auditarchive.KeyDoc{}, false, nil
	}
	// GetDoc returns the whole hit; the document is under _source.
	var hit struct {
		Source auditarchive.KeyDoc `json:"_source"`
	}
	if err := json.Unmarshal(raw, &hit); err != nil {
		return auditarchive.KeyDoc{}, false, fmt.Errorf("decode key doc: %w", err)
	}
	return hit.Source, true, nil
}

func unwrapDEK(ctx context.Context, wrapper atrest.KeyWrapper, doc auditarchive.KeyDoc) ([]byte, error) { //nolint:gocritic // hugeParam: a small value read once at startup
	dek, err := wrapper.Unwrap(ctx, doc.WrappedDek)
	if err != nil {
		return nil, fmt.Errorf("unwrap archive DEK: %w", err)
	}
	if len(dek) != auditarchive.DEKSize {
		return nil, fmt.Errorf("unwrapped DEK is %d bytes, want %d", len(dek), auditarchive.DEKSize)
	}
	return dek, nil
}
