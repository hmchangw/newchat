// Package preview builds, seals and stores the room-list preview. Every path
// that produces one runs through here — history-service's read-time walk, its
// warm-back, and its post-mutation writes — so the shape is identical
// regardless of origin.
//
// A stored preview is split: the metadata Cassandra also leaves unencrypted
// stays clear so the freshness check and the guarded write can work
// server-side, while the user-authored body is sealed under a per-site DEK
// (see Seal). Writes land through the watermark-guarded field sets so an
// out-of-order redelivery cannot overwrite a newer preview.
package preview

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

// MaxContentRunes caps PreviewMessage.Content: the room list renders a snippet,
// so persisting/shipping the full (≤20KB) body is pure waste.
const MaxContentRunes = 500

// Build normalizes a composed preview into its wire/storage form, truncating
// content and forcing the timestamp to UTC. Callers assemble the raw fields;
// taking model.PreviewMessage itself (rather than a parallel params struct)
// keeps a newly added field from silently defaulting on every write path.
// Sender must arrive with DisplayName already bot-aware resolved
// (BotAwareDisplayName).
//
//nolint:gocritic // hugeParam: PreviewMessage is the stored/wire shape itself; by-value keeps callers simple and the copy cost is negligible.
func Build(p model.PreviewMessage) model.PreviewMessage {
	p.Content = truncateContent(p.Content)
	p.CreatedAt = p.CreatedAt.UTC()
	return p
}

// truncateContent caps s at MaxContentRunes runes (never splitting a rune).
func truncateContent(s string) string {
	// Byte length bounds rune count, so the common short message needs no scan.
	if len(s) <= MaxContentRunes {
		return s
	}
	n := 0
	for i := range s { // ranging a string yields each rune's start offset
		if n == MaxContentRunes {
			return s[:i]
		}
		n++
	}
	return s
}

// AppNameLookup resolves a bot account's app display name; ("", nil) means no
// app matches.
type AppNameLookup func(ctx context.Context, botAccount string) (string, error)

// BotAwareDisplayName composes a render-ready name; for a bot account it
// prefers the app's display name, degrading to the composed name on
// miss/error/nil lookup.
func BotAwareDisplayName(ctx context.Context, lookup AppNameLookup, engName, chineseName, account string) string {
	name := displayfmt.CombineWithFallback(engName, chineseName, account)
	if model.IsBot(account) && lookup != nil {
		if appName, err := lookup(ctx, account); err != nil {
			slog.WarnContext(ctx, "app name lookup failed, using composed name", "account", account, "error", err)
		} else if appName != "" {
			name = appName
		}
	}
	return name
}

// previewDocFields is the single list of room-doc keys that together make up a
// stored preview. Both write paths iterate it, so they cannot drift apart and
// strand a fragment — a nonce and epoch describing a ciphertext that no longer
// exists. previewAsOf is deliberately absent: it is the watermark itself and
// advances on every landing write, set or clear.
var previewDocFields = []string{
	"previewMeta",
	"previewCiphertext",
	"previewNonce",
	"previewKeyEpoch",
	"previewForMsgId",
}

// guardedFields builds the watermark-guarded $set shared by the set and clear
// paths: each preview field becomes onPass[field] when asOf >= the stored
// previewAsOf (missing => 0; ties => last writer wins), and previewAsOf
// advances with it. Both paths route through here so the ordering rule cannot
// drift between them.
func guardedFields(asOf int64, onPass map[string]any) bson.M {
	cond := bson.M{"$gte": bson.A{asOf, bson.M{"$ifNull": bson.A{"$previewAsOf", 0}}}}
	out := bson.M{"previewAsOf": bson.M{"$cond": bson.A{cond, asOf, "$previewAsOf"}}}
	for _, f := range previewDocFields {
		out[f] = bson.M{"$cond": bson.A{cond, onPass[f], "$" + f}}
	}
	return out
}

// GuardedSetFields returns the $set fields for an aggregation-pipeline update
// that applies s+asOf under the watermark guard. $literal shields each stored
// value from aggregation evaluation (a sender display name may legitimately
// start with "$").
//
//nolint:gocritic // hugeParam: Sealed is the stored shape itself; by-value matches Seal/Open.
func GuardedSetFields(s Sealed, asOf int64) bson.M {
	return guardedFields(asOf, map[string]any{
		"previewMeta":       bson.M{"$literal": s.Meta},
		"previewCiphertext": bson.M{"$literal": s.Ciphertext},
		"previewNonce":      bson.M{"$literal": s.Nonce},
		"previewKeyEpoch":   bson.M{"$literal": s.KeyEpoch},
		"previewForMsgId":   bson.M{"$literal": s.ForMsgID},
	})
}

// GuardedClearFields returns the $set fields for an aggregation-pipeline update
// that REMOVES every stored preview field under the watermark guard, while
// still advancing previewAsOf — so a redelivered older write cannot resurrect
// the cleared preview.
func GuardedClearFields(asOf int64) bson.M {
	onPass := make(map[string]any, len(previewDocFields))
	for _, f := range previewDocFields {
		onPass[f] = "$$REMOVE"
	}
	return guardedFields(asOf, onPass)
}
