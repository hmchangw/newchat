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
	r := []rune(s)
	if len(r) <= MaxContentRunes {
		// Multi-byte but within the cap — return s rather than re-encoding it.
		return s
	}
	// string() copies, which is the point: slicing s would alias its backing
	// array, pinning a whole 20KB body behind a 500-rune snippet for as long as
	// the preview sits in the cache.
	return string(r[:MaxContentRunes])
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

// previewDocFields is the one list of room-doc keys making up a stored preview.
// Both write paths iterate it so they cannot drift and strand a fragment — a
// nonce and epoch describing a ciphertext that no longer exists. previewAsOf is
// absent by design: it is the watermark, and advances on every landing write.
var previewDocFields = []string{
	"previewMeta",
	"previewCiphertext",
	"previewNonce",
	"previewKeyEpoch",
	"previewForMsgId",
}

// guardedFields builds the watermark-guarded $set shared by the set and clear
// paths: each field becomes onPass[field] when asOf >= the stored previewAsOf
// (missing => 0; ties => last writer wins), and previewAsOf advances with it.
func guardedFields(asOf int64, onPass map[string]any) bson.M {
	cond := bson.M{"$gte": bson.A{asOf, bson.M{"$ifNull": bson.A{"$previewAsOf", 0}}}}
	out := bson.M{"previewAsOf": bson.M{"$cond": bson.A{cond, asOf, "$previewAsOf"}}}
	for _, f := range previewDocFields {
		out[f] = bson.M{"$cond": bson.A{cond, onPass[f], "$" + f}}
	}
	return out
}

// GuardedSetFields returns the $set fields applying s+asOf under the watermark
// guard. $literal keeps a "$"-prefixed value (e.g. a display name) from being
// evaluated as a field path.
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

// GuardedClearFields REMOVES every stored preview field under the same guard,
// still advancing previewAsOf so an older redelivery cannot resurrect it.
func GuardedClearFields(asOf int64) bson.M {
	onPass := make(map[string]any, len(previewDocFields))
	for _, f := range previewDocFields {
		onPass[f] = "$$REMOVE"
	}
	return guardedFields(asOf, onPass)
}
