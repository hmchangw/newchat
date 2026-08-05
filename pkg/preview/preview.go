// Package preview builds the room-list preview message shared by every writer
// and reader path: history-service's read-time walk, broadcast-worker's
// denormalized create/edit/delete writes, and the warm-back. Building here
// keeps the shapes identical regardless of which path produced the preview.
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
	p.Content = TruncateContent(p.Content)
	p.CreatedAt = p.CreatedAt.UTC()
	return p
}

// TruncateContent caps s at MaxContentRunes runes (never splitting a rune).
func TruncateContent(s string) string {
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

// guardedFields builds the watermark-guarded $set shared by the set and clear
// paths: previewMessage becomes onPass when asOf >= the stored previewAsOf
// (missing => 0; ties => last writer wins), and previewAsOf advances with it.
// Both paths route through here so the ordering rule cannot drift between them.
func guardedFields(asOf int64, onPass any) bson.M {
	cond := bson.M{"$gte": bson.A{asOf, bson.M{"$ifNull": bson.A{"$previewAsOf", 0}}}}
	return bson.M{
		"previewMessage": bson.M{"$cond": bson.A{cond, onPass, "$previewMessage"}},
		"previewAsOf":    bson.M{"$cond": bson.A{cond, asOf, "$previewAsOf"}},
	}
}

// GuardedSetFields returns the $set fields for an aggregation-pipeline update
// that applies pvw+asOf under the watermark guard. $literal shields the preview
// doc from aggregation evaluation (content may legitimately start with "$").
func GuardedSetFields(pvw *model.PreviewMessage, asOf int64) bson.M {
	return guardedFields(asOf, bson.M{"$literal": pvw})
}

// GuardedClearFields returns the $set fields for an aggregation-pipeline update
// that REMOVES the stored preview under the watermark guard, while still
// advancing previewAsOf — so a redelivered older create cannot resurrect the
// cleared preview.
func GuardedClearFields(asOf int64) bson.M {
	return guardedFields(asOf, "$$REMOVE")
}
