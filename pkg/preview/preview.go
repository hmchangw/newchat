// Package preview builds and seals the room-list preview, so every writer stores the same
// shape. Metadata stays clear for the server-side guard; the body is sealed under a DEK.
package preview

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

// MaxContentRunes caps PreviewMessage.Content: the room list renders a snippet only.
const MaxContentRunes = 500

// Build normalizes a composed preview: truncated content, UTC timestamp. Takes
// PreviewMessage itself so a new field cannot silently default on every write path.
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
	// string() copies deliberately: slicing would pin the whole 20KB body in cache.
	return string(r[:MaxContentRunes])
}

// Eligible reports whether a message may represent its room — system and deleted ones
// may not. Shared: a rule on one writer only would strand a preview the walk never picks.
func Eligible(deleted bool, msgType string) bool {
	return !deleted && !model.IsSystemMessageType(msgType)
}

// AppNameLookup resolves a bot account's app display name; ("", nil) means no match.
type AppNameLookup func(ctx context.Context, botAccount string) (string, error)

// BotAwareDisplayName prefers a bot's app name, degrading to the composed name on miss.
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

// previewBodyFields move together or not at all: a nonce without its ciphertext is worse.
var previewBodyFields = []string{
	"previewMeta",
	"previewCiphertext",
	"previewNonce",
	"previewKeyEpoch",
}

// previewDocFields is the body plus its freshness key; writes iterate it, so no drift.
var previewDocFields = append(append([]string{}, previewBodyFields...), "previewForMsgId")

// watermarkGuard: apply only when asOf >= stored previewAsOf (missing => 0, ties => last wins).
func watermarkGuard(asOf int64) bson.M {
	return bson.M{"$gte": bson.A{asOf, bson.M{"$ifNull": bson.A{"$previewAsOf", 0}}}}
}

// guardedFields builds a watermark-guarded $set: each field takes onPass when cond holds.
// previewAsOf guards on the watermark alone, else a superseded write stays replayable.
func guardedFields(asOf int64, fields []string, cond bson.M, onPass map[string]any) bson.M {
	out := bson.M{"previewAsOf": bson.M{"$cond": bson.A{watermarkGuard(asOf), asOf, "$previewAsOf"}}}
	for _, f := range fields {
		out[f] = bson.M{"$cond": bson.A{cond, onPass[f], "$" + f}}
	}
	return out
}

// literalBody $literal-wraps the body so a "$"-prefixed value is never a field path.
//
//nolint:gocritic // hugeParam: Sealed is the stored shape itself; by-value matches Seal/Open.
func literalBody(s Sealed) map[string]any {
	return map[string]any{
		"previewMeta":       bson.M{"$literal": s.Meta},
		"previewCiphertext": bson.M{"$literal": s.Ciphertext},
		"previewNonce":      bson.M{"$literal": s.Nonce},
		"previewKeyEpoch":   bson.M{"$literal": s.KeyEpoch},
	}
}

// GuardedSetFields writes body and freshness key together, for the room's newest message.
//
//nolint:gocritic // hugeParam: Sealed is the stored shape itself; by-value matches Seal/Open.
func GuardedSetFields(s Sealed, asOf int64) bson.M {
	onPass := literalBody(s)
	onPass["previewForMsgId"] = bson.M{"$literal": s.ForMsgID}
	return guardedFields(asOf, previewDocFields, watermarkGuard(asOf), onPass)
}

// GuardedAdvanceKeyFields moves the freshness key only, for an ineligible insert: the key
// must follow lastMsgId or a still-correct preview reads as stale.
func GuardedAdvanceKeyFields(forMsgID string, asOf int64) bson.M {
	return guardedFields(asOf, []string{"previewForMsgId"}, watermarkGuard(asOf), map[string]any{
		"previewForMsgId": bson.M{"$literal": forMsgID},
	})
}

// GuardedUpdateBodyFields replaces an EXISTING body, leaving the key alone — a mutation
// never moves lastMsgId. The extra conjunct refuses to create: only an insert may mint one.
//
//nolint:gocritic // hugeParam: Sealed is the stored shape itself; by-value matches Seal/Open.
func GuardedUpdateBodyFields(s Sealed, asOf int64) bson.M {
	cond := bson.M{"$and": bson.A{
		watermarkGuard(asOf),
		bson.M{"$gt": bson.A{bson.M{"$strLenCP": bson.M{"$ifNull": bson.A{"$previewForMsgId", ""}}}, 0}},
	}}
	return guardedFields(asOf, previewBodyFields, cond, literalBody(s))
}

// GuardedClearFields removes every preview field, advancing previewAsOf against replays.
func GuardedClearFields(asOf int64) bson.M {
	onPass := make(map[string]any, len(previewDocFields))
	for _, f := range previewDocFields {
		onPass[f] = "$$REMOVE"
	}
	return guardedFields(asOf, previewDocFields, watermarkGuard(asOf), onPass)
}
