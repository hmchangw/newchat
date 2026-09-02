// Package preview builds and seals the room-list preview, so every writer stores the same
// shape. Metadata stays clear for the server-side guard; the body is sealed under a DEK.
package preview

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// MaxAttachments and MaxMentions cap the collections the room list renders as a count or
// a couple of chips. Content is deliberately NOT capped: the room list gets the full body
// (bounded upstream by the gatekeeper's message size limit). Attachments and mentions ride
// uncapped from the message, so a single wide message could size the coalescer's buffered
// entry, the stored document and every reader's materialization of it at once. One cap at
// compose time bounds both (#290).
const (
	MaxAttachments = 10
	MaxMentions    = 20
)

// Build normalizes a composed preview: capped collections, UTC timestamp. Content passes
// through untouched. Takes PreviewMessage itself so a new field cannot silently default on
// every write path.
//
//nolint:gocritic // hugeParam: PreviewMessage is the stored/wire shape itself; by-value keeps callers simple and the copy cost is negligible.
func Build(p model.PreviewMessage) model.PreviewMessage {
	// Copy, don't reslice. Reslicing keeps the ORIGINAL backing array reachable, tail
	// included, and this value is retained by Sealed.Meta and by the read cache — so the
	// cap would bound the length while pinning exactly the memory it exists to bound.
	if len(p.Attachments) > MaxAttachments {
		p.Attachments = append([]cassandra.Attachment(nil), p.Attachments[:MaxAttachments]...)
	}
	if len(p.Mentions) > MaxMentions {
		p.Mentions = append([]model.Participant(nil), p.Mentions[:MaxMentions]...)
	}
	p.CreatedAt = p.CreatedAt.UTC()
	return p
}

// Eligible reports whether a message may represent its room — system and deleted ones
// may not. Shared: a rule on one writer only would strand a preview the walk never picks.
// VisibleTo is deliberately NOT a gate: a message with a visibility marker is still
// previewed and carries the marker, leaving the client to honour the scope.
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
//
// The second conjunct requires the key to ALREADY match lastMsgId — i.e. the stored body
// is current — before advancing it. Every expression in a $set stage reads the pre-update
// document, so this compares the previous key against the previous last message, not the
// ones this same write installs.
//
// Without it the advance can REVALIDATE a stale body. lastMsgAt/lastMsgId are unguarded,
// so an eligible insert whose body write loses the watermark (a mutation stamped a later
// previewAsOf) still moves lastMsgId: the key and lastMsgId diverge, and that divergence
// is exactly what withholds the now-stale preview. A later ineligible insert clearing the
// watermark would otherwise move the key back into agreement and serve the old body as
// current — the #224 shape reached from a third direction.
func GuardedAdvanceKeyFields(forMsgID string, asOf int64) bson.M {
	cond := bson.M{"$and": bson.A{
		watermarkGuard(asOf),
		bson.M{"$eq": bson.A{
			bson.M{"$ifNull": bson.A{"$previewForMsgId", ""}},
			bson.M{"$ifNull": bson.A{"$lastMsgId", ""}},
		}},
	}}
	return guardedFields(asOf, []string{"previewForMsgId"}, cond, map[string]any{
		"previewForMsgId": bson.M{"$literal": forMsgID},
	})
}

// GuardedUpdateBodyFields replaces an EXISTING body, leaving the key alone — a mutation
// never moves lastMsgId. forMsgID is the key the caller's walk OBSERVED, and the write
// lands only while the stored key still equals it. That equality does double duty: it
// refuses to create (a missing key cannot equal a non-empty id, and only an insert may
// mint one), and it closes the window where an insert advances the key between the walk
// and this write — without it the older body would be stored under the newer key and the
// reader's identity check would pass on the mismatch, exactly the #224 shape.
//
//nolint:gocritic // hugeParam: Sealed is the stored shape itself; by-value matches Seal/Open.
func GuardedUpdateBodyFields(s Sealed, forMsgID string, asOf int64) bson.M {
	cond := bson.M{"$and": bson.A{
		watermarkGuard(asOf),
		bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$previewForMsgId", ""}}, forMsgID}},
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

// GuardedInvalidateKeyFields withdraws a stored preview's certification when a mutation
// could not replace the body it describes — the repair for #226. The reader serves a
// preview on previewForMsgId == lastMsgId, and a mutation never moves lastMsgId, so a
// body whose message was edited or deleted goes on reading as current forever unless
// something withdraws the key. Removing it makes the next read miss, walk, and warm back.
//
// The predicate is the STORED BODY's own message id, not the freshness key: an ineligible
// insert advances the key over an untouched body, so the key does not identify what the
// body describes. Keying on the body also makes this a no-op precisely when it should be
// — once any newer write has replaced the body, previewMeta.messageId no longer names the
// mutated message.
//
// previewAsOf is STAMPED with asOf, not removed. Removing it looked right -- a
// future-stamped watermark would reject the warm-back this invalidation exists to invite --
// but it also drops the fence against OLDER writes. An insert flush delayed past this
// mutation carries an older asOf, and against a missing watermark ($ifNull => 0) it passes,
// runs GuardedSetFields, and restores the ciphertext for the very message that was just
// edited or deleted, under a key that then equals lastMsgId and so reads as current.
//
// Stamping the invalidation time does both jobs: it rejects anything older, still admits
// the warm-back (which runs later), and lowers a future-skewed watermark, which is what
// removing it was reaching for. The repair itself stays unguarded by the watermark: a
// watermark comparison is one of the ways the write it follows failed to land.
func GuardedInvalidateKeyFields(msgID string, asOf int64) bson.M {
	cond := bson.M{"$eq": bson.A{
		bson.M{"$ifNull": bson.A{"$previewMeta.messageId", ""}},
		msgID,
	}}
	return bson.M{
		"previewForMsgId": bson.M{"$cond": bson.A{cond, "$$REMOVE", "$previewForMsgId"}},
		"previewAsOf":     bson.M{"$cond": bson.A{cond, asOf, "$previewAsOf"}},
	}
}
