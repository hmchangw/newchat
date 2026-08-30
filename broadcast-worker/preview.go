package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/preview"
)

// eligibleAsPreview reports whether an inserted message may become the room's preview.
// deleted is false: a message on the created subject has never been deleted.
// VisibleTo is not consulted — a message with a visibility marker is still previewed and
// carries it, leaving the client to honour the scope.
func eligibleAsPreview(msg *model.Message) bool {
	return preview.Eligible(false, msg.Type)
}

// previewSealTimeout bounds the seal's two external calls — the bot app-name read and
// the cipher — so a stalled dependency cannot spend the budget the fan-out still needs.
// Matches parentFetchTimeout, the other optional enrichment on this path.
const previewSealTimeout = 2 * time.Second

// previewSealReserve is what the seal must leave behind for the fan-out it precedes.
// Bounding the seal is not a reserve on its own: context.WithTimeout inherits the
// parent's earlier deadline, so on a short budget a wedged dependency still spends all
// of it. Mirrors history-service's warmBackReserve on the other optional-write path.
const previewSealReserve = 250 * time.Millisecond

// previewSealer seals the room-doc preview; the only read is a bot sender's app name.
type previewSealer struct {
	cipher  atrest.Cipher // nil when ATREST_ENABLED=false — previews are then not stored
	key     preview.Key
	appName preview.AppNameLookup
	// timeout bounds one seal; newPreviewSealer sets previewSealTimeout.
	timeout time.Duration
}

func newPreviewSealer(cipher atrest.Cipher, key preview.Key, appName preview.AppNameLookup) *previewSealer {
	return &previewSealer{cipher: cipher, key: key, appName: appName, timeout: previewSealTimeout}
}

// enabled: no cipher, no preview — the room doc must never hold a plaintext body.
func (p *previewSealer) enabled() bool { return p != nil && p.cipher != nil }

// sealInserted seals the preview for a newly inserted message, reusing the enrichment the
// handler already resolved. The flush stamps ForMsgID: a later message may arrive first.
func (p *previewSealer) sealInserted(
	ctx context.Context,
	msg *model.Message,
	users map[string]model.User,
	mentions []model.Participant,
) (*preview.Sealed, error) {
	decoded, skipped := cassandra.DecodeAttachments(msg.Attachments)
	if skipped > 0 {
		// The event carries what the gatekeeper encoded, so a skip means malformed upstream.
		return nil, fmt.Errorf("decode %d preview attachment(s) for message %s", skipped, msg.ID)
	}

	pvw := preview.Build(model.PreviewMessage{
		MessageID:   msg.ID,
		Sender:      p.sender(ctx, msg, users),
		Content:     msg.Content,
		CreatedAt:   msg.CreatedAt,
		Attachments: decoded,
		Mentions:    mentions,
		VisibleTo:   msg.VisibleTo,
	})
	sealed, err := preview.Seal(ctx, p.cipher, p.key, msg.ID, pvw)
	if err != nil {
		return nil, fmt.Errorf("seal room preview: %w", err)
	}
	return &sealed, nil
}

// sender composes the preview's sender to match what history-service's walk produces.
func (p *previewSealer) sender(ctx context.Context, msg *model.Message, users map[string]model.User) model.Participant {
	s := model.Participant{UserID: msg.UserID, Account: msg.UserAccount}
	if u, ok := users[msg.UserAccount]; ok {
		s.ChineseName = u.ChineseName
		s.EngName = u.EngName
	}
	s.DisplayName = preview.BotAwareDisplayName(ctx, p.appName, s.EngName, s.ChineseName, s.Account)
	return s
}

// previewForInserted returns (sealed, failed) — see roomLastMessage.Preview/PreviewFailed.
// A seal failure degrades to a blank row rather than redelivering the whole fan-out.
func (h *Handler) previewForInserted(
	ctx context.Context,
	msg *model.Message,
	users map[string]model.User,
	mentions []model.Participant,
) (*preview.Sealed, bool) {
	if !h.sealer.enabled() || !eligibleAsPreview(msg) {
		return nil, false
	}
	// The preview is optional, the fan-out below it is not. A budget that cannot cover the
	// seal AND leave the reserve is not worth starting: the bound below would inherit the
	// caller's earlier deadline, and a wedged dependency would spend the fan-out's time.
	//
	// Skipping reports a seal FAILURE, not an ineligible message. The message is eligible,
	// so letting the freshness key advance over the previous body would certify it for a
	// message it does not describe; clearing sends the room to the lazy walk instead.
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < h.sealer.timeout+previewSealReserve {
		slog.WarnContext(ctx, "room preview seal skipped; too little budget left for the fan-out",
			"room_id", msg.RoomID, "messageID", msg.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil, true
	}
	// Bound the seal on its own clock so a wedged cipher or app-name read cannot spend
	// more of the caller's budget than the reserve check above allowed for.
	sealCtx, cancel := context.WithTimeout(ctx, h.sealer.timeout)
	defer cancel()
	sealed, err := h.sealer.sealInserted(sealCtx, msg, users, mentions)
	if err != nil {
		slog.WarnContext(ctx, "seal room preview failed; room list will show no preview until the next message",
			"error", err, "room_id", msg.RoomID, "messageID", msg.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil, true
	}
	return sealed, false
}

// appNameRepo resolves a bot's app display name, the one field not carried by the message.
type appNameRepo struct {
	apps *mongoutil.Collection[struct {
		Name string `bson:"name"`
	}]
}

func newAppNameRepo(coll *mongo.Collection) preview.AppNameLookup {
	r := &appNameRepo{apps: mongoutil.NewCollection[struct {
		Name string `bson:"name"`
	}](coll)}
	return r.lookup
}

// lookup returns ("", nil) on no match — BotAwareDisplayName then uses the composed name.
func (r *appNameRepo) lookup(ctx context.Context, botAccount string) (string, error) {
	app, err := r.apps.FindOne(ctx, bson.M{"assistant.name": botAccount},
		mongoutil.WithProjection(bson.M{"name": 1, "_id": 0}))
	if err != nil {
		return "", fmt.Errorf("app name for %s: %w", botAccount, err)
	}
	if app == nil {
		return "", nil
	}
	return app.Name, nil
}

// guardedAppNameLookup fences the app-name read behind the service's Mongo breaker.
//
// It is the odd one out among this service's Mongo reads: the others gate delivery and
// are fenced in mongoStore, while this one only decorates a preview. That makes it the
// easiest to miss and the cheapest to lose — a bot's app name falling back to its
// composed name for the duration of an outage is invisible to correctness, whereas a 2s
// stall per cold bot account on the fan-out path is not.
//
// It shares the one breaker for the same reason every other call site does: the breaker
// tracks whether Mongo is reachable, and this read is evidence about that like any other.
// A nil breaker passes through, which is what the tests and a breaker-less config get.
func guardedAppNameLookup(inner preview.AppNameLookup, b *circuitbreaker.Breaker) preview.AppNameLookup {
	if inner == nil {
		return nil
	}
	return func(ctx context.Context, botAccount string) (string, error) {
		return circuitbreaker.Do1(b, func() (string, error) { return inner(ctx, botAccount) })
	}
}
