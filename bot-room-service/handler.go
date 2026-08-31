package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/outbox"
	"github.com/hmchangw/chat/pkg/roomkeymetrics"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/timeutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Rocket.Chat-legacy single-char rooms.t values.
const (
	roomTypeChannel = "c"
	roomTypeDM      = "d"
)

type (
	BotIdentity            = model.BotIdentity
	BotCreateRoomRequest   = model.BotCreateRoomRequest
	BotCreateRoomResponse  = model.BotCreateRoomResponse
	OwnerResp              = model.BotOwnerResp
	BotMembersBatchRequest = model.BotMembersBatchRequest
	BotAddResponse         = model.BotAddResponse
	BotRemoveResponse      = model.BotRemoveResponse
	AddedRemoved           = model.BotAddedRemoved
	BotRoomGetRequest      = model.BotRoomGetRequest
	BotRoomGetResponse     = model.BotRoomGetResponse
	BotDMEnsureRequest     = model.BotDMEnsureRequest
	BotDMEnsureResponse    = model.BotDMEnsureResponse
)

// outboxPublisher is a raw NATS publish that stamps msgID as the Nats-Msg-Id header.
type outboxPublisher func(ctx context.Context, subj string, data []byte, msgID string) error

// sysmsgPublisher emits LOCAL-ONLY system messages onto BOT-MESSAGES-CANONICAL.
// nil disables sysmsg emission; membership state stays correct without the narrative message.
type sysmsgPublisher interface {
	PublishWithMsgID(ctx context.Context, subj string, data []byte, msgID string) error
}

// handler wires the room/member endpoints. Cross-site membership federates through OUTBOX; sysmsgs stay local.
type handler struct {
	store      RoomStore
	siteID     string
	allSiteIDs []string
	publishFn  outboxPublisher
	sysmsgPub  sysmsgPublisher
	keyStore   RoomKeyStore
	keySender  *roomkeysender.Sender
	now        func() time.Time
	newMsgID   func() string
	newUUIDv7  func() string
	// valkey is the L2 (Valkey) client used only to invalidate subauthcache
	// entries after a member removal. nil disables invalidation (best-effort).
	// Set post-construction, mirroring room-worker/room-service/inbox-worker.
	valkey valkeyutil.Client
}

func newHandler(store RoomStore, siteID string, allSiteIDs []string, pub outboxPublisher,
	keyStore RoomKeyStore, keySender *roomkeysender.Sender,
) *handler {
	return &handler{
		store: store, siteID: siteID, allSiteIDs: allSiteIDs, publishFn: pub,
		keyStore: keyStore, keySender: keySender,
		now:       func() time.Time { return time.Now().UTC() },
		newMsgID:  idgen.GenerateMessageID,
		newUUIDv7: idgen.GenerateUUIDv7,
	}
}

func (h *handler) Register(r *natsrouter.Router) {
	natsrouter.Register[BotCreateRoomRequest, BotCreateRoomResponse](r,
		subject.ServerBotRoomCreate(h.siteID), h.handleCreate)
	natsrouter.Register[BotMembersBatchRequest, BotAddResponse](r,
		subject.ServerBotRoomMemberAddPattern(h.siteID), h.handleAdd)
	natsrouter.Register[BotMembersBatchRequest, BotRemoveResponse](r,
		subject.ServerBotRoomMemberRemovePattern(h.siteID), h.handleRemove)
	natsrouter.Register[BotRoomGetRequest, BotRoomGetResponse](r,
		subject.ServerBotRoomGet(h.siteID), h.handleGet)
	natsrouter.Register[BotDMEnsureRequest, BotDMEnsureResponse](r,
		subject.ServerBotRoomDMEnsure(h.siteID), h.handleDMEnsure)
}

// handleDMEnsure materializes a DM room + subscriptions; this site becomes the DM's origin.
// For a remote target, only member_added federates — the target upserts a subscription, no rooms doc.
func (h *handler) handleDMEnsure(c *natsrouter.Context, req BotDMEnsureRequest) (*BotDMEnsureResponse, error) { //nolint:gocritic // hugeParam: natsrouter contract
	ident, err := parseIdentity(c.Msg.Header)
	if err != nil {
		return nil, err
	}
	if req.TargetUserID == "" {
		return nil, errcode.BadRequest("targetUserId is required",
			errcode.WithReason(errcode.BotContentInvalid))
	}
	if req.TargetUserID == ident.ID {
		return nil, errcode.BadRequest("cannot DM self",
			errcode.WithReason(errcode.BotCannotDMSelf))
	}

	target, err := h.store.FindUser(c, req.TargetUserID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errcode.NotFound("target user not found",
				errcode.WithReason(errcode.BotDMTargetNotFound))
		}
		return nil, fmt.Errorf("find target user: %w", err)
	}

	roomID := idgen.BuildDMRoomID(ident.ID, target.ID)
	createdAt := h.now()

	// Duplicate insert is benign (same-side retry, or race between two ensures).
	room := &Room{
		ID: roomID, Type: roomTypeDM, SiteID: h.siteID, CreatedAt: createdAt,
		Owner: &Participant{
			UserID: ident.ID, Account: ident.Account, SiteID: ident.SiteID,
			IsBot: true,
		},
		CreatedByBot: ident.ID,
	}
	if err := h.store.InsertRoom(c, room); err != nil {
		if !errors.Is(err, ErrDuplicate) {
			return nil, fmt.Errorf("insert dm room: %w", err)
		}
		// The DM id is deterministic, so a duplicate means this room already
		// exists and may carry history — take its position rather than federating
		// nil and leaving the target unable to order it.
		if existing, findErr := h.store.FindRoom(c, roomID); findErr == nil {
			room.LastMsgAt = existing.LastMsgAt
		} else {
			slog.WarnContext(c, "read existing dm room failed; federating without an activity position",
				"room_id", roomID, "error", findErr)
		}
	}
	if _, err := h.store.UpsertSubscription(c, &Subscription{
		ID: h.newUUIDv7(), RoomID: roomID, UserID: ident.ID, Account: ident.Account,
		SiteID: h.siteID, CreatedAt: createdAt, IsBot: true,
		Name: target.Account, RoomType: model.SubscriptionRoomType(target.Account),
	}); err != nil {
		return nil, fmt.Errorf("upsert bot dm subscription: %w", err)
	}

	// Same-site target (or unset SiteID, treated as local): upsert the subscription directly. Remote: federate via OUTBOX so the target-site inbox-worker upserts it.
	if target.SiteID == "" || target.SiteID == h.siteID {
		if _, err := h.store.UpsertSubscription(c, &Subscription{
			ID: h.newUUIDv7(), RoomID: roomID, UserID: target.ID, Account: target.Account,
			SiteID: h.siteID, CreatedAt: createdAt,
			Name: ident.Account, RoomType: model.SubscriptionRoomType(ident.Account),
		}); err != nil {
			return nil, fmt.Errorf("upsert target dm subscription: %w", err)
		}
	} else {
		// DM member_added carries roomType=botDM + RequesterAccount=bot so the target names the subscription after the counterparty; subscription-only, no rooms doc.
		if err := h.federateMemberAdded(c, roomID, target.ID, target.Account, target.SiteID, createdAt,
			model.RoomTypeBotDM, "", ident.Account, room.LastMsgAt); err != nil {
			return nil, err
		}
	}

	return &BotDMEnsureResponse{RoomID: roomID, CreatedAt: createdAt}, nil
}

// ----- create-room ---------------------------------------------------------

func (h *handler) handleCreate(c *natsrouter.Context, req BotCreateRoomRequest) (*BotCreateRoomResponse, error) { //nolint:gocritic // hugeParam: natsrouter contract
	ident, err := parseIdentity(c.Msg.Header)
	if err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, errcode.BadRequest("name is required", errcode.WithReason(errcode.BotContentInvalid))
	}
	if len(req.Orgs) > 0 {
		return nil, errcode.BadRequest("org expansion not yet supported",
			errcode.WithReason(errcode.BotUnsupported))
	}

	roomID := idgen.GenerateID()
	createdAt := h.now()

	owner := &Participant{
		UserID: ident.ID, Account: ident.Account, SiteID: ident.SiteID,
		EngName: ident.EngName, ChineseName: ident.ChineseName,
		AppID: ident.AppID, AppName: ident.AppName, IsBot: true,
	}
	room := &Room{
		ID: roomID, Type: roomTypeChannel, Name: req.Name, Topic: req.Topic,
		SiteID: h.siteID, CreatedAt: createdAt, Owner: owner, CreatedByBot: ident.ID,
	}
	if err := h.store.InsertRoom(c, room); err != nil {
		if errors.Is(err, ErrDuplicate) {
			return nil, errcode.Conflict("room already exists", errcode.WithReason(errcode.BotRoomExists))
		}
		return nil, fmt.Errorf("insert room: %w", err)
	}

	if _, err := h.store.UpsertSubscription(c, &Subscription{
		ID: h.newUUIDv7(), RoomID: roomID, UserID: ident.ID, Account: ident.Account,
		SiteID: h.siteID, CreatedAt: createdAt, IsBot: true,
		Name: room.Name, RoomType: model.RoomTypeChannel,
	}); err != nil {
		return nil, fmt.Errorf("upsert owner subscription: %w", err)
	}

	// Channel rooms are always encrypted (mirrors room-service/room-worker): generate+store
	// the room key, then fan out to the owner; a publish failure is logged only — the key is already stored.
	pair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("generate room key: %w", err)
	}
	ver, err := h.keyStore.Set(c, roomID, *pair)
	if err != nil {
		return nil, fmt.Errorf("store room key: %w", err)
	}
	h.fanOutKey(c, roomID, []string{owner.Account}, model.RoomKeyEvent{
		RoomID:     roomID,
		Version:    ver,
		PrivateKey: pair.PrivateKey,
	}, "fan out room key on create failed")

	// Seed members: idempotent upsert; per-destination member_added via outbox.
	addedIDs := []string{ident.ID}
	for _, memberID := range req.Members {
		if memberID == ident.ID {
			continue
		}
		u, err := h.store.FindUser(c, memberID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, errcode.NotFound(
					fmt.Sprintf("member %s not found", memberID),
					errcode.WithReason(errcode.BotMemberNotFound))
			}
			return nil, fmt.Errorf("find member: %w", err)
		}
		if _, err := h.store.UpsertSubscription(c, &Subscription{
			ID: h.newUUIDv7(), RoomID: roomID, UserID: u.ID, Account: u.Account,
			SiteID: u.SiteID, CreatedAt: createdAt,
			Name: room.Name, RoomType: model.RoomTypeChannel,
		}); err != nil {
			return nil, fmt.Errorf("upsert member subscription: %w", err)
		}
		addedIDs = append(addedIDs, u.ID)

		// Channel-shape event; RoomName names the subscription at the target.
		if u.SiteID != "" && u.SiteID != h.siteID {
			if err := h.federateMemberAdded(c, roomID, u.ID, u.Account, u.SiteID, createdAt,
				model.RoomTypeChannel, req.Name, ident.Account, nil); err != nil {
				return nil, err
			}
		}
	}

	// LOCAL sysmsg covering the seed roster; remote members learn via the OUTBOX event.
	h.emitSysmsg(c, roomID, ident, model.MessageTypeMembersAdded,
		model.MembersAdded{
			Individuals:     addedIDs,
			Orgs:            req.Orgs,
			AddedUsersCount: len(addedIDs),
		},
		fmt.Sprintf("create:%d", createdAt.UnixMilli()))

	return &BotCreateRoomResponse{
		ID: roomID, Name: req.Name,
		Owner:   OwnerResp{ID: ident.ID, IsBot: true, AppID: ident.AppID, AppName: ident.AppName},
		Members: addedIDs, CreatedAt: createdAt,
	}, nil
}

// ----- add-members ---------------------------------------------------------

func (h *handler) handleAdd(c *natsrouter.Context, req BotMembersBatchRequest) (*BotAddResponse, error) { //nolint:gocritic // hugeParam: natsrouter contract
	roomID := c.Params.Get("roomID")
	if roomID == "" {
		return nil, errcode.BadRequest("roomID missing from subject")
	}
	if len(req.OrgIDs) > 0 {
		return nil, errcode.BadRequest("org expansion not yet supported",
			errcode.WithReason(errcode.BotUnsupported))
	}
	ident, err := parseIdentity(c.Msg.Header)
	if err != nil {
		return nil, err
	}
	room, err := h.loadRoomAndAssertOwner(c, roomID, ident)
	if err != nil {
		return nil, err
	}
	// A DM is exactly two participants; mirrors room-service's channel-only guard.
	if room.Type != roomTypeChannel {
		return nil, errcode.BadRequest("cannot add members to a non-channel room",
			errcode.WithReason(errcode.RoomNonChannelOperation))
	}

	created := h.now()
	added := []string{}
	newAccounts := []string{}
	for _, userID := range req.UserIDs {
		u, err := h.store.FindUser(c, userID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, errcode.NotFound(fmt.Sprintf("member %s not found", userID),
					errcode.WithReason(errcode.BotMemberNotFound))
			}
			return nil, fmt.Errorf("find member: %w", err)
		}
		newlyAdded, err := h.store.UpsertSubscription(c, &Subscription{
			ID: h.newUUIDv7(), RoomID: roomID, UserID: u.ID, Account: u.Account,
			SiteID: u.SiteID, CreatedAt: created,
			Name: room.Name, RoomType: model.RoomTypeChannel,
		})
		if err != nil {
			return nil, fmt.Errorf("upsert subscription: %w", err)
		}
		if !newlyAdded {
			// Duplicate add is a no-op.
			continue
		}
		added = append(added, u.ID)
		newAccounts = append(newAccounts, u.Account)

		if u.SiteID != "" && u.SiteID != h.siteID {
			roomType := roomTypeToModel(room.Type)
			if err := h.federateMemberAdded(c, roomID, u.ID, u.Account, u.SiteID, created,
				roomType, room.Name, ident.Account, room.LastMsgAt); err != nil {
				return nil, err
			}
		}
	}

	// Fan out the room's current key only to newly-subscribed accounts — duplicate adds already
	// have it from their original add; the key isn't re-rotated for adds (mirrors room-worker.buildAndFanOutRoomKey).
	if len(newAccounts) > 0 {
		// Self-heal a key-absent room instead of silently skipping the fan-out
		// (which left the new member unable to decrypt with no retry). Mirrors the
		// create path's mint; going-forward-only — see keyPairOrHeal.
		pair, err := h.keyPairOrHeal(c, roomID)
		if err != nil {
			return nil, fmt.Errorf("get room key: %w", err)
		}
		h.fanOutKey(c, roomID, newAccounts, model.RoomKeyEvent{
			RoomID:     roomID,
			Version:    pair.Version,
			PrivateKey: pair.KeyPair.PrivateKey,
		}, "fan out room key on add failed")
	}

	// Skip sysmsg on all-dup batches (true no-op: no message, no OUTBOX event).
	if len(added) > 0 {
		h.emitSysmsg(c, roomID, ident, model.MessageTypeMembersAdded,
			model.MembersAdded{Individuals: added, AddedUsersCount: len(added)},
			fmt.Sprintf("add:%d", created.UnixMilli()))
	}
	return &BotAddResponse{Added: AddedRemoved{UserIDs: added, OrgIDs: nil}}, nil
}

// roomTypeToModel surfaces "d" as RoomTypeBotDM so the target picks the botDM naming branch.
func roomTypeToModel(t string) model.RoomType {
	switch t {
	case roomTypeDM:
		return model.RoomTypeBotDM
	default:
		return model.RoomTypeChannel
	}
}

// ----- remove-members ------------------------------------------------------

// memberRemoval is one removal awaiting its cross-site publish, held between the
// delete loop and the federation pass so the room-key rotation can run between them.
type memberRemoval struct {
	userID     string
	account    string
	destSiteID string
}

func (h *handler) handleRemove(c *natsrouter.Context, req BotMembersBatchRequest) (*BotRemoveResponse, error) { //nolint:gocritic // hugeParam: natsrouter contract
	roomID := c.Params.Get("roomID")
	if roomID == "" {
		return nil, errcode.BadRequest("roomID missing from subject")
	}
	if len(req.OrgIDs) > 0 {
		return nil, errcode.BadRequest("org expansion not yet supported",
			errcode.WithReason(errcode.BotUnsupported))
	}
	ident, err := parseIdentity(c.Msg.Header)
	if err != nil {
		return nil, err
	}
	if _, err := h.loadRoomAndAssertOwner(c, roomID, ident); err != nil {
		return nil, err
	}

	// Pre-validate the whole batch so a mid-loop self-ID doesn't leave earlier removals committed before the request fails.
	for _, userID := range req.UserIDs {
		if userID == ident.ID {
			return nil, errcode.Forbidden("bot cannot remove itself",
				errcode.WithReason(errcode.BotCannotRemoveSelf))
		}
	}

	removed := []string{}
	removedAccounts := []string{}
	// Collected in the loop and published after the rotation below, so a failed
	// publish cannot skip it. See the rotation comment for why that ordering is
	// load-bearing rather than cosmetic.
	// Every user in the batch can queue one now that federation no longer waits on
	// the delete, so the exact bound is known here.
	pendingFederations := make([]memberRemoval, 0, len(req.UserIDs))
	// Deferred, not called at the end of the loop: every path out of here from
	// this point on has already committed subscription deletes, so the cached
	// positive subauthcache decisions must die on the error paths too. A
	// federation failure that returned before the bust would leave every account
	// in the batch still passing authorization for the rest of the L2 TTL. One
	// round trip for the whole set — the {roomID} hash tag keeps the keys in one
	// slot — and BustSubs no-ops on an empty set.
	defer func() { subauthcache.BustSubs(c, h.valkey, roomID, removedAccounts) }()
	// Deferred for the same reason as the bust above, and it has to be: a
	// mid-batch error on a LATER user returns before the explicit rotation
	// below, with earlier deletes already committed. The retry cannot repair
	// that — those deletes report wasThere=false next time, so `removed` can
	// come back empty and the rotation is then skipped for good, leaving an
	// already-removed member holding a key that opens every future message.
	// Best effort and logged rather than returned: this only runs on a path
	// that is already failing, and the caller's error must not be replaced.
	rotated := false
	defer func() {
		if rotated || len(removed) == 0 {
			return
		}
		if err := h.rotateAndFanOut(c, roomID); err != nil {
			slog.ErrorContext(c, "room key rotation after a failed bot removal did not complete; removed members may still hold a working key",
				"error", err, "roomID", roomID, "removed", len(removed))
		}
	}()
	for _, userID := range req.UserIDs {
		// The account comes from the delete itself, so the bust below cannot be
		// skipped by the enrichment lookup failing. It is the same value
		// UpsertSubscription wrote into subscriptions.u.account at add-time,
		// which is what subauthcache.SubKey is keyed on.
		// The destination is resolved BEFORE the delete, because the user doc is
		// the only place it lives: the subscription's own siteId is the ROOM's
		// site, and its u sub-document carries just _id/account/isBot. Resolving
		// first means a lookup failure aborts before anything is committed, so a
		// retry re-runs the whole removal; resolving afterwards would delete the
		// row and only then discover it cannot address the remote site.
		u, err := h.store.FindUser(c, userID)
		switch {
		case errors.Is(err, ErrNotFound):
			// The user doc is genuinely gone, not unreachable: there is no remote
			// site to notify, so the local removal proceeds with no federation.
			u = nil
		case err != nil:
			return nil, fmt.Errorf("resolve removal destination for user %s: %w", userID, err)
		}

		account, wasThere, err := h.store.DeleteSubscription(c, roomID, userID)
		if err != nil {
			return nil, fmt.Errorf("delete subscription: %w", err)
		}

		// Queued whether or not THIS call did the deleting. A first call that
		// committed the delete and then failed on its publish leaves the member
		// still subscribed on their home site, and every retry reports
		// wasThere=false — so gating the federation here means nothing ever
		// repairs it. Republishing is safe: the dedup id is derived only from
		// (roomID, userID, destSiteID), so JetStream drops it where the first
		// publish landed and accepts it where it did not.
		if u != nil && u.SiteID != "" && u.SiteID != h.siteID {
			pendingFederations = append(pendingFederations, memberRemoval{userID: u.ID, account: u.Account, destSiteID: u.SiteID})
		}

		if !wasThere {
			// Duplicate remove: nothing was de-authorized here, so nothing local
			// follows — no bust, no rotation, no second system message. Only the
			// federation above, which is idempotent at the destination.
			continue
		}
		removed = append(removed, userID)
		if account != "" {
			removedAccounts = append(removedAccounts, account)
		}
	}
	// Only rotate when at least one subscription was actually deleted — a no-op remove must not rotate the room key (matches user pipeline).
	//
	// Rotation runs BEFORE the federation publishes, for the same reason the
	// subauthcache bust above is deferred: by here the deletes are committed, and
	// the retry cannot repair anything. It re-runs with wasThere=false on every
	// user, so `removed` is empty, so it skips the rotation too — permanently.
	// Federating first and returning on its failure therefore leaves the removed
	// member holding a key that still opens every future message in the room,
	// with nothing that ever rotates it. Rotating first downgrades that to the
	// stranded-federation gap this handler already documents: the remote site
	// still shows them subscribed, but they cannot read anything new.
	if len(removed) > 0 {
		// Set only AFTER the call succeeds. Returning the error here does not
		// hand the problem to the caller's retry — that retry finds every
		// committed delete reporting wasThere=false, so `removed` comes back
		// empty and the rotation is skipped for good. This request is the last
		// moment anything still knows a key must be rotated, so a failure must
		// fall through to the deferred net for one more attempt rather than
		// suppress it. A second rotation on an already-rotated key costs a spare
		// version, which retired_room_keys expires; not rotating at all leaves a
		// removed member reading every future message.
		if err := h.rotateAndFanOut(c, roomID); err != nil {
			return nil, err
		}
		rotated = true
	}
	for _, f := range pendingFederations {
		if err := h.federateMemberRemoved(c, roomID, f.userID, f.account, f.destSiteID); err != nil {
			return nil, err
		}
	}

	// Batch remove uses RemovedUsersCount; the individual User field stays nil.
	if len(removed) > 0 {
		h.emitSysmsg(c, roomID, ident, model.MessageTypeMemberRemoved,
			model.MemberRemoved{RemovedUsersCount: len(removed)},
			fmt.Sprintf("remove:%d", h.now().UnixMilli()))
	}
	return &BotRemoveResponse{Removed: AddedRemoved{UserIDs: removed, OrgIDs: nil}}, nil
}

// fanOutKey marshals evt once and best-effort delivers to every account; a per-recipient
// publish failure is logged and doesn't abort — recoverable via room-service.getRoomKey on next decrypt-miss.
func (h *handler) fanOutKey(ctx context.Context, roomID string, accounts []string, evt model.RoomKeyEvent, warnMsg string) {
	if len(accounts) == 0 {
		return
	}
	data, err := h.keySender.Marshal(evt)
	if err != nil {
		slog.WarnContext(ctx, "marshal room key event failed", "error", err, "roomID", roomID)
		return
	}
	for _, acct := range accounts {
		if err := h.keySender.SendData(acct, data); err != nil {
			slog.WarnContext(ctx, warnMsg, "account", acct, "roomID", roomID, "error", err)
		}
	}
}

// keyPairOrHeal returns the room's current key, minting and persisting a fresh
// key when the room has none (legacy / never-keyed). Going-forward-only: it
// unblocks new members but does NOT recover history a lost key had encrypted.
// RecordKeyAbsent fires on the mint so ops can spot a lost-key event. SetIfAbsent
// converges concurrent minters on a single v0 key. Mirrors the create path's mint.
func (h *handler) keyPairOrHeal(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
	pair, err := h.keyStore.Get(ctx, roomID)
	if err != nil && !errors.Is(err, roomkeystore.ErrNoCurrentKey) {
		return nil, fmt.Errorf("get room key: %w", err)
	}
	if pair != nil {
		return pair, nil
	}
	roomkeymetrics.RecordKeyAbsent(ctx, "")
	fresh, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("generate room key: %w", err)
	}
	committed, err := h.keyStore.SetIfAbsent(ctx, roomID, *fresh)
	if err != nil {
		return nil, fmt.Errorf("store room key: %w", err)
	}
	return committed, nil
}

// rotateAndFanOut commits before fan-out; a predicted current+1 mislabels keys when removals race.
func (h *handler) rotateAndFanOut(ctx context.Context, roomID string) error {
	survivors, err := h.store.ListRoomMemberAccounts(ctx, roomID)
	if err != nil {
		return fmt.Errorf("list survivors: %w", err)
	}

	currentPair, err := h.keyStore.Get(ctx, roomID)
	if err != nil && !errors.Is(err, roomkeystore.ErrNoCurrentKey) {
		return fmt.Errorf("get current key: %w", err)
	}

	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate new key: %w", err)
	}

	committed, err := roomkeystore.CommitRotation(ctx, h.keyStore, roomID, currentPair, newPair)
	if err != nil {
		return err
	}

	h.fanOutKey(ctx, roomID, survivors,
		model.RoomKeyEvent{RoomID: roomID, Version: committed.Version, PrivateKey: committed.KeyPair.PrivateKey},
		"fan out rotated key failed")
	return nil
}

// ----- room.get ------------------------------------------------------------

func (h *handler) handleGet(c *natsrouter.Context, req BotRoomGetRequest) (*BotRoomGetResponse, error) { //nolint:gocritic // hugeParam: natsrouter contract
	if req.RoomID == "" {
		return nil, errcode.BadRequest("roomId is required",
			errcode.WithReason(errcode.BotContentInvalid))
	}
	room, err := h.store.FindRoom(c, req.RoomID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errcode.NotFound("room not found",
				errcode.WithReason(errcode.BotRoomNotFound))
		}
		return nil, fmt.Errorf("find room: %w", err)
	}
	return &BotRoomGetResponse{
		ID: room.ID, Type: room.Type, Name: room.Name, Topic: room.Topic,
		SiteID: room.SiteID, CreatedAt: room.CreatedAt,
	}, nil
}

// ----- helpers -------------------------------------------------------------

// loadRoomAndAssertOwner requires room.CreatedByBot to match the caller's bot ID.
func (h *handler) loadRoomAndAssertOwner(ctx context.Context, roomID string, ident *BotIdentity) (*Room, error) {
	room, err := h.store.FindRoom(ctx, roomID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errcode.NotFound("room not found", errcode.WithReason(errcode.BotRoomNotFound))
		}
		return nil, fmt.Errorf("find room: %w", err)
	}
	if room.CreatedByBot == "" || room.CreatedByBot != ident.ID {
		return nil, errcode.Forbidden("caller is not the owning bot",
			errcode.WithReason(errcode.BotNotARoomOwner))
	}
	return room, nil
}

// federateMemberAdded relays member_added to a remote site's inbox-worker.
// For DM/botDM the target names the subscription from RequesterAccount; for channels, from RoomName.
// lastMsgAt is nil for a room with no messages yet (both creation paths).
func (h *handler) federateMemberAdded(ctx context.Context, roomID, userID, account, destSiteID string, at time.Time,
	roomType model.RoomType, roomName, requesterAccount string, lastMsgAt *time.Time,
) error {
	payload, err := json.Marshal(model.MemberAddEvent{
		Type:             "member_added",
		RoomID:           roomID,
		RoomType:         roomType,
		RoomName:         roomName,
		RequesterAccount: requesterAccount,
		SiteID:           h.siteID,
		Accounts:         []string{account},
		JoinedAt:         at.UnixMilli(),
		LastMsgAt:        timeutil.TimeToMillis(lastMsgAt),
		Timestamp:        at.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("marshal member_added payload: %w", err)
	}
	dedupID := fmt.Sprintf("bot-add:%s:%s:%s", roomID, userID, destSiteID)
	return outbox.Publish(ctx, h.publishFn, h.siteID, roomID, destSiteID,
		model.InboxMemberAdded, payload, dedupID, at.UnixMilli())
}

func (h *handler) federateMemberRemoved(ctx context.Context, roomID, userID, account, destSiteID string) error {
	atMs := h.now().UnixMilli()
	payload, err := json.Marshal(model.MemberRemoveEvent{
		Type: "member_removed", RoomID: roomID, SiteID: h.siteID,
		Accounts: []string{account}, Timestamp: atMs,
	})
	if err != nil {
		return fmt.Errorf("marshal member_removed payload: %w", err)
	}
	dedupID := fmt.Sprintf("bot-remove:%s:%s:%s", roomID, userID, destSiteID)
	return outbox.Publish(ctx, h.publishFn, h.siteID, roomID, destSiteID,
		model.InboxMemberRemoved, payload, dedupID, atMs)
}

func parseIdentity(h nats.Header) (*BotIdentity, error) {
	raw := h.Get(model.HeaderBotIdentity)
	if raw == "" {
		return nil, errcode.BadRequest("missing X-Bot-Identity header",
			errcode.WithReason(errcode.BotInvalidHeader))
	}
	var ident BotIdentity
	if err := json.Unmarshal([]byte(raw), &ident); err != nil {
		return nil, errcode.BadRequest("malformed X-Bot-Identity header",
			errcode.WithReason(errcode.BotInvalidHeader), errcode.WithCause(err))
	}
	if ident.ID == "" || ident.Account == "" {
		return nil, errcode.BadRequest("X-Bot-Identity missing id or account",
			errcode.WithReason(errcode.BotInvalidHeader))
	}
	return &ident, nil
}
