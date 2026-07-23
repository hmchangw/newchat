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
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
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

// sysmsgPublisher emits LOCAL-ONLY system messages onto BOT_MESSAGES_CANONICAL.
// nil disables sysmsg emission; membership state stays correct without the narrative message.
type sysmsgPublisher interface {
	PublishWithMsgID(ctx context.Context, subj string, data []byte, msgID string) error
}

// handler wires the room/member endpoints.
// Cross-site membership federates through OUTBOX; sysmsgs stay local.
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
	if err := h.store.InsertRoom(c, room); err != nil && !errors.Is(err, ErrDuplicate) {
		return nil, fmt.Errorf("insert dm room: %w", err)
	}
	if _, err := h.store.UpsertSubscription(c, &Subscription{
		ID: h.newUUIDv7(), RoomID: roomID, UserID: ident.ID, Account: ident.Account,
		SiteID: h.siteID, CreatedAt: createdAt, IsBot: true,
	}); err != nil {
		return nil, fmt.Errorf("upsert bot dm subscription: %w", err)
	}

	// Same-site target (or unset SiteID → treated as local since we have no
	// remote address to federate to): upsert the subscription directly.
	// Remote: federate via OUTBOX so the target-site inbox-worker upserts it.
	if target.SiteID == "" || target.SiteID == h.siteID {
		if _, err := h.store.UpsertSubscription(c, &Subscription{
			ID: h.newUUIDv7(), RoomID: roomID, UserID: target.ID, Account: target.Account,
			SiteID: h.siteID, CreatedAt: createdAt,
		}); err != nil {
			return nil, fmt.Errorf("upsert target dm subscription: %w", err)
		}
	} else {
		// DM member_added carries roomType=botDM + RequesterAccount=bot so the
		// target names the subscription after the counterparty; subscription-only, no rooms doc.
		if err := h.federateMemberAdded(c, roomID, target.ID, target.Account, target.SiteID, createdAt,
			model.RoomTypeBotDM, "", ident.Account); err != nil {
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
	}); err != nil {
		return nil, fmt.Errorf("upsert owner subscription: %w", err)
	}

	// Channel rooms are always encrypted (mirrors room-service/room-worker):
	// generate + durably store the room key, then fan it out to the owner.
	// A fan-out publish failure is logged, not fatal — the key is already
	// stored, so a later ensure/get RPC (added in a follow-up task) can
	// recover it.
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
		}); err != nil {
			return nil, fmt.Errorf("upsert member subscription: %w", err)
		}
		addedIDs = append(addedIDs, u.ID)

		// Channel-shape event; RoomName names the subscription at the target.
		if u.SiteID != "" && u.SiteID != h.siteID {
			if err := h.federateMemberAdded(c, roomID, u.ID, u.Account, u.SiteID, createdAt,
				model.RoomTypeChannel, req.Name, ident.Account); err != nil {
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
				roomType, room.Name, ident.Account); err != nil {
				return nil, err
			}
		}
	}

	// Fan out the room's current key to newly-subscribed accounts only —
	// duplicate adds already have the key from their original add. The key
	// was created at room-create time and is not re-rotated for adds
	// (mirrors room-worker.buildAndFanOutRoomKey).
	if len(newAccounts) > 0 {
		pair, err := h.keyStore.Get(c, roomID)
		if err != nil {
			if errors.Is(err, roomkeystore.ErrNoCurrentKey) {
				// Legacy/broken room with no key: nothing to fan out, not fatal.
				slog.WarnContext(c, "no current key on add-member; skip fan-out", "roomID", roomID)
			} else {
				return nil, fmt.Errorf("get room key: %w", err)
			}
		} else {
			h.fanOutKey(c, roomID, newAccounts, model.RoomKeyEvent{
				RoomID:     roomID,
				Version:    pair.Version,
				PrivateKey: pair.KeyPair.PrivateKey,
			}, "fan out room key on add failed")
		}
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

	// Pre-validate the whole batch so a mid-loop self-ID doesn't leave
	// earlier removals committed before the request fails.
	for _, userID := range req.UserIDs {
		if userID == ident.ID {
			return nil, errcode.Forbidden("bot cannot remove itself",
				errcode.WithReason(errcode.BotCannotRemoveSelf))
		}
	}

	removed := []string{}
	for _, userID := range req.UserIDs {
		wasThere, err := h.store.DeleteSubscription(c, roomID, userID)
		if err != nil {
			return nil, fmt.Errorf("delete subscription: %w", err)
		}
		if !wasThere {
			// Duplicate remove is a no-op.
			continue
		}
		removed = append(removed, userID)

		u, err := h.store.FindUser(c, userID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// User doc is gone (best-effort federation is fine).
				continue
			}
			// Transient store error — the local removal already committed
			// but federation would be lost. Log so an operator can see it;
			// don't fail the outer op (matches sysmsg best-effort semantics).
			slog.WarnContext(c, "bot-room-service find user for federation failed",
				"userID", userID, "roomID", roomID, "error", err)
			continue
		}
		if u.SiteID != "" && u.SiteID != h.siteID {
			if err := h.federateMemberRemoved(c, roomID, u.ID, u.Account, u.SiteID); err != nil {
				return nil, err
			}
		}
	}
	// Only rotate when at least one subscription was actually deleted — a
	// no-op remove must not rotate the room key (matches user pipeline).
	if len(removed) > 0 {
		if err := h.rotateAndFanOut(c, roomID); err != nil {
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

// fanOutKey marshals evt once and best-effort delivers it to every account.
// A per-recipient publish failure is logged with `warnMsg` + account and does
// not abort the fan-out — key delivery is recoverable via room-service.getRoomKey
// on the client's next decrypt-miss.
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

// rotateAndFanOut generates v+1 for roomID, fans it out to the post-deletion
// survivor snapshot, then commits it via Rotate. Fan-out happens BEFORE
// Rotate so survivors hold v+1 before broadcast-worker starts encrypting
// under it (mirrors room-worker.rotateAndFanOut).
//
// If the room has no current key (legacy/broken channel), the fan-out is
// skipped entirely and the new pair is stored via Set so the room lands with
// a valid v1 key.
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

	if currentPair == nil {
		slog.WarnContext(ctx, "no current key on remove-member; skip fan-out", "roomID", roomID)
		if _, err := h.keyStore.Set(ctx, roomID, *newPair); err != nil {
			return fmt.Errorf("store new key (no prior): %w", err)
		}
		return nil
	}

	predictedVersion := currentPair.Version + 1
	h.fanOutKey(ctx, roomID, survivors,
		model.RoomKeyEvent{RoomID: roomID, Version: predictedVersion, PrivateKey: newPair.PrivateKey},
		"fan out rotated key failed")

	if _, err := h.keyStore.Rotate(ctx, roomID, *newPair); err != nil {
		if errors.Is(err, roomkeystore.ErrNoCurrentKey) {
			// Fan-out already committed survivors to predictedVersion; persist at
			// the same version so broadcast-worker reads under the key clients hold.
			if setErr := h.keyStore.SetWithVersion(ctx, roomID, *newPair, predictedVersion); setErr != nil {
				return fmt.Errorf("store new key (fallback): %w", setErr)
			}
			return nil
		}
		return fmt.Errorf("rotate key: %w", err)
	}
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
func (h *handler) federateMemberAdded(ctx context.Context, roomID, userID, account, destSiteID string, at time.Time,
	roomType model.RoomType, roomName, requesterAccount string,
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
