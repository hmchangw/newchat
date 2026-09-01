package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/badgecache"
	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	NatsURL       string `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"default"`
	MongoURI      string `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"  envDefault:""`
	Pool          mongoutil.PoolConfig
	MaxWorkers    int `env:"MAX_WORKERS"     envDefault:"100"`
	// RoomSubCache memoizes the room-membership check on the activity-refresh
	// lane, which would otherwise cost a Mongo read per broadcast message.
	// Either value non-positive disables it.
	RoomSubCacheSize int                     `env:"ROOM_SUB_CACHE_SIZE" envDefault:"50000"`
	RoomSubCacheTTL  time.Duration           `env:"ROOM_SUB_CACHE_TTL"  envDefault:"5m"`
	Consumer         stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap        bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	HealthAddr       string                  `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled     bool                    `env:"PPROF_ENABLED" envDefault:"false"`
	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string               `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`
	Buddy           natsutil.BuddyConfig `envPrefix:"BUDDY_"`
	// ValkeyAddrs seeds the Valkey cluster backing the badge cache
	// (pkg/badgecache) and best-effort subauthcache L2 invalidation after a
	// federated role_updated/member_removed write; empty disables both (clear
	// hooks and busts become no-ops, and the subauthcache TTL reconciles). A
	// connect failure logs and continues rather than exiting — both are
	// optional cache tiers, not hard dependencies.
	Valkey valkeyutil.Config
	// BadgeCacheTTL bounds how long a badge set survives without a refresh.
	// Keep identical across all badge-cache writers.
	BadgeCacheTTL time.Duration `env:"BADGE_CACHE_TTL" envDefault:"24h"`
}

// mongoInboxStore implements InboxStore using MongoDB.
type mongoInboxStore struct {
	subCol        *mongo.Collection
	roomCol       *mongo.Collection
	userCol       *mongo.Collection
	threadSubCol  *mongo.Collection
	remoteRoomCol *mongo.Collection
}

// remoteRoomsCollection stores model.RemoteRoom. Deliberately NOT rooms: that
// one is room-service's and its readers assume complete documents (members,
// encKey, counts), which a federated peer cannot supply. Migration's room_sync
// does replicate whole rooms here, so a migrated room can hold both and a
// reader spanning the two must prefer rooms.
const remoteRoomsCollection = "remote_rooms"

// HasRoomSubscription reports whether any subscription for roomID exists here.
// Served by the (roomId, u.account) unique index as a prefix scan; projected to
// _id and capped at one doc since only existence matters.
func (s *mongoInboxStore) HasRoomSubscription(ctx context.Context, roomID string) (bool, error) {
	err := s.subCol.FindOne(ctx, bson.M{"roomId": roomID},
		options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		return false, nil
	default:
		return false, fmt.Errorf("check room subscription %q: %w", roomID, err)
	}
}

// DeleteRemoteRoomActivity drops a remote room's ordering row. Absent row is
// not an error — the room may never have had one.
func (s *mongoInboxStore) DeleteRemoteRoomActivity(ctx context.Context, roomID string) error {
	if _, err := s.remoteRoomCol.DeleteOne(ctx, bson.M{"_id": roomID}); err != nil {
		return fmt.Errorf("delete remote room activity %q: %w", roomID, err)
	}
	return nil
}

// UpsertRemoteRoomActivity advances a remote room's position under $max, so
// out-of-order delivery can never regress it. $setOnInsert carries the
// immutable identity so a first touch creates the row complete.
func (s *mongoInboxStore) UpsertRemoteRoomActivity(ctx context.Context, roomID, siteID string, lastMsgAt time.Time) error {
	filter := bson.M{"_id": roomID}
	update := bson.M{
		"$max":         bson.M{"lastMsgAt": lastMsgAt},
		"$setOnInsert": bson.M{"siteId": siteID},
	}
	opts := options.UpdateOne().SetUpsert(true)
	if _, err := s.remoteRoomCol.UpdateOne(ctx, filter, update, opts); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("upsert remote room activity %q: %w", roomID, err)
		}
		// Two concurrent first-touches. Unlike UpsertRoom, whose guard lives in
		// the filter, this one filters on _id alone — so a duplicate says
		// nothing about which value is newer. Retry; the row exists now.
		if _, err := s.remoteRoomCol.UpdateOne(ctx, filter, update, opts); err != nil {
			return fmt.Errorf("upsert remote room activity %q (retry after duplicate key): %w", roomID, err)
		}
	}
	return nil
}

func (s *mongoInboxStore) CreateSubscription(ctx context.Context, sub *model.Subscription) error {
	_, err := s.subCol.InsertOne(ctx, sub)
	return err
}

// UpsertRoom replicates room metadata, guarded by the incoming room's
// UpdatedAt so out-of-order federated delivery cannot regress it. The guard
// is in the filter, so an event whose UpdatedAt is not strictly newer than the
// stored one fails to match; with upsert enabled that falls back to an insert
// which collides on _id (the room already exists) — a duplicate-key error we
// treat as a no-op. A genuinely new room (no stored doc) is inserted normally.
func (s *mongoInboxStore) UpsertRoom(ctx context.Context, room *model.Room) error {
	filter := bson.M{
		"_id": room.ID,
		"$or": bson.A{
			bson.M{"updatedAt": bson.M{"$exists": false}},
			bson.M{"updatedAt": bson.M{"$lt": room.UpdatedAt}},
		},
	}
	update := bson.M{"$set": room}
	opts := options.UpdateOne().SetUpsert(true)
	if _, err := s.roomCol.UpdateOne(ctx, filter, update, opts); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Guard rejected a stale/duplicate room_sync; the existing doc is
			// newer-or-equal, so dropping this event is correct.
			return nil
		}
		return fmt.Errorf("upsert room %q: %w", room.ID, err)
	}
	return nil
}

// UpdateSubscriptionRoles applies roles under a rolesUpdatedAt guard so an
// out-of-order or duplicate role_updated cannot regress roles. A MatchedCount
// of 0 is ambiguous — either the subscription is missing (federation race:
// surface an error so the event is redelivered until member_added lands) or
// the guard rejected a stale event (the sub exists with rolesUpdatedAt >= the
// incoming one — a silent no-op). One existence check on this cold path
// disambiguates the two.
func (s *mongoInboxStore) UpdateSubscriptionRoles(ctx context.Context, account, roomID string, roles []model.Role, rolesUpdatedAt time.Time) error {
	filter := bson.M{
		"u.account": account,
		"roomId":    roomID,
		"$or": bson.A{
			bson.M{"rolesUpdatedAt": bson.M{"$exists": false}},
			bson.M{"rolesUpdatedAt": bson.M{"$lt": rolesUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"roles": roles, "rolesUpdatedAt": rolesUpdatedAt}}
	res, err := s.subCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription roles for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	return nil
}

// naksIfSubscriptionMissing disambiguates a MatchedCount==0 guarded subscription write. A genuinely
// missing subscription returns an error (Nak → redelivered until member_added lands, the
// federation/migration race where field events can race ahead of member_added); a stale event the
// high-water guard rejected is a silent no-op (the sub exists with a newer-or-equal value).
func (s *mongoInboxStore) naksIfSubscriptionMissing(ctx context.Context, account, roomID string) error {
	exists, err := s.subscriptionExists(ctx, account, roomID)
	if err != nil {
		return fmt.Errorf("check subscription exists for %q in room %q: %w", account, roomID, err)
	}
	if !exists {
		return fmt.Errorf("subscription not found for %q in room %q", account, roomID)
	}
	return nil
}

// subscriptionExists reports whether a subscription for (account, roomID) is
// present, used to distinguish a missing sub from a guard rejection.
func (s *mongoInboxStore) subscriptionExists(ctx context.Context, account, roomID string) (bool, error) {
	err := s.subCol.FindOne(ctx,
		bson.M{"u.account": account, "roomId": roomID},
		options.FindOne().SetProjection(bson.M{"_id": 1}),
	).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *mongoInboxStore) DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) error {
	_, err := s.subCol.DeleteMany(ctx, bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}})
	if err != nil {
		return fmt.Errorf("delete subscriptions in room %q: %w", roomID, err)
	}
	return nil
}

func (s *mongoInboxStore) DeleteThreadSubscriptions(ctx context.Context, roomID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	_, err := s.threadSubCol.DeleteMany(ctx, bson.M{"roomId": roomID, "userAccount": bson.M{"$in": accounts}})
	if err != nil {
		return fmt.Errorf("delete thread subscriptions in room %q: %w", roomID, err)
	}
	return nil
}

func (s *mongoInboxStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	cursor, err := s.userCol.Find(ctx, bson.M{"account": bson.M{"$in": accounts}})
	if err != nil {
		return nil, fmt.Errorf("find users by accounts: %w", err)
	}
	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}

// UpdateUserStatus mirrors a cross-site status change onto the local users doc
// keyed by account. statusIsShow is written only when non-nil so a text-only
// update cannot clobber the stored flag. A missing user (no doc on this site)
// is a silent no-op — the event is for an account that doesn't live here.
func (s *mongoInboxStore) UpdateUserStatus(ctx context.Context, account, statusText string, statusIsShow *bool, statusUpdatedAt time.Time) error {
	set := bson.M{"statusText": statusText, "statusUpdatedAt": statusUpdatedAt}
	if statusIsShow != nil {
		set["statusIsShow"] = *statusIsShow
	}
	// Guard on the statusUpdatedAt high-water mark so an out-of-order or duplicate event
	// (the status fans to all sites) can't regress to an older status.
	filter := bson.M{"account": account, "$or": bson.A{
		bson.M{"statusUpdatedAt": bson.M{"$exists": false}},
		bson.M{"statusUpdatedAt": bson.M{"$lt": statusUpdatedAt}},
	}}
	if _, err := s.userCol.UpdateOne(ctx, filter, bson.M{"$set": set}); err != nil {
		return fmt.Errorf("update user status for %q: %w", account, err)
	}
	return nil
}

// UpdateUserSettings replaces the local users doc's settings sub-document with the origin
// site's full post-update settings — whole-object, so a field the user cleared is cleared
// here too. A missing user (no doc on this site) is a silent no-op.
func (s *mongoInboxStore) UpdateUserSettings(ctx context.Context, account string, settings *model.UserSettings, updatedAt time.Time) error {
	// Guard on the settingsUpdatedAt high-water mark so an out-of-order or duplicate event
	// (settings fan to all sites) can't regress to older settings. $lte, not $lt: two
	// writes can share a millisecond, and dropping the second would leave a remote site
	// permanently behind. Safe because the apply is an idempotent whole-object replace —
	// a same-ms tie resolves to last-delivered.
	filter := bson.M{"account": account, "$or": bson.A{
		bson.M{"settingsUpdatedAt": bson.M{"$exists": false}},
		bson.M{"settingsUpdatedAt": bson.M{"$lte": updatedAt}},
	}}
	set := bson.M{"settings": settings, "settingsUpdatedAt": updatedAt}
	if _, err := s.userCol.UpdateOne(ctx, filter, bson.M{"$set": set}); err != nil {
		return fmt.Errorf("update user settings for %q: %w", account, err)
	}
	return nil
}

// ApplyUserPermissions applies state to every listed account under the per-key watermark
// guard. $lte, not $lt: two writes can share a millisecond, and the apply is an
// idempotent whole-state replace, so a same-ms tie resolves to last-delivered. No
// upsert — a missing user doc is a silent no-op; MatchedCount < len(accounts) is normal.
func (s *mongoInboxStore) ApplyUserPermissions(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) error {
	if len(accounts) == 0 {
		return nil
	}
	field, ok := model.PermissionFieldName(permission)
	if !ok {
		return fmt.Errorf("apply user permissions: unknown permission %q", permission)
	}
	path := "permissions." + field
	filter := bson.M{
		"account": bson.M{"$in": accounts},
		"$or": bson.A{
			bson.M{path + ".updatedAt": bson.M{"$exists": false}},
			bson.M{path + ".updatedAt": bson.M{"$lte": state.UpdatedAt}},
		},
	}
	if _, err := s.userCol.UpdateMany(ctx, filter, bson.M{"$set": bson.M{path: state}}); err != nil {
		return fmt.Errorf("update user permissions: %w", err)
	}
	return nil
}

// UpdateUserChatlist replaces the local users doc's chatlist sub-document with the origin
// site's full post-update state — whole-object, so a removed section is removed here too.
// A missing user (no doc on this site) is a silent no-op. updatedAt is unix-millis
// (int64), matching how user-service stamps chatlistUpdatedAt — keeps the field's
// representation consistent instead of round-tripping through time.Time.
func (s *mongoInboxStore) UpdateUserChatlist(ctx context.Context, account string, chatlist *model.ChatlistState, updatedAt int64) error {
	// Guard on the chatlistUpdatedAt high-water mark so an out-of-order or duplicate event
	// (chatlist fans to all sites) can't regress to older state.
	filter := bson.M{"account": account, "$or": bson.A{
		bson.M{"chatlistUpdatedAt": bson.M{"$exists": false}},
		bson.M{"chatlistUpdatedAt": bson.M{"$lt": updatedAt}},
	}}
	set := bson.M{"chatlist": chatlist, "chatlistUpdatedAt": updatedAt}
	if _, err := s.userCol.UpdateOne(ctx, filter, bson.M{"$set": set}); err != nil {
		return fmt.Errorf("update user chatlist for %q: %w", account, err)
	}
	return nil
}

// UpsertUserAccount — see the InboxStore interface comment for why this one
// upserts when the other user_* appliers do not.
func (s *mongoInboxStore) UpsertUserAccount(ctx context.Context, e *model.UserAccountUpdated, updatedAt time.Time) error {
	roles := e.Roles
	if roles == nil {
		roles = []model.UserRole{}
	}
	filter := bson.M{"account": e.Account, "$or": bson.A{
		bson.M{"accountUpdatedAt": bson.M{"$exists": false}},
		bson.M{"accountUpdatedAt": bson.M{"$lte": updatedAt}},
	}}
	update := bson.M{
		"$setOnInsert": bson.M{"_id": e.ID, "siteId": e.SiteID},
		"$set": bson.M{"engName": e.EngName, "chineseName": e.ChineseName,
			"roles": roles, "active": e.Active, "accountUpdatedAt": updatedAt},
	}
	_, err := s.userCol.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		// Doc exists: a newer snapshot (stale → retry matches nothing) or the
		// HR lane's insert raced ours (no watermark → retry applies).
		_, err = s.userCol.UpdateOne(ctx, filter, update)
	}
	if err != nil {
		return fmt.Errorf("upsert user account for %q: %w", e.Account, err)
	}
	return nil
}

// BulkCreateSubscriptions inserts the supplied subs idempotently. Each is
// keyed by (roomId, u.account) and written via $setOnInsert so an existing
// sub (from a previous delivery, or with read-state already accumulated) is
// preserved. Redelivered cross-site events become no-ops on Mongo.
func (s *mongoInboxStore) BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error {
	if len(subs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, len(subs))
	for i, sub := range subs {
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.M{"roomId": sub.RoomID, "u.account": sub.User.Account}).
			SetUpdate(bson.M{"$setOnInsert": sub}).
			SetUpsert(true)
	}
	opts := options.BulkWrite().SetOrdered(false)
	if _, err := s.subCol.BulkWrite(ctx, models, opts); err != nil {
		return fmt.Errorf("bulk upsert subscriptions: %w", err)
	}
	return nil
}

// BulkRefreshJoinedAt sets joinedAt on existing (roomId, account) replicas — the
// Teams migration's cross-site joinedAt correction. joinedAt only; a missing
// replica leaves MatchedCount 0 and is a silent no-op (no insert).
func (s *mongoInboxStore) BulkRefreshJoinedAt(ctx context.Context, roomID string, joinedAtByAccount map[string]time.Time) error {
	if len(joinedAtByAccount) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(joinedAtByAccount))
	for account, joinedAt := range joinedAtByAccount {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"roomId": roomID, "u.account": account}).
			SetUpdate(bson.M{"$set": bson.M{"joinedAt": joinedAt}}))
	}
	if _, err := s.subCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk refresh joinedAt for %d replicas: %w", len(models), err)
	}
	return nil
}

// UpdateSubscriptionMute sets muted by (roomID, account) under a muteUpdatedAt
// guard so an out-of-order or duplicate toggle cannot regress mute state.
// Missing-sub and guard-rejected events both leave MatchedCount 0 and are
// silent no-ops.
func (s *mongoInboxStore) UpdateSubscriptionMute(ctx context.Context, roomID, account string, muted bool, muteUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"muteUpdatedAt": bson.M{"$exists": false}},
			bson.M{"muteUpdatedAt": bson.M{"$lt": muteUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"muted": muted, "muteUpdatedAt": muteUpdatedAt}}
	res, err := s.subCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription mute for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	return nil
}

// UpdateSubscriptionFavorite sets favorite by (roomID, account) under a
// favoriteUpdatedAt guard so an out-of-order or duplicate toggle cannot regress
// favorite state. Missing-sub and guard-rejected events both leave MatchedCount
// 0 and are silent no-ops. Mirrors the origin toggle's section clear: turning
// favorite off also drops sectionId/sectionOrder when they're "favorites".
func (s *mongoInboxStore) UpdateSubscriptionFavorite(ctx context.Context, roomID, account string, favorite bool, favoriteUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"favoriteUpdatedAt": bson.M{"$exists": false}},
			bson.M{"favoriteUpdatedAt": bson.M{"$lt": favoriteUpdatedAt}},
		},
	}
	set := bson.M{"favorite": favorite, "favoriteUpdatedAt": favoriteUpdatedAt}
	var update any = bson.M{"$set": set}
	if !favorite {
		clearFavSection := bson.M{"$eq": bson.A{"$sectionId", model.SectionFavorites}}
		set["sectionId"] = bson.M{"$cond": bson.A{clearFavSection, "$$REMOVE", "$sectionId"}}
		set["sectionOrder"] = bson.M{"$cond": bson.A{clearFavSection, "$$REMOVE", "$sectionOrder"}}
		update = mongo.Pipeline{bson.D{{Key: "$set", Value: set}}}
	}
	res, err := s.subCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription favorite for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	return nil
}

// UpdateSubscriptionSection sets sectionId+sectionOrder (or clears both when
// sectionID==nil, a remove) by (roomID, account) under a sectionUpdatedAt guard so
// an out-of-order or duplicate move cannot regress. A guard-rejected event is a
// silent no-op; a missing sub NAKs so it retries after the sub replicates. Mirrors
// favorite alongside, same as the origin write: true only when sectionID ==
// "favorites", false otherwise (including a remove).
func (s *mongoInboxStore) UpdateSubscriptionSection(ctx context.Context, roomID, account string, sectionID *string, order float64, updatedAt time.Time) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"sectionUpdatedAt": bson.M{"$exists": false}},
			bson.M{"sectionUpdatedAt": bson.M{"$lt": updatedAt}},
		},
	}
	var update bson.M
	if sectionID == nil {
		update = bson.M{
			"$set":   bson.M{"sectionUpdatedAt": updatedAt, "favorite": false, "favoriteUpdatedAt": updatedAt},
			"$unset": bson.M{"sectionId": "", "sectionOrder": ""},
		}
	} else {
		update = bson.M{"$set": bson.M{
			"sectionId":         *sectionID,
			"sectionOrder":      order,
			"sectionUpdatedAt":  updatedAt,
			"favorite":          *sectionID == model.SectionFavorites,
			"favoriteUpdatedAt": updatedAt,
		}}
	}
	res, err := s.subCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription section for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	return nil
}

// UpdateSubscriptionOpen sets open by (roomID, account). No high-water guard —
// set-true is idempotent and order-insensitive. A genuinely missing sub returns an
// error (Nak) via naksIfSubscriptionMissing so the event redelivers until the
// member_added that creates the sub lands.
func (s *mongoInboxStore) UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error {
	res, err := s.subCol.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"open": open}},
	)
	if err != nil {
		return fmt.Errorf("update subscription open for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	return nil
}

func (s *mongoInboxStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) (bool, int, error) {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"lastSeenAt": bson.M{"$exists": false}},
			bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
		},
	}
	update := bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}}
	var out struct {
		ThreadUnread []string `bson:"threadUnread"`
	}
	opts := options.FindOneAndUpdate().
		SetProjection(bson.D{{Key: "threadUnread", Value: 1}}).
		SetReturnDocument(options.After)
	err := s.subCol.FindOneAndUpdate(ctx, filter, update, opts).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Either the subscription is missing (NAK to retry) or the order guard
		// rejected a stale event (a no-op, not a failure).
		return false, 0, s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	if err != nil {
		return false, 0, fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	return true, len(out.ThreadUnread), nil
}

// ensureIndexes verifies the thread_subscriptions (threadRoomId, userAccount)
// unique index that UpsertThreadSubscription relies on. The index is owned by
// room-service (which also drops the legacy threadRoomId_1_userId_1 index); this
// worker only warns if it is missing, never creates it — a divergent spec would
// crashloop the shared collection, and a missing index must not take the worker down.
func (s *mongoInboxStore) ensureIndexes(ctx context.Context) {
	mongoutil.WarnMissingIndexes(ctx, s.threadSubCol, "threadRoomId_1_userAccount_1")
	// SetSubscriptionMentions filters on (roomId, u.account); without this index
	// the federated badge write collscans the shared subscriptions collection.
	mongoutil.WarnMissingIndexes(ctx, s.subCol, "roomId_1_u.account_1")
	// users.account (unique, owned by user-service): UpsertUserAccount's E11000
	// retry branch — the stale-event-vs-HR-race disambiguator — only fires when
	// account uniqueness is index-enforced.
	mongoutil.WarnMissingIndexes(ctx, s.userCol, "account_1")
}

// SetSubscriptionMentions flags the accounts' subscriptions as mentioned. The
// guard is $not/$gte rather than $lt so a never-read subscription (missing
// lastSeenAt) still matches — plain $lt would skip the missing field (#467).
func (s *mongoInboxStore) SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error {
	_, err := s.subCol.UpdateMany(ctx,
		bson.M{
			"roomId":     roomID,
			"u.account":  bson.M{"$in": accounts},
			"lastSeenAt": bson.M{"$not": bson.M{"$gte": msgCreatedAt}},
		},
		bson.M{"$set": bson.M{"hasMention": true}},
	)
	if err != nil {
		return fmt.Errorf("set subscription mentions for room %s: %w", roomID, err)
	}
	return nil
}

// UpsertThreadSubscription inserts the subscription on first event for a
// (threadRoomId, userAccount) pair, and on subsequent events updates only
// updatedAt and (monotonically) hasMention. $setOnInsert pins the immutable
// fields on insert; $set always refreshes updatedAt; $max on hasMention
// guarantees a non-mention event never clears a prior mention=true.
//
// The dedupe key is userAccount (not userId) so it matches the unique index and
// the key used by message-worker / room-service / history-service. userId is a
// site-local identity that can differ between the local document and a federated
// event for the same account; keying on it would insert a duplicate that then
// collides with the (threadRoomId, userAccount) unique index.
//
// $max on a bool works because BSON encodes false (0x00) < true (0x01), so
// $max(existing, incoming) for a bool is equivalent to a monotonic OR.
//
// $setOnInsert and $max operate on disjoint fields (hasMention is set by $max
// only — never by $setOnInsert) so MongoDB doesn't reject the update with a
// "conflicting update operators" error.
func (s *mongoInboxStore) UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error {
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":             sub.ID,
			"parentMessageId": sub.ParentMessageID,
			"roomId":          sub.RoomID,
			"threadRoomId":    sub.ThreadRoomID,
			"userId":          sub.UserID,
			"userAccount":     sub.UserAccount,
			"siteId":          sub.SiteID,
			"lastSeenAt":      sub.LastSeenAt,
			"createdAt":       sub.CreatedAt,
		},
		"$set": bson.M{"updatedAt": sub.UpdatedAt},
		"$max": bson.M{"hasMention": sub.HasMention},
	}
	if _, err := s.threadSubCol.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert thread subscription (threadRoomID %q, userID %q): %w",
			sub.ThreadRoomID, sub.UserID, err)
	}
	return nil
}

// UpdateSubscriptionNamesForRoom sets name on every subscription in the room,
// each guarded by its own nameUpdatedAt ($lt) so an out-of-order rename cannot
// regress a sub to a stale name. UpdateMany applies the guard per document, so
// subs already carrying a newer rename are skipped while the rest advance.
func (s *mongoInboxStore) UpdateSubscriptionNamesForRoom(ctx context.Context, roomID, newName string, nameUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId": roomID,
		"$or": bson.A{
			bson.M{"nameUpdatedAt": bson.M{"$exists": false}},
			bson.M{"nameUpdatedAt": bson.M{"$lt": nameUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"name": newName, "nameUpdatedAt": nameUpdatedAt}}
	if _, err := s.subCol.UpdateMany(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription names for room %s: %w", roomID, err)
	}
	return nil
}

// ApplySubscriptionRestriction writes {restricted, externalAccess, roles} to all
// subs in the room, each guarded by its own restrictUpdatedAt ($lt) so an
// out-of-order visibility change cannot regress the flags/roles. The guard lives
// in the filter for both the restrict-with-owner pipeline branch and the
// flags-only branch.
func (s *mongoInboxStore) ApplySubscriptionRestriction(ctx context.Context, roomID string, restricted, externalAccess bool, ownerAccount string, restrictUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId": roomID,
		"$or": bson.A{
			bson.M{"restrictUpdatedAt": bson.M{"$exists": false}},
			bson.M{"restrictUpdatedAt": bson.M{"$lt": restrictUpdatedAt}},
		},
	}
	if restricted && ownerAccount != "" {
		pipeline := mongo.Pipeline{
			bson.D{{Key: "$set", Value: bson.M{
				"restricted":        true,
				"externalAccess":    externalAccess,
				"restrictUpdatedAt": restrictUpdatedAt,
				"roles": bson.M{"$cond": bson.M{
					"if":   bson.M{"$eq": bson.A{"$u.account", ownerAccount}},
					"then": bson.A{string(model.RoleOwner)},
					"else": bson.A{string(model.RoleUser)},
				}},
			}}},
		}
		if _, err := s.subCol.UpdateMany(ctx, filter, pipeline); err != nil {
			return fmt.Errorf("apply visibility (restrict+rewrite): %w", err)
		}
		return nil
	}
	if _, err := s.subCol.UpdateMany(ctx, filter, bson.M{
		"$set": bson.M{"restricted": restricted, "externalAccess": externalAccess, "restrictUpdatedAt": restrictUpdatedAt},
	}); err != nil {
		return fmt.Errorf("apply visibility (flags only): %w", err)
	}
	return nil
}

// ListSubscriptionAccountsByRoom returns the accounts subscribed to roomID on
// this site's local replica. Callers only read the subscriber account, so
// project just that field — mirrors room-service's ListSubscriptionsByRoom.
func (s *mongoInboxStore) ListSubscriptionAccountsByRoom(ctx context.Context, roomID string) ([]string, error) {
	cursor, err := s.subCol.Find(ctx,
		bson.M{"roomId": roomID},
		options.Find().SetProjection(bson.M{"_id": 0, "u.account": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("list subscription accounts for room %q: find: %w", roomID, err)
	}
	var docs []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("list subscription accounts for room %q: decode: %w", roomID, err)
	}
	accounts := make([]string, 0, len(docs))
	for _, d := range docs {
		if d.User.Account != "" {
			accounts = append(accounts, d.User.Account)
		}
	}
	return accounts, nil
}

// threadReadGuard adds the order-safety clause to a thread-subscription filter:
// only docs whose lastSeenAt is unset or older than the event's may advance, so
// out-of-order delivery can never regress a read position.
func threadReadGuard(filter bson.M, lastSeenAt time.Time) bson.M {
	filter["$or"] = bson.A{
		bson.M{"lastSeenAt": nil},
		bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
	}
	return filter
}

// threadReadUpdate is the read-advance write shared by the single and bulk
// thread-read applies: advance the read position, clear the mention flag.
func threadReadUpdate(lastSeenAt time.Time) bson.M {
	return bson.M{"$set": bson.M{
		"lastSeenAt": lastSeenAt,
		"updatedAt":  lastSeenAt,
		"hasMention": false,
	}}
}

// ApplyThreadRead advances the home-replica ThreadSubscription under the $lt
// guard, then — only when that guarded update matched — $pulls parentMessageID
// from the Subscription's threadUnread. The MatchedCount gate keeps a
// stale/duplicate event from touching the Subscription.
func (s *mongoInboxStore) ApplyThreadRead(ctx context.Context, roomID, threadRoomID, account, parentMessageID string, lastSeenAt time.Time) error {
	filter := threadReadGuard(bson.M{"threadRoomId": threadRoomID, "userAccount": account}, lastSeenAt)
	tsRes, err := s.threadSubCol.UpdateOne(ctx, filter, threadReadUpdate(lastSeenAt))
	if err != nil {
		return fmt.Errorf("apply thread read for %q in thread room %q: %w", account, threadRoomID, err)
	}
	// Guard rejected (stale/duplicate) or legacy event without a parent ID:
	// leave threadUnread alone.
	if tsRes.MatchedCount == 0 || parentMessageID == "" {
		return nil
	}

	// Per-ID $pull commutes with concurrent thread_unread_added $addToSets.
	if _, err := s.subCol.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$pull": bson.M{"threadUnread": parentMessageID}},
	); err != nil {
		return fmt.Errorf("apply thread read on subscription for %q in room %q: %w", account, roomID, err)
	}
	return nil
}

// ApplyThreadReadAll is the home-replica bulk clear for the federated
// thread_read_all event: advance all of account's thread subscriptions under a
// per-doc $lt guard and $unset threadUnread on every subscription that has
// unread threads. Missing docs are a no-op.
func (s *mongoInboxStore) ApplyThreadReadAll(ctx context.Context, account string, lastSeenAt time.Time) error {
	filter := threadReadGuard(bson.M{"userAccount": account}, lastSeenAt)
	if _, err := s.threadSubCol.UpdateMany(ctx, filter, threadReadUpdate(lastSeenAt)); err != nil {
		return fmt.Errorf("apply thread read all on thread subscriptions for %q: %w", account, err)
	}

	subFilter := bson.M{"u.account": account, "threadUnread.0": bson.M{"$exists": true}}
	subUpdate := bson.M{"$unset": bson.M{"threadUnread": ""}}
	if _, err := s.subCol.UpdateMany(ctx, subFilter, subUpdate); err != nil {
		return fmt.Errorf("apply thread read all on subscriptions for %q: %w", account, err)
	}
	return nil
}

// AddThreadUnread marks parentMessageID unread for accounts' subscriptions in
// roomID via a single $addToSet UpdateMany. Idempotent under JetStream
// redelivery; accounts not subscribed simply match nothing.
func (s *mongoInboxStore) AddThreadUnread(ctx context.Context, roomID, parentMessageID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	if _, err := s.subCol.UpdateMany(ctx,
		bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}},
		bson.M{"$addToSet": bson.M{"threadUnread": parentMessageID}},
	); err != nil {
		return fmt.Errorf("add thread unread %q in room %q: %w", parentMessageID, roomID, err)
	}
	return nil
}

// laneMsg pairs a consumed JetStream message with the per-message context
// carrying its consumer span. The o11y/nats facade delivers (ctx, jetstream.Msg)
// separately rather than an o11y-owned message type, so the two-lane dispatch
// carries them together through membershipCh.
type laneMsg struct {
	ctx context.Context
	msg jetstream.Msg
}

// startInboxLane wires the two-lane pull pattern described at the call site:
// membership events serialized on one worker, everything else fanned out across
// a bounded pool. The home connection and the buddy connection each run this
// same pipeline over their own consumer — the handler, the membership
// serialization and the Ack/Nak disposition are identical on both, because a
// redirected event is still this site's event.
func startInboxLane(ctx context.Context, cons o11ynats.Consumer, cfg *config, handler *Handler,
	sem chan struct{}, wg *sync.WaitGroup,
) (*natsutil.Lane, error) {
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		return nil, fmt.Errorf("bind consumer messages: %w", err)
	}

	membershipCh := make(chan laneMsg, cfg.MaxWorkers)

	process := func(m laneMsg) {
		// jobguard recovers handler panics — both the membership lane and the
		// fan-out goroutines run outside natsrouter's Recovery middleware, so an
		// unrecovered panic would crash the worker and crash-loop on JetStream
		// redelivery. On panic it Acks (poison drop).
		jobguard.Run(m.msg, func() {
			msg := m.msg
			handlerCtx, _ := logctx.ConsumeContext(m.ctx, msg.Headers(), msg.Subject(), msg.Data())
			jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, handler.HandleEvent(handlerCtx, msg.Data()))
		})
	}

	// Membership lane: a single worker drains membershipCh in FIFO order, so
	// add/remove for the same (room, account) are applied in arrival order.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range membershipCh {
			process(msg)
		}
	}()

	go func() {
		defer close(membershipCh)
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			m := laneMsg{ctx: msgCtx, msg: msg}
			if isMembershipSubject(msg.Subject(), cfg.SiteID) {
				membershipCh <- m
				continue
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				process(m)
			}()
		}
	}()

	return natsutil.NewLane(iter, wg), nil
}

// startFailoverLane binds the buddy-hosted INBOX-FAILOVER consumer, feeding the
// same two-lane pattern as the home lane — a failover-lane event is still this
// site's event.
func startFailoverLane(ctx context.Context, js o11ynats.JetStream, cfg *config, handler *Handler,
	binder *failoverlane.Binder, sem chan struct{}, wg *sync.WaitGroup,
) (*natsutil.Lane, error) {
	cons, err := binder.BindConsumer(ctx, js, &failoverlane.LaneSpec{
		Stream:   stream.InboxFailover(cfg.SiteID),
		Consumer: buildFailoverConsumerConfig(cfg.Consumer, cfg.SiteID),
	})
	if err != nil {
		return nil, err
	}
	return startInboxLane(ctx, cons, cfg, handler, sem, wg)
}

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid mongo pool config", "error", err)
		os.Exit(1)
	}

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	store := &mongoInboxStore{
		subCol:        db.Collection("subscriptions"),
		roomCol:       db.Collection("rooms"),
		userCol:       db.Collection("users"),
		threadSubCol:  db.Collection("thread_subscriptions"),
		remoteRoomCol: db.Collection(remoteRoomsCollection),
	}
	store.ensureIndexes(ctx)

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	inboxCfg := stream.Inbox(cfg.SiteID)

	// Internal lane is reserved for search-sync-worker; scope to external.> only.
	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, buildConsumerConfig(cfg.Consumer, cfg.SiteID))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	// Empty VALKEY_ADDRS disables the badge cache and the subauthcache L2 bust
	// — both become no-ops (nil-checked in handler.go).
	var badge badgeCache
	var subValkey valkeyutil.Client
	valkeyClient, err := valkeyutil.ConnectRaw(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}
	if valkeyClient != nil {
		badge = badgecache.New(valkeyClient, cfg.BadgeCacheTTL, badgecache.DefaultMaxCount)
		// The same connection backs best-effort subauthcache L2 invalidation
		// after a federated role_updated/member_removed write. One pool serves
		// both tiers; an empty VALKEY_ADDRS leaves subValkey nil, which makes
		// the bust a no-op that the L2 TTL reconciles.
		subValkey = valkeyutil.WrapClusterClient(valkeyClient)
		slog.Info("badge cache and subauth L2 invalidation enabled", "ttl", cfg.BadgeCacheTTL)
	} else {
		slog.Warn("badge cache and subauth L2 invalidation DISABLED — VALKEY_ADDRS is empty (dev only)")
	}

	handler := NewHandler(store, WithRoomSubCache(cfg.RoomSubCacheSize, cfg.RoomSubCacheTTL))
	handler.badge = badge
	handler.valkey = subValkey
	slog.Info("room-sub cache configured", "size", cfg.RoomSubCacheSize, "ttl", cfg.RoomSubCacheTTL)

	// Core-NATS queue subscriber for the cross-site room-activity refresh. Not on
	// INBOX by design: the signal is coalesced, idempotent and $max-guarded, so it
	// needs no persistence or ordering, and keeping it off the stream stops a
	// high-rate hint competing for retention with membership events that do need
	// both. Fire-and-forget — a failure self-heals on the room's next message.
	activitySub, err := nc.QueueSubscribe(ctx, subject.RoomActivity(cfg.SiteID), "inbox-worker",
		func(msgCtx context.Context, msg *nats.Msg) {
			actCtx, _ := logctx.ConsumeContext(msgCtx, msg.Header, msg.Subject, msg.Data)
			if err := handler.HandleRoomActivity(actCtx, msg.Data); err != nil {
				slog.WarnContext(actCtx, "apply room activity refresh failed", "error", err)
			}
		})
	if err != nil {
		slog.Error("subscribe room-activity failed", "error", err)
		os.Exit(1)
	}

	// Two-lane pull pattern over the single INBOX external consumer:
	//
	//   - Membership events (member_added/member_removed) run on ONE
	//     sequential lane. They are NOT individually order-safe — a physical
	//     delete carries no high-water mark, so a stale add could otherwise
	//     resurrect a removed membership (and vice versa). Serializing them
	//     restores in-order processing within this instance and keeps the
	//     add/remove resurrection race at its pre-fan-out baseline.
	//   - Everything else (the high-volume subscription_read/thread_read
	//     receipts, plus role/mute/room_sync) fans out across a bounded
	//     worker pool. Those handlers are idempotent and order-safe (Mongo
	//     $lt/$max/$setOnInsert guards), so concurrent processing is correct.
	//
	// Membership traffic is a tiny fraction of the lane, so serializing it
	// costs negligible throughput while the read-receipt path keeps its full
	// MaxWorkers concurrency.
	// One pool shared by both lanes: a buddy lane with its own semaphore would
	// take this worker to 2×MAX_WORKERS in-flight handlers against the same
	// MongoDB, even though the two lanes carry the same site's events.
	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	homeLane, err := startInboxLane(ctx, cons, &cfg, handler, sem, &wg)
	if err != nil {
		slog.Error("bind INBOX lane failed", "error", err)
		os.Exit(1)
	}

	// Buddy lane. BindBuddy never fails startup — on any failure buddyLane stays
	// nil and the service runs home-only, which beats refusing to boot over a
	// peer cluster we only need during an outage.
	var buddyLane *natsutil.Lane
	binder := failoverlane.Binder{
		SiteID: cfg.SiteID, Buddy: cfg.Buddy,
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: cfg.MaxWorkers, Sem: sem, WG: &wg,
	}
	buddyConn := natsutil.BindBuddy(ctx, cfg.Buddy, cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace,
		func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			var bErr error
			buddyLane, bErr = startFailoverLane(ctx, bjs, &cfg, handler, &binder, sem, &wg)
			return bErr
		})

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("inbox-worker started", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		// Stop both iterators before draining either, so neither lane pulls new
		// work while the other is still finishing.
		func(_ context.Context) error {
			homeLane.Stop()
			buddyLane.Stop()
			return nil
		},
		func(ctx context.Context) error {
			if err := homeLane.Wait(ctx); err != nil {
				return err
			}
			return buddyLane.Wait(ctx)
		},
		// Unsubscribe before the drain so no refresh arrives after the store closes.
		func(_ context.Context) error { return activitySub.Unsubscribe() },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		// Closes the pool shared by the badge cache and the subauthcache bust.
		func(ctx context.Context) error {
			if valkeyClient == nil {
				return nil
			}
			return valkeyClient.Close()
		},
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// isMembershipSubject reports whether an INBOX external-lane subject carries a
// membership event (member_added/member_removed) for this site. Those events
// are routed to a single sequential lane because, unlike the read-receipt and
// role/mute/room_sync handlers, they have no per-document high-water-mark guard
// and so must be applied in order to avoid the add/remove resurrection race.
func isMembershipSubject(subj, siteID string) bool {
	return subj == subject.InboxExternal(siteID, model.InboxMemberAdded) ||
		subj == subject.InboxExternal(siteID, model.InboxMemberRemoved)
}

// buildConsumerConfig returns the durable consumer config for
// inbox-worker. The site-scoped FilterSubjects keeps inbox-worker on the
// cross-site `external.>` lane only; same-site internal publishes are
// reserved for search-sync-worker.
func buildConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "inbox-worker"
	cc.FilterSubjects = []string{subject.InboxExternalAll(siteID)}
	return cc
}

// buildFailoverConsumerConfig returns the durable consumer config for the
// buddy-hosted INBOX-FAILOVER lane, which carries federation events peers
// redirected here because this site's own NATS was unreachable.
//
// The durable name differs from the home lane's so the two keep independent
// cursors — in a single-server dev setup both streams live on one server, and a
// shared durable would have them clobber each other.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "inbox-worker-failover"
	cc.FilterSubjects = []string{subject.FailoverInboxExternalAll(siteID)}
	return cc
}
