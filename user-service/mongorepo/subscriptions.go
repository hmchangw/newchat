package mongorepo

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const subscriptionsCollection = "subscriptions"

// roomsCollection is the $lookup target for the deleted-filter and enrichment; owned by room-service, referenced only by name.
const roomsCollection = "rooms"

// deletedRoomNamePrefix marks room-service's soft-delete rename ("Del-"+name);
// the deleted-filter excludes matching local subs. The pipeline regex is
// derived from it so the marker has a single source of truth.
const deletedRoomNamePrefix = "Del-"

const deletedRoomNameRegex = "^" + deletedRoomNamePrefix

// SubscriptionRepo is the Mongo implementation of service.SubscriptionRepository.
type SubscriptionRepo struct {
	subscriptions *mongoutil.Collection[model.Subscription]
	// enriched decodes the room-enriched aggregation results (stored sub + read-time
	// room baseline) over the same subscriptions collection; writes go through
	// subscriptions so the baseline fields are never persisted.
	enriched *mongoutil.Collection[model.EnrichedSubscription]
	// rooms serves the phased list path's batched reads (sort keys + page
	// baselines); owned by room-service, referenced only by name.
	rooms    *mongo.Collection
	sortKeys *sortKeyCache
}

// NewSubscriptionRepo builds a SubscriptionRepo over db. The deleted-filter is
// room.name-based (^Del-): rows with a missing room doc — cross-site — are
// kept. sortKeyCacheSize/sortKeyCacheTTL configure the list sort-key cache — a
// non-positive value disables it (every list resolves sort keys from a fresh
// batched Mongo read). Defaults live in the service config (SUBS_SORTKEY_CACHE_*),
// the single source.
func NewSubscriptionRepo(db *mongo.Database, sortKeyCacheSize int, sortKeyCacheTTL time.Duration) *SubscriptionRepo {
	col := db.Collection(subscriptionsCollection)
	return &SubscriptionRepo{
		subscriptions: mongoutil.NewCollection[model.Subscription](col),
		enriched:      mongoutil.NewCollection[model.EnrichedSubscription](col),
		rooms:         db.Collection(roomsCollection),
		sortKeys:      newSortKeyCache(sortKeyCacheSize, sortKeyCacheTTL),
	}
}

// EnsureIndexes creates the subscription indexes this service queries on.
func (r *SubscriptionRepo) EnsureIndexes(ctx context.Context) error {
	if _, err := r.subscriptions.Raw().Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Serves the account+roomType match on every list/count path; the retention
		// window keys on room.lastMsgAt (a room field), so no trailing time key.
		{Keys: bson.D{{Key: "u.account", Value: 1}, {Key: "roomType", Value: 1}}},
		// Unique logical key (one subscription per room per user). Must match
		// room-service's declaration on the shared collection (mismatch → conflict).
		{Keys: bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "name", Value: 1}, {Key: "roomType", Value: 1}}},
	}); err != nil {
		return fmt.Errorf("create subscription indexes: %w", err)
	}
	return nil
}

// roomsEnrichStages builds the shared rooms-join + enrichment. When dropDeleted is true
// it drops local soft-deleted (^Del-) rooms — the list, count, and active paths all
// pass true. A missing/cross-site room has no room.name so it is kept either way. The
// rooms-type activity window is applied separately by the caller on the room's
// lastMsgAt (surfaced here).
func roomsEnrichStages(dropDeleted bool) bson.A {
	stages := bson.A{
		// Project only the room fields this enrichment surfaces (not the whole room doc) so
		// the join+sort working set stays lean; the correlated $expr/_id match uses the _id
		// index, same as roomMatchStages.
		bson.M{"$lookup": bson.M{
			"from": roomsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$_id", "$$rid"}}}},
				// No createdAt: its only consumer was the in-pipeline sort key the
				// phased list path replaced (the in-memory sort resolves its own).
				bson.M{"$project": bson.M{
					"name":              1,
					"userCount":         1,
					"appCount":          1,
					"lastMsgAt":         1,
					"lastMsgId":         1,
					"lastMentionAllAt":  1,
					"minUserLastSeenAt": 1,
					"encKey.priv":       1,
					"encKey.ver":        1,
					"crossSite":         1,
				}},
			},
			"as": "room",
		}},
		bson.M{"$unwind": bson.M{"path": "$room", "preserveNullAndEmptyArrays": true}},
	}
	if dropDeleted {
		// A local Del- room.name matches the regex → inverted by $not → dropped.
		stages = append(stages, bson.M{"$match": bson.M{"room.name": bson.M{"$not": bson.M{"$regex": deletedRoomNameRegex}}}})
	}
	return append(stages,
		bson.M{"$addFields": bson.M{
			"userCount":         "$room.userCount",
			"lastMsgAt":         "$room.lastMsgAt",
			"lastMsgId":         "$room.lastMsgId",
			"lastMentionAllAt":  "$room.lastMentionAllAt",
			"minUserLastSeenAt": "$room.minUserLastSeenAt",
			"appCount":          "$room.appCount",
			"roomName":          "$room.name",
			"crossSite":         "$room.crossSite",
			// Room E2E key baseline (current slot) for local enrichment — folds the
			// key read into this single $lookup, no separate keystore round-trip.
			"encKeyPriv": "$room.encKey.priv",
			"encKeyVer":  "$room.encKey.ver",
		}},
		bson.M{"$project": bson.M{"room": 0}},
	)
}

// matchedRoomField is the scratch array the member-match pipeline joins the local
// room into; stripped by subscriptionProjection before the result decodes.
const matchedRoomField = "__matchedRoom"

// roomMatchStages joins the local rooms collection into the matchedRoomField array
// — excluding soft-deleted (^Del-) rooms inside the $lookup — then drops any sub
// whose room is missing/deleted (empty array, via $ne: []). It runs BEFORE the
// heavier co-member self-join so the cheap room filter shrinks the candidate set
// first. Unlike roomsEnrichStages this DROPS missing/cross-site rooms (no local
// room doc ⇒ empty array): member matching is inherently local.
func roomMatchStages() []bson.D {
	return []bson.D{
		{{Key: "$lookup", Value: bson.M{
			"from": roomsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{
					"$expr": bson.M{"$eq": bson.A{"$_id", "$$rid"}},
					"name":  bson.M{"$not": bson.M{"$regex": deletedRoomNameRegex}},
				}},
				// Project only the fields FindChannelsByMembers copies out of
				// matchedRoomField (mirrors roomsEnrichStages) so the whole room doc —
				// including prior E2E key slots — doesn't transit the pipeline.
				bson.M{"$project": bson.M{
					"name":              1,
					"userCount":         1,
					"appCount":          1,
					"lastMsgAt":         1,
					"lastMsgId":         1,
					"lastMentionAllAt":  1,
					"minUserLastSeenAt": 1,
					"createdAt":         1,
					"encKey.priv":       1,
					"encKey.ver":        1,
					"crossSite":         1,
				}},
			},
			"as": matchedRoomField,
		}}},
		{{Key: "$match", Value: bson.M{matchedRoomField: bson.M{"$ne": bson.A{}}}}},
	}
}

// subscriptionFieldsProjection is the inclusion projection of the subscription
// document's own fields — every persisted field a decoded model.Subscription
// carries. The phased list path fetches it page-sized (enrichListRows); the
// member-match pipeline projects it terminally via subscriptionProjection.
func subscriptionFieldsProjection() bson.M {
	return bson.M{
		"_id":                1,
		"u":                  1,
		"roomId":             1,
		"siteId":             1,
		"origin":             1,
		"roles":              1,
		"name":               1,
		"roomType":           1,
		"isSubscribed":       1,
		"historySharedSince": 1,
		"joinedAt":           1,
		"lastSeenAt":         1,
		"hasMention":         1,
		// hasGroupMention removed from the schema; hasUnread is computed at read
		// time (bson:"-"). Neither is projected from Mongo.
		"threadUnread":      1,
		"alert":             1,
		"muted":             1,
		"favorite":          1,
		"open":              1,
		"restricted":        1,
		"externalAccess":    1,
		"favoriteUpdatedAt": 1,
		"muteUpdatedAt":     1,
		"rolesUpdatedAt":    1,
		"nameUpdatedAt":     1,
		"restrictUpdatedAt": 1,
	}
}

// subscriptionProjection is the terminal $project for the member-match pipeline:
// an inclusion projection of the subscription's fields plus the room baseline
// copied to the top level. Being inclusion-only, it naturally drops the
// pipeline's scratch arrays (__matchedRoom, __members, __memberAccounts).
func subscriptionProjection() bson.M {
	proj := subscriptionFieldsProjection()
	for _, k := range []string{
		"userCount", "lastMsgAt", "lastMsgId", "lastMentionAllAt",
		"minUserLastSeenAt", "appCount", "roomName", "crossSite",
		"encKeyPriv", "encKeyVer",
	} {
		proj[k] = 1
	}
	return proj
}

// dedupeStrings returns in with duplicates removed, preserving first-seen order.
func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// AggregateSubscriptions returns one page of account's subscriptions for listType
// (rooms = dm+channel, apps = subscribed botDMs, current = both) ordered by room
// activity (lastMsgAt, falling back to room createdAt) desc, plus a hasMore flag
// (over-fetch by one). Locally soft-deleted (^Del-) rooms are excluded. favorite
// restricts to favorited rows and pins the caller's self-DM first; withinDays
// windows the rooms type on the room's lastMsgAt (ignored for apps/current).
//
// Phased read path — no per-sub $lookup and no in-Mongo sort over the joined
// rooms: (1) one indexed fetch of the account's matching subs projected to the
// lite sort/filter fields only (_id, roomId, roomType, name), (2) per-room
// sort keys from the process-local cache with misses batched into one $in
// read, (3) deleted-filter/window/sort/page in memory, (4) fresh page-sized
// $in reads of the page's room baselines AND full subscription documents —
// only the page's rows are ever fetched or decoded in full. Ordering
// tolerates sort-key staleness up to the cache TTL; window membership and the
// page rows themselves are always fresh (a stale hit that fails the window is
// re-read, and the fresh room read re-drops rooms soft-deleted meanwhile).
func (r *SubscriptionRepo) AggregateSubscriptions(ctx context.Context, account, listType string, favorite bool, withinDays *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
	var zero mongoutil.OffsetPageHasMore[model.EnrichedSubscription]
	match := bson.M{"u.account": account}
	switch listType {
	case "current":
		match["$or"] = bson.A{
			bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}},
			bson.M{"roomType": "botDM", "isSubscribed": true},
		}
	case "rooms":
		match["roomType"] = bson.M{"$in": bson.A{"dm", "channel"}}
	case "apps":
		match["roomType"] = "botDM"
		match["isSubscribed"] = true
	}
	if favorite {
		match["favorite"] = true
	}
	// Exclude rooms explicitly closed by the user; a missing field (defensive)
	// and open:true both pass. Applied to subscription.list only.
	match["open"] = bson.M{"$ne": false}
	cur, err := r.subscriptions.Raw().Find(ctx, match,
		options.Find().SetProjection(subscriptionLiteProjection()))
	if err != nil {
		return zero, fmt.Errorf("find subscriptions: %w", err)
	}
	var subs []subLite
	if err := cur.All(ctx, &subs); err != nil {
		return zero, fmt.Errorf("read subscriptions: %w", err)
	}
	cutoff := listWindowCutoff(listType, withinDays)
	keys, err := r.resolveSortKeys(ctx, subs, cutoff)
	if err != nil {
		return zero, err
	}
	rows := buildListRows(subs, keys, account, favorite, cutoff)
	sortListRows(rows)
	data, hasMore, err := r.fillListPage(ctx, rows, page)
	if err != nil {
		return zero, err
	}
	return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: data, HasMore: hasMore}, nil
}

// subLite holds the only subscription fields the pre-page phases read: _id
// keys the page refetch, roomId the sort-key resolution, roomType+name the
// favorite self-DM pin and the name-asc tiebreak.
type subLite struct {
	ID       string         `bson:"_id"`
	RoomID   string         `bson:"roomId"`
	RoomType model.RoomType `bson:"roomType"`
	Name     string         `bson:"name"`
}

// subscriptionLiteProjection fetches exactly subLite's fields — the phase-1
// read of the phased list path. The full documents are refetched page-sized
// in enrichListRows, so the per-request working set stays O(page) full docs
// plus O(subs) lite ones.
func subscriptionLiteProjection() bson.M {
	return bson.M{"_id": 1, "roomId": 1, "roomType": 1, "name": 1}
}

// listWindowCutoff returns the withinDays activity cutoff for the rooms list
// type, or nil when no window applies (other types ignore withinDays).
func listWindowCutoff(listType string, withinDays *int) *time.Time {
	if listType != "rooms" || withinDays == nil {
		return nil
	}
	c := time.Now().UTC().AddDate(0, 0, -*withinDays)
	return &c
}

// listRow pairs a fetched subscription with its resolved sort position.
type listRow struct {
	sub subLite
	// sortAt is room lastMsgAt, falling back to room createdAt; nil (missing or
	// signal-less room) sorts after every dated row — same order the old
	// in-Mongo $ifNull sort produced.
	sortAt *time.Time
	selfDM bool // favorite view only: pins the caller's self-DM first
}

// buildListRows applies the in-memory row filters (locally soft-deleted rooms,
// the activity-window cutoff) and computes each row's sort position from the
// resolved sort keys. Missing rooms (cross-site) are kept — their deletion
// isn't visible here — except under the window, which requires a dated room.
// Window membership is fresh: resolveSortKeys re-reads any cache hit that
// fails the cutoff before the row gets here.
func buildListRows(subs []subLite, keys map[string]roomSortKey, account string, favorite bool, cutoff *time.Time) []listRow {
	rows := make([]listRow, 0, len(subs))
	for i := range subs {
		key := keys[subs[i].RoomID]
		if !key.Missing && strings.HasPrefix(key.Name, deletedRoomNamePrefix) {
			continue
		}
		if cutoff != nil && (key.LastMsgAt == nil || key.LastMsgAt.Before(*cutoff)) {
			continue
		}
		sortAt := key.LastMsgAt
		if sortAt == nil {
			sortAt = key.CreatedAt
		}
		rows = append(rows, listRow{
			sub:    subs[i],
			sortAt: sortAt,
			selfDM: favorite && subs[i].RoomType == model.RoomTypeDM && subs[i].Name == account,
		})
	}
	return rows
}

// sortListRows orders rows the way the old in-Mongo sort did: pinned self-DM
// first (favorite view), then activity desc with nil keys last, then name asc.
func sortListRows(rows []listRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		if a.selfDM != b.selfDM {
			return a.selfDM
		}
		switch {
		case a.sortAt != nil && b.sortAt != nil:
			if !a.sortAt.Equal(*b.sortAt) {
				return a.sortAt.After(*b.sortAt)
			}
		case a.sortAt != nil:
			return true
		case b.sortAt != nil:
			return false
		}
		return a.sub.Name < b.sub.Name
	})
}

// fillListPage walks the sorted candidates from the page offset, freshly
// enriching limit+1-sized batches (mirrors mongoutil.AggregatePagedHasMore's
// over-read) and refilling from later candidates whenever the fresh read drops
// a row (room turned Del- after its sort key was cached, or the sub was
// deleted between phases) — so the page and hasMore always describe the live
// sequence, never a short page with a dangling hasMore whose next offset would
// skip live rows. Steady state is the usual single batch; refill rounds read
// full page-sized batches so k dropped rows cost ~k/limit extra reads, not k.
func (r *SubscriptionRepo) fillListPage(ctx context.Context, rows []listRow, page mongoutil.OffsetPageRequest) ([]model.EnrichedSubscription, bool, error) {
	// Degrade non-normalized values the way the old Mongo-side $skip/$limit
	// path did (no panic): a negative offset reads from the start, a negative
	// limit behaves like limit 0 (empty page, hasMore from the over-read).
	offset := max(page.Offset, 0)
	limit := max(page.Limit, 0)
	candidates := rows[min(offset, int64(len(rows))):]
	// minFillBatch keeps a degenerate limit (0 after clamping) from turning a
	// long freshly-dead candidate prefix into one enrich round trip per row.
	const minFillBatch = 32
	// Cap the over-read at the candidate count: collected can never exceed it,
	// and the cap keeps limit+1 from overflowing (and the slice capacity sane)
	// for a pathological MaxInt64 limit.
	need := min(limit, int64(len(candidates))) + 1
	collected := make([]model.EnrichedSubscription, 0, need)
	for len(candidates) > 0 && int64(len(collected)) < need {
		take := min(max(need, minFillBatch), int64(len(candidates)))
		batch, err := r.enrichListRows(ctx, candidates[:take])
		if err != nil {
			return nil, false, err
		}
		collected = append(collected, batch...)
		candidates = candidates[take:]
	}
	hasMore := int64(len(collected)) > limit
	if hasMore {
		collected = collected[:limit]
	}
	return collected, hasMore, nil
}

// resolveSortKeys returns the roomSortKey for every sub's room, serving from
// the cache and batching all misses into a single projected $in read. A cache
// hit that fails the activity window (cutoff) is demoted to a miss and
// re-read: lastMsgAt only ever advances, so hits that pass the window are
// provably still in it, while a failing hit may have gone stale the moment
// the room got its first in-window message — window MEMBERSHIP must not lag
// the TTL the way ordering may. Rooms absent from the batched read are
// negative-cached as Missing so cross-site rooms don't re-probe Mongo on
// every list (Missing hits are not demoted: there is no local doc to re-read,
// and the old join dropped them under a window too).
func (r *SubscriptionRepo) resolveSortKeys(ctx context.Context, subs []subLite, cutoff *time.Time) (map[string]roomSortKey, error) {
	keys := make(map[string]roomSortKey, len(subs))
	// The unique (roomId, u.account) index makes one account's roomIDs distinct
	// already — no dedup needed.
	var misses []string
	for i := range subs {
		id := subs[i].RoomID
		k, ok := r.sortKeys.get(ctx, id)
		if ok && cutoff != nil && !k.Missing && (k.LastMsgAt == nil || k.LastMsgAt.Before(*cutoff)) {
			ok = false
		}
		if ok {
			keys[id] = k
		} else {
			misses = append(misses, id)
		}
	}
	if len(misses) == 0 {
		return keys, nil
	}
	cur, err := r.rooms.Find(ctx, bson.M{"_id": bson.M{"$in": misses}},
		options.Find().SetProjection(bson.M{"name": 1, "lastMsgAt": 1, "createdAt": 1}))
	if err != nil {
		return nil, fmt.Errorf("find room sort keys: %w", err)
	}
	var docs []struct {
		ID        string     `bson:"_id"`
		Name      string     `bson:"name"`
		LastMsgAt *time.Time `bson:"lastMsgAt"`
		CreatedAt *time.Time `bson:"createdAt"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode room sort keys: %w", err)
	}
	for i := range docs {
		k := roomSortKey{Name: docs[i].Name, LastMsgAt: docs[i].LastMsgAt, CreatedAt: docs[i].CreatedAt}
		keys[docs[i].ID] = k
		r.sortKeys.add(docs[i].ID, k)
	}
	for _, id := range misses {
		if _, ok := keys[id]; !ok {
			k := roomSortKey{Missing: true}
			keys[id] = k
			r.sortKeys.add(id, k)
		}
	}
	return keys, nil
}

// roomBaseline is the fresh page-sized room read backing enrichListRows —
// the same field set roomsEnrichStages projects through its $lookup. Its bson
// tags and roomBaselineProjection are pinned to each other by
// TestRoomBaselineProjection_MatchesStructTags.
type roomBaseline struct {
	ID                string     `bson:"_id"`
	Name              string     `bson:"name"`
	UserCount         int        `bson:"userCount"`
	AppCount          int        `bson:"appCount"`
	LastMsgAt         *time.Time `bson:"lastMsgAt"`
	LastMsgID         string     `bson:"lastMsgId"`
	LastMentionAllAt  *time.Time `bson:"lastMentionAllAt"`
	MinUserLastSeenAt *time.Time `bson:"minUserLastSeenAt"`
	CreatedAt         *time.Time `bson:"createdAt"`
	CrossSite         *bool      `bson:"crossSite"`
	EncKey            struct {
		Priv []byte `bson:"priv"`
		Ver  int    `bson:"ver"`
	} `bson:"encKey"`
}

// roomBaselineProjection is the fresh page read's projection — exactly
// roomBaseline's fields, pinned by TestRoomBaselineProjection_MatchesStructTags.
func roomBaselineProjection() bson.M {
	return bson.M{
		"name": 1, "userCount": 1, "appCount": 1, "lastMsgAt": 1, "lastMsgId": 1,
		"lastMentionAllAt": 1, "minUserLastSeenAt": 1, "createdAt": 1,
		"encKey.priv": 1, "encKey.ver": 1, "crossSite": 1,
	}
}

// enrichListRows serves one fill batch from two fresh page-sized $in reads:
// the room baselines (LOCAL rows; missing rooms — cross-site — stay
// zero-valued) and the full subscription documents, which only page rows ever
// pay for. Rows whose room turned soft-deleted since the sort keys were
// cached, or whose subscription was deleted since phase 1, are dropped
// (fillListPage refills the gap from later candidates); a subscription
// modified between phases serves its current state, matching the fresh-room
// philosophy. The fresh room values also refresh the sort-key cache.
func (r *SubscriptionRepo) enrichListRows(ctx context.Context, rows []listRow) ([]model.EnrichedSubscription, error) {
	out := make([]model.EnrichedSubscription, 0, len(rows))
	if len(rows) == 0 {
		return out, nil
	}
	roomIDs := make([]string, 0, len(rows))
	subIDs := make([]string, 0, len(rows))
	for i := range rows {
		roomIDs = append(roomIDs, rows[i].sub.RoomID)
		subIDs = append(subIDs, rows[i].sub.ID)
	}
	cur, err := r.rooms.Find(ctx, bson.M{"_id": bson.M{"$in": roomIDs}},
		options.Find().SetProjection(roomBaselineProjection()))
	if err != nil {
		return nil, fmt.Errorf("find room baselines: %w", err)
	}
	var docs []roomBaseline
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode room baselines: %w", err)
	}
	baselines := make(map[string]*roomBaseline, len(docs))
	for i := range docs {
		baselines[docs[i].ID] = &docs[i]
		r.sortKeys.add(docs[i].ID, roomSortKey{
			Name: docs[i].Name, LastMsgAt: docs[i].LastMsgAt, CreatedAt: docs[i].CreatedAt,
		})
	}
	subCur, err := r.subscriptions.Raw().Find(ctx, bson.M{"_id": bson.M{"$in": subIDs}},
		options.Find().SetProjection(subscriptionFieldsProjection()))
	if err != nil {
		return nil, fmt.Errorf("find subscription page rows: %w", err)
	}
	var fullSubs []model.Subscription
	if err := subCur.All(ctx, &fullSubs); err != nil {
		return nil, fmt.Errorf("decode subscription page rows: %w", err)
	}
	subByID := make(map[string]*model.Subscription, len(fullSubs))
	for i := range fullSubs {
		subByID[fullSubs[i].ID] = &fullSubs[i]
	}
	// Iterate the candidate rows (not cursor order) so the page keeps the
	// sorted sequence.
	for i := range rows {
		b, ok := baselines[rows[i].sub.RoomID]
		if ok && strings.HasPrefix(b.Name, deletedRoomNamePrefix) {
			continue
		}
		sub, found := subByID[rows[i].sub.ID]
		if !found {
			continue
		}
		var es model.EnrichedSubscription
		es.Subscription = *sub
		if ok {
			es.UserCount = b.UserCount
			es.AppCount = b.AppCount
			es.LastMsgAt = b.LastMsgAt
			es.LastMsgID = b.LastMsgID
			es.LastMentionAllAt = b.LastMentionAllAt
			es.MinUserLastSeenAt = b.MinUserLastSeenAt
			es.RoomName = b.Name
			es.CrossSite = b.CrossSite
			es.RoomKeyPriv = b.EncKey.Priv
			es.RoomKeyVer = b.EncKey.Ver
		}
		out = append(out, es)
	}
	return out, nil
}

// FindChannelsByMembers returns one page of the requester's channel subs whose room contains the requester and ALL given members (bots excluded by the ".bot" suffix), room.createdAt desc, plus a hasMore flag (over-fetch by one).
// The room match (roomMatchStages) runs first so the deleted/missing filter shrinks the set before the co-member self-join.
func (r *SubscriptionRepo) FindChannelsByMembers(ctx context.Context, account string, members []string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
	// allAccounts is the full set the room must contain: the requested members plus
	// the requester, deduped once (a duplicate member, or a member equal to the
	// requester, collapses here). Bots (".bot" accounts) are excluded in the co-member
	// join below, so a bot passed in members can never satisfy the match.
	allAccounts := dedupeStrings(append(append([]string{}, members...), account))
	pipeline := bson.A{
		bson.M{"$match": bson.M{"u.account": account, "roomType": "channel"}},
	}
	for _, st := range roomMatchStages() {
		pipeline = append(pipeline, st)
	}
	pipeline = append(pipeline,
		// Co-member self-join — NOT siteId-filtered (any local/federated sub counts),
		// projected to u.account only. Bots are excluded by the ".bot" account suffix.
		// allAccounts is $literal-wrapped so $-values read as literals, not field paths.
		bson.M{"$lookup": bson.M{
			"from": subscriptionsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$roomId", "$$rid"}},
					bson.M{"$not": bson.M{"$regexMatch": bson.M{"input": "$u.account", "regex": "\\.bot$"}}},
					bson.M{"$in": bson.A{"$u.account", bson.M{"$literal": allAccounts}}},
				}}}},
				bson.M{"$project": bson.M{"_id": 0, "u.account": 1}},
			},
			"as": "__members",
		}},
		// Require every account present: $all (subset) + $size (exact count). The unique
		// (roomId, u.account) index gives one row per account, so the mapped accounts are
		// already distinct — no $setUnion needed.
		bson.M{"$addFields": bson.M{"__memberAccounts": bson.M{"$map": bson.M{
			"input": "$__members", "as": "m", "in": "$$m.u.account",
		}}}},
		bson.M{"$match": bson.M{"__memberAccounts": bson.M{"$all": allAccounts, "$size": len(allAccounts)}}},
		// Copy the matched room's baseline to the top level (consumed by local enrichment).
		bson.M{"$addFields": bson.M{
			"userCount":         bson.M{"$first": "$" + matchedRoomField + ".userCount"},
			"lastMsgAt":         bson.M{"$first": "$" + matchedRoomField + ".lastMsgAt"},
			"lastMsgId":         bson.M{"$first": "$" + matchedRoomField + ".lastMsgId"},
			"lastMentionAllAt":  bson.M{"$first": "$" + matchedRoomField + ".lastMentionAllAt"},
			"minUserLastSeenAt": bson.M{"$first": "$" + matchedRoomField + ".minUserLastSeenAt"},
			"appCount":          bson.M{"$first": "$" + matchedRoomField + ".appCount"},
			"roomName":          bson.M{"$first": "$" + matchedRoomField + ".name"},
			"crossSite":         bson.M{"$first": "$" + matchedRoomField + ".crossSite"},
			// Room E2E key baseline (current slot) — folds the key read into this join.
			"encKeyPriv": bson.M{"$first": "$" + matchedRoomField + ".encKey.priv"},
			"encKeyVer":  bson.M{"$first": "$" + matchedRoomField + ".encKey.ver"},
		}},
		bson.M{"$sort": bson.D{{Key: matchedRoomField + ".createdAt", Value: -1}}},
		bson.D{{Key: "$project", Value: subscriptionProjection()}},
	)
	return r.enriched.AggregatePagedHasMore(ctx, pipeline, page)
}

// GetDMSubscription returns the requester's room-enriched DM sub with target plus the counterpart's HRInfo (cross-site ⇒ nil), or (nil, nil).
func (r *SubscriptionRepo) GetDMSubscription(ctx context.Context, account, target string) (*model.EnrichedDMSubscription, error) {
	pipeline := bson.A{
		bson.M{"$match": bson.M{"u.account": account, "name": target, "roomType": "dm"}},
		bson.M{"$limit": int64(1)}, // (account, name, roomType=dm) is unique — short-circuit defensively
	}
	pipeline = append(pipeline, roomsEnrichStages(false)...)
	pipeline = append(pipeline,
		bson.M{"$lookup": bson.M{"from": usersCollection, "localField": "name", "foreignField": "account", "as": "hrUser"}},
		bson.M{"$unwind": bson.M{"path": "$hrUser", "preserveNullAndEmptyArrays": true}},
		bson.M{"$addFields": bson.M{"hrInfo": bson.M{"$cond": bson.A{
			bson.M{"$ifNull": bson.A{"$hrUser", false}},
			bson.M{
				"account": "$hrUser.account",
				// HRInfo.Name carries the Chinese (native) name — User has no plain "name".
				"name":    "$hrUser.chineseName",
				"engName": "$hrUser.engName",
			},
			"$$REMOVE",
		}}}},
		bson.M{"$project": bson.M{"hrUser": 0}},
	)
	// r.enriched.Raw(): decodes into []model.EnrichedDMSubscription (stored sub + room baseline + hrInfo).
	cur, err := r.enriched.Raw().Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate dm subscription: %w", err)
	}
	var out []model.EnrichedDMSubscription
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode dm subscription: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return &out[0], nil
}

// GetSubscriptionByRoomID returns the requester's deleted-filtered sub for roomID, or (nil, nil); (account, roomId) is unique in practice.
func (r *SubscriptionRepo) GetSubscriptionByRoomID(ctx context.Context, account, roomID string) (*model.EnrichedSubscription, error) {
	pipeline := bson.A{bson.M{"$match": bson.M{"u.account": account, "roomId": roomID}}}
	pipeline = append(pipeline, roomsEnrichStages(false)...)
	pipeline = append(pipeline, bson.M{"$limit": int64(1)}) // (roomId, u.account) is unique — short-circuit defensively
	out, err := r.enriched.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate subscription by roomId: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return &out[0], nil
}

// activeSubscriptionFilter: non-muted dm/channel subs, or non-muted subscribed botDMs (the count
// endpoints' notion of active). Unlike the list endpoints, the count EXCLUDES muted subs — mute
// keeps a room visible in lists but out of the active/badge count.
func activeSubscriptionFilter(account string) bson.M {
	return bson.M{"u.account": account, "muted": bson.M{"$ne": true}, "$or": bson.A{
		bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}},
		bson.M{"roomType": "botDM", "isSubscribed": true},
	}}
}

// CountActiveSubscriptions counts the deleted-filtered active set via $count over the enriched pipeline (CountDocuments cannot see the join).
func (r *SubscriptionRepo) CountActiveSubscriptions(ctx context.Context, account string) (int, error) {
	pipeline := bson.A{bson.M{"$match": activeSubscriptionFilter(account)}}
	pipeline = append(pipeline, roomsEnrichStages(true)...)
	pipeline = append(pipeline, bson.M{"$count": "n"})
	cur, err := r.subscriptions.Raw().Aggregate(ctx, pipeline)
	if err != nil {
		return 0, fmt.Errorf("count active subscriptions: %w", err)
	}
	var out []struct {
		N int `bson:"n"`
	}
	if err := cur.All(ctx, &out); err != nil {
		return 0, fmt.Errorf("decode active subscription count: %w", err)
	}
	if len(out) == 0 {
		return 0, nil
	}
	return out[0].N, nil
}

// GetActiveSubscriptions returns the deleted-filtered active set used by the unread count, capped by limit.
func (r *SubscriptionRepo) GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]model.EnrichedSubscription, error) {
	pipeline := bson.A{bson.M{"$match": activeSubscriptionFilter(account)}}
	pipeline = append(pipeline, roomsEnrichStages(true)...)
	// MongoDB rejects $limit:0 — callers short-circuit zero; stay defensive here.
	if limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": int64(limit)})
	}
	return r.enriched.Aggregate(ctx, pipeline)
}

// GetAppSubscription returns the requester's botDM subscription for botName, or (nil, nil).
func (r *SubscriptionRepo) GetAppSubscription(ctx context.Context, account, botName string) (*model.Subscription, error) {
	return r.subscriptions.FindOne(ctx, bson.M{"u.account": account, "name": botName, "roomType": "botDM"})
}

// SetAppSubscribed updates isSubscribed/muted on the requester's botDM subscription.
func (r *SubscriptionRepo) SetAppSubscribed(ctx context.Context, account, botName string, subscribed, muted bool) error {
	if _, err := r.subscriptions.Raw().UpdateOne(ctx,
		bson.M{"u.account": account, "name": botName, "roomType": "botDM"},
		bson.M{"$set": bson.M{"isSubscribed": subscribed, "muted": muted}},
	); err != nil {
		return fmt.Errorf("update app subscription: %w", err)
	}
	return nil
}
