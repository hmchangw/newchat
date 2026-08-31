package mongorepo

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/timeutil"
	"github.com/hmchangw/chat/user-service/models"
)

const subscriptionsCollection = "subscriptions"

// roomsCollection is the $lookup target for room enrichment; owned by room-service, referenced only by name.
const roomsCollection = "rooms"

// SubscriptionRepo is the Mongo implementation of service.SubscriptionRepository.
type SubscriptionRepo struct {
	subscriptions *mongoutil.Collection[model.Subscription]
	// enriched decodes the room-enriched aggregation results (stored sub + read-time
	// room baseline) over the same subscriptions collection; writes go through
	// subscriptions so the baseline fields are never persisted.
	enriched *mongoutil.Collection[model.EnrichedSubscription]
	// *Secondary route list/count reads; GetAppSubscription (dedup guard) keeps the
	// primary handle.
	subscriptionsSecondary *mongoutil.Collection[model.Subscription]
	enrichedSecondary      *mongoutil.Collection[model.EnrichedSubscription]
	// activeSecondary decodes the badge path's narrow projection over the same
	// subscriptions collection; routed to a secondary like the other read handles.
	activeSecondary *mongoutil.Collection[models.ActiveSubscription]
	// showTeamsRoom mirrors SHOW_TEAMS_ROOM: false (default) excludes
	// Teams-migrated rooms (origin "teams") from list/count results.
	showTeamsRoom bool
	// showTeamsAccounts (SHOW_TEAMS_ROOM_ACCOUNTS) allowlists accounts that see
	// Teams rooms even when showTeamsRoom is false.
	showTeamsAccounts map[string]bool
	// rooms backs the list path's batched reads: sort keys and page data. The
	// collection belongs to room-service; we only name it.
	rooms    *mongo.Collection
	sortKeys *sortKeyCache
}

// NewSubscriptionRepo builds a SubscriptionRepo over db. sortKeyCacheSize and
// sortKeyCacheTTL configure the sort-key cache; zero or less for either turns it
// off. Defaults live in the service config (SUBS_SORTKEY_CACHE_*).
func NewSubscriptionRepo(db *mongo.Database, sortKeyCacheSize int, sortKeyCacheTTL time.Duration, opts ...Option) *SubscriptionRepo {
	s := applyOptions(opts)
	col := db.Collection(subscriptionsCollection)
	subscriptions := mongoutil.NewCollection[model.Subscription](col)
	enriched := mongoutil.NewCollection[model.EnrichedSubscription](col)
	active := mongoutil.NewCollection[models.ActiveSubscription](col)
	return &SubscriptionRepo{
		subscriptions:          subscriptions,
		enriched:               enriched,
		subscriptionsSecondary: subscriptions.WithReadPreference(s.readPref),
		enrichedSecondary:      enriched.WithReadPreference(s.readPref),
		activeSecondary:        active.WithReadPreference(s.readPref),
		showTeamsRoom:          s.showTeamsRoom,
		showTeamsAccounts:      s.showTeamsAccounts,
		rooms:                  db.Collection(roomsCollection),
		sortKeys:               newSortKeyCache(sortKeyCacheSize, sortKeyCacheTTL),
	}
}

// EnsureIndexes creates the subscription indexes this service queries on.
func (r *SubscriptionRepo) EnsureIndexes(ctx context.Context) error {
	// subscriptions.{roomId,u.account} (unique) is owned by room-service; verify + warn only, never create.
	mongoutil.WarnMissingIndexes(ctx, r.subscriptions.Raw(), "roomId_1_u.account_1")
	if _, err := r.subscriptions.Raw().Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Serves the account+roomType match on every list/count path; the retention
		// window keys on room.lastMsgAt (a room field), so no trailing time key.
		{Keys: bson.D{{Key: "u.account", Value: 1}, {Key: "roomType", Value: 1}}},
		{Keys: bson.D{{Key: "name", Value: 1}, {Key: "roomType", Value: 1}}},
	}); err != nil {
		return fmt.Errorf("create subscription indexes: %w", err)
	}
	return nil
}

// roomsEnrichStages builds the shared rooms-join + enrichment. It never removes a
// subscription: a missing or cross-site room simply yields no room fields. The
// rooms-type activity window is applied separately by the caller on the room's
// lastMsgAt (surfaced here).
func roomsEnrichStages() bson.A {
	return bson.A{
		// Project only the room fields this enrichment surfaces (not the whole room doc) so
		// the join+sort working set stays lean; the correlated $expr/_id match uses the _id
		// index, same as roomMatchStages.
		bson.M{"$lookup": bson.M{
			"from": roomsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$_id", "$$rid"}}}},
				bson.M{"$project": bson.M{
					"name":              1,
					"userCount":         1,
					"appCount":          1,
					"lastMsgAt":         1,
					"lastUserMsgAt":     1,
					"lastMsgId":         1,
					"lastMentionAllAt":  1,
					"minUserLastSeenAt": 1,
					"createdAt":         1,
					"encKey.priv":       1,
					"encKey.ver":        1,
					"crossSite":         1,
				}},
			},
			"as": "room",
		}},
		bson.M{"$unwind": bson.M{"path": "$room", "preserveNullAndEmptyArrays": true}},
		bson.M{"$addFields": bson.M{
			"userCount":         "$room.userCount",
			"lastMsgAt":         "$room.lastMsgAt",
			"lastUserMsgAt":     "$room.lastUserMsgAt",
			"lastMsgId":         "$room.lastMsgId",
			"lastMentionAllAt":  "$room.lastMentionAllAt",
			"minUserLastSeenAt": "$room.minUserLastSeenAt",
			"appCount":          "$room.appCount",
			"roomName":          "$room.name",
			"crossSite":         "$room.crossSite",
			// origin is NOT set from room here — the subscription's own origin is kept
			// (reliable cross-site; a remote room's $room.origin is null). See originFilter.
			// Room E2E key baseline (current slot) for local enrichment — folds the
			// key read into this single $lookup, no separate keystore round-trip.
			"encKeyPriv": "$room.encKey.priv",
			"encKeyVer":  "$room.encKey.ver",
		}},
		bson.M{"$project": bson.M{"room": 0}},
	}
}

// matchedRoomField is the scratch array the member-match pipeline joins the local
// room into; stripped by subscriptionProjection before the result decodes.
const matchedRoomField = "__matchedRoom"

// roomMatchStages joins the local rooms collection into the matchedRoomField array,
// then drops any sub whose room is missing (empty array, via $ne: []). It runs
// BEFORE the heavier co-member self-join so the cheap room filter shrinks the
// candidate set first. Unlike roomsEnrichStages this DROPS missing/cross-site rooms
// (no local room doc ⇒ empty array): member matching is inherently local.
func roomMatchStages() []bson.D {
	return []bson.D{
		{{Key: "$lookup", Value: bson.M{
			"from": roomsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$_id", "$$rid"}}}},
				// Project only the fields FindChannelsByMembers copies out of
				// matchedRoomField (mirrors roomsEnrichStages) so the whole room doc —
				// including prior E2E key slots — doesn't transit the pipeline.
				bson.M{"$project": bson.M{
					"name":              1,
					"userCount":         1,
					"appCount":          1,
					"lastMsgAt":         1,
					"lastUserMsgAt":     1,
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

// subscriptionFieldsProjection names every stored subscription field, so a
// decoded model.Subscription comes back complete. The list path uses it for the
// page refetch in enrichListRows; the member-match pipeline goes through
// subscriptionProjection.
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
		// Which chatlist section a room sits in. The client reads these, and
		// TestSubscriptionFieldsProjection_MatchesModelTags fails if one is missed.
		"sectionId":        1,
		"sectionOrder":     1,
		"sectionUpdatedAt": 1,
	}
}

// subscriptionProjection is the last $project in the member-match pipeline: the
// subscription's fields plus the room data at the top level. Naming only what to
// keep drops the scratch arrays (__matchedRoom, __members, __memberAccounts).
func subscriptionProjection() bson.M {
	proj := subscriptionFieldsProjection()
	for _, k := range []string{
		"userCount", "lastMsgAt", "lastUserMsgAt", "lastMsgId", "lastMentionAllAt",
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

// AggregateSubscriptions returns one page of account's subscriptions for
// listType (rooms = dm+channel, apps = subscribed botDMs, current = both),
// newest activity first — the room's lastMsgAt, or its createdAt when the room
// has no messages — plus a hasMore flag from reading one row extra. favorite narrows to favorited rows
// and pins the caller's self-DM first; withinDays limits the rooms type by the
// room's lastMsgAt and is ignored elsewhere.
//
// Rather than joining every subscription to its room and sorting that in Mongo,
// the read runs in four steps:
//
//  1. One indexed read of the matching subscriptions, cut to the fields used to
//     filter and sort: _id, roomId, roomType, name.
//  2. Each room's sort key from the in-process cache, misses batched into one
//     $in read.
//  3. Deleted-room check, activity window, sort and paging, all in Go.
//  4. Two reads sized to the page: its rooms and its full subscription
//     documents. Only rows that made the page are fetched in full.
//
// Only the ordering may lag, by up to the cache TTL. A cached key that falls
// outside the window is re-read first, and the fresh room read drops any room
// soft-deleted meanwhile.
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
	// The Teams exclusion goes in this filter rather than a pipeline stage,
	// because this path uses Find, not an aggregation.
	if f := r.originFilter(account); f != nil {
		match["origin"] = f
	}
	// All primary: the list must show a subscription the caller just changed,
	// and the page's rows come from fresh room reads. Only sort keys may lag.
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
	data, hasMore, err := r.fillListPage(ctx, match, rows, page)
	if err != nil {
		return zero, err
	}
	return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: data, HasMore: hasMore}, nil
}

// originFilter excludes Teams-migrated rooms (origin "teams") for account unless
// SHOW_TEAMS_ROOM is true or account is allowlisted; nil means "no restriction".
// Filters on the subscription's own origin field (reliable cross-site — a remote
// room has no local doc), so callers merge it into the match they already build.
func (r *SubscriptionRepo) originFilter(account string) bson.M {
	if r.showTeamsRoom || r.showTeamsAccounts[account] {
		return nil
	}
	return bson.M{"$ne": model.OriginTeams}
}

// subLite is the small set of fields the steps before paging need: _id to
// refetch the page, roomId to look up the sort key, roomType and name for the
// self-DM pin and the name tiebreak.
type subLite struct {
	ID       string         `bson:"_id"`
	RoomID   string         `bson:"roomId"`
	RoomType model.RoomType `bson:"roomType"`
	Name     string         `bson:"name"`
}

// subscriptionLiteProjection fetches exactly subLite's fields for the first
// read. Full documents are refetched a page at a time, so a request holds one
// page of them plus one small row per subscription.
func subscriptionLiteProjection() bson.M {
	return bson.M{"_id": 1, "roomId": 1, "roomType": 1, "name": 1}
}

// listWindowCutoff returns the withinDays activity cutoff for the rooms list
// type, or nil when no window applies — the other types ignore withinDays.
func listWindowCutoff(listType string, withinDays *int) *time.Time {
	if listType != "rooms" || withinDays == nil {
		return nil
	}
	c := time.Now().UTC().AddDate(0, 0, -*withinDays)
	return &c
}

// listRow pairs a fetched subscription with the position it sorts to.
type listRow struct {
	sub subLite
	// sortAt is the room's user-activity position: lastUserMsgAt, falling back
	// to lastMsgAt, falling back to createdAt when the room is undated. nil —
	// room missing, or neither date — sorts after every dated row, as the old
	// Mongo sort did.
	sortAt *time.Time
	selfDM bool // favorite view only: pins the caller's self-DM first
}

// effectiveUserAt is the ordering/window position: the last USER message,
// falling back to lastMsgAt for rooms that predate lastUserMsgAt. Never
// createdAt — that stays buildListRows' final undated fallback only.
func effectiveUserAt(k roomSortKey) *time.Time {
	return timeutil.Coalesce(k.LastUserMsgAt, k.LastMsgAt)
}

// buildListRows drops rows that shouldn't appear — rooms soft-deleted here, and
// rooms outside the activity window — and works out where the rest sort. A room
// owned by another site has no local document; it is kept, since its deletion
// isn't visible here, but a window drops it because that needs a date. Window
// membership is current: resolveSortKeys re-reads keys that miss the cutoff.
// Both the window and the sort key key off user activity (effectiveUserAt), so
// a system-only bump (e.g. a rename) can't resurface a dormant room or push it
// above one with real recent conversation.
func buildListRows(subs []subLite, keys map[string]roomSortKey, account string, favorite bool, cutoff *time.Time) []listRow {
	rows := make([]listRow, 0, len(subs))
	for i := range subs {
		key := keys[subs[i].RoomID]
		at := effectiveUserAt(key)
		if cutoff != nil && (at == nil || at.Before(*cutoff)) {
			continue
		}
		sortAt := at
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

// sortListRows orders rows as the old Mongo sort did: pinned self-DM first in
// the favorite view, then newest activity, undated last, then by name. The
// subscription _id breaks a remaining tie, so separate page requests read the
// same sequence — a stable sort would otherwise fall back to the phase-one
// Find's arbitrary order and let a page boundary repeat one row and drop
// another.
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
		if a.sub.Name != b.sub.Name {
			return a.sub.Name < b.sub.Name
		}
		return a.sub.ID < b.sub.ID
	})
}

// fillListPage builds the page by walking the sorted rows from the offset and
// reading them in batches of one more than the page holds — the same over-read
// mongoutil.AggregatePagedHasMore does. When a fresh read drops a row — the
// subscription was deleted, or it stopped matching the list's filter — the next
// candidate takes its place, so the page and hasMore describe rows that exist rather than
// a short page whose hasMore skips live rows. Refills read a full batch each
// round, so k drops cost about k/limit extra reads, not k.
func (r *SubscriptionRepo) fillListPage(ctx context.Context, match bson.M, rows []listRow, page mongoutil.OffsetPageRequest) ([]model.EnrichedSubscription, bool, error) {
	// Handle out-of-range values as the old Mongo $skip/$limit did, without
	// panicking: a negative offset reads from the start, a negative limit acts
	// like zero — empty page, hasMore from the over-read.
	offset := max(page.Offset, 0)
	limit := max(page.Limit, 0)
	candidates := rows[min(offset, int64(len(rows))):]
	// Cap the over-read at the candidate count: we can never collect more, and
	// it keeps limit+1 from overflowing on a limit like MaxInt64.
	need := min(limit, int64(len(candidates))) + 1
	collected := make([]model.EnrichedSubscription, 0, need)
	for len(candidates) > 0 && int64(len(collected)) < need {
		take := fillBatchSize(need, int64(len(collected)), int64(len(candidates)))
		batch, err := r.enrichListRows(ctx, match, candidates[:take])
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

// minFillBatch keeps a limit of zero from turning a long run of just-deleted
// candidates into one round trip per row.
const minFillBatch = 32

// fillBatchSize sizes a round from how many rows are still missing rather than
// the whole page, so topping up after a few drops costs a small read. Never
// more than the candidates left.
func fillBatchSize(need, collected, candidates int64) int64 {
	return min(max(need-collected, minFillBatch), candidates)
}

// resolveSortKeys returns a sort key for every subscription's room, serving what
// it can from the cache and batching the rest into one projected $in read.
//
// A cached key outside the activity window is treated as a miss and read again.
// lastMsgAt only moves forward, so a key that passes the window is still in it,
// but one that fails may have just gone stale. Ordering may lag the TTL;
// whether a room appears at all may not.
//
// Rooms the read doesn't return are cached as Missing, so rooms owned by another
// site aren't queried on every list. Those are never re-read — there is no local
// document to find, and the old join dropped them under a window too.
func (r *SubscriptionRepo) resolveSortKeys(ctx context.Context, subs []subLite, cutoff *time.Time) (map[string]roomSortKey, error) {
	keys := make(map[string]roomSortKey, len(subs))
	// An account holds at most one subscription per room (unique index on roomId
	// and u.account), so these IDs are already distinct.
	var misses []string
	for i := range subs {
		id := subs[i].RoomID
		k, ok := r.sortKeys.get(ctx, id)
		if ok && cutoff != nil && !k.Missing {
			if at := effectiveUserAt(k); at == nil || at.Before(*cutoff) {
				ok = false
			}
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
		options.Find().SetProjection(bson.M{"lastMsgAt": 1, "lastUserMsgAt": 1, "createdAt": 1}))
	if err != nil {
		return nil, fmt.Errorf("find room sort keys: %w", err)
	}
	var docs []struct {
		ID            string     `bson:"_id"`
		LastMsgAt     *time.Time `bson:"lastMsgAt"`
		LastUserMsgAt *time.Time `bson:"lastUserMsgAt"`
		CreatedAt     *time.Time `bson:"createdAt"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode room sort keys: %w", err)
	}
	for i := range docs {
		k := roomSortKey{LastUserMsgAt: docs[i].LastUserMsgAt, LastMsgAt: docs[i].LastMsgAt, CreatedAt: docs[i].CreatedAt}
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

// roomBaseline is the room data the page read fetches — the same fields
// roomsEnrichStages pulls through its $lookup.
// TestRoomBaselineProjection_MatchesStructTags keeps it and the projection in step.
type roomBaseline struct {
	ID                string     `bson:"_id"`
	Name              string     `bson:"name"`
	UserCount         int        `bson:"userCount"`
	AppCount          int        `bson:"appCount"`
	LastMsgAt         *time.Time `bson:"lastMsgAt"`
	LastUserMsgAt     *time.Time `bson:"lastUserMsgAt"`
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

// roomBaselineProjection is the page read's projection: exactly roomBaseline's
// fields, checked by TestRoomBaselineProjection_MatchesStructTags.
func roomBaselineProjection() bson.M {
	return bson.M{
		"name": 1, "userCount": 1, "appCount": 1, "lastMsgAt": 1, "lastUserMsgAt": 1, "lastMsgId": 1,
		"lastMentionAllAt": 1, "minUserLastSeenAt": 1, "createdAt": 1,
		"encKey.priv": 1, "encKey.ver": 1, "crossSite": 1,
	}
}

// enrichListRows builds one batch of the page from two reads sized to it: the
// rooms, and the full subscription documents. Only rows that reach the page pay
// for a full document, and a room owned by another site has none, so its room
// fields stay empty.
//
// A row drops out when its room was soft-deleted after the key was cached, when
// its subscription was deleted, or when the subscription no longer matches the
// list's filter — the refetch applies that filter again, so an un-favorited or
// closed row leaves rather than showing state that doesn't belong. fillListPage
// refills the gap. The room values also refresh the sort-key cache.
func (r *SubscriptionRepo) enrichListRows(ctx context.Context, match bson.M, rows []listRow) ([]model.EnrichedSubscription, error) {
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
			LastUserMsgAt: docs[i].LastUserMsgAt, LastMsgAt: docs[i].LastMsgAt, CreatedAt: docs[i].CreatedAt,
		})
	}
	// Apply the list's filter again rather than just fetching the page's _ids: a
	// subscription that stopped matching since the first read — un-favorited, or
	// closed — should drop out instead of serving state that no longer matches.
	subCur, err := r.subscriptions.Raw().Find(ctx,
		bson.M{"$and": bson.A{match, bson.M{"_id": bson.M{"$in": subIDs}}}},
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
	// Walk the candidate rows, not the cursor, so the page keeps its sorted order.
	for i := range rows {
		b, ok := baselines[rows[i].sub.RoomID]
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
			es.LastUserMsgAt = b.LastUserMsgAt
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
// The room match (roomMatchStages) runs first so the missing-room filter shrinks the set before the co-member self-join.
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
			"lastUserMsgAt":     bson.M{"$first": "$" + matchedRoomField + ".lastUserMsgAt"},
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
	// Primary: the subscription list must reflect a just-changed subscription immediately.
	return r.enriched.AggregatePagedHasMore(ctx, pipeline, page)
}

// GetDMSubscription returns the requester's room-enriched DM sub with target plus the counterpart's HRInfo (cross-site ⇒ nil), or (nil, nil).
func (r *SubscriptionRepo) GetDMSubscription(ctx context.Context, account, target string) (*model.EnrichedDMSubscription, error) {
	pipeline := bson.A{
		bson.M{"$match": bson.M{"u.account": account, "name": target, "roomType": "dm"}},
		bson.M{"$limit": int64(1)}, // (account, name, roomType=dm) is unique — short-circuit defensively
	}
	pipeline = append(pipeline, roomsEnrichStages()...)
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
	cur, err := r.enrichedSecondary.Raw().Aggregate(ctx, pipeline)
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

// GetSubscriptionByRoomID returns the requester's room-enriched sub for roomID, or (nil, nil); (account, roomId) is unique in practice.
func (r *SubscriptionRepo) GetSubscriptionByRoomID(ctx context.Context, account, roomID string) (*model.EnrichedSubscription, error) {
	pipeline := bson.A{bson.M{"$match": bson.M{"u.account": account, "roomId": roomID}}}
	pipeline = append(pipeline, roomsEnrichStages()...)
	pipeline = append(pipeline, bson.M{"$limit": int64(1)}) // (roomId, u.account) is unique — short-circuit defensively
	out, err := r.enrichedSecondary.Aggregate(ctx, pipeline)
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
	return bson.M{"u.account": account, "muted": bson.M{"$ne": true},
		// Rooms the user closed are hidden from subscription.list, so counting
		// them here would put the two endpoints permanently out of step — and
		// a client folding its badge from the list could never reconcile.
		// Missing field (legacy docs) and open:true both pass, as in the list.
		"open": bson.M{"$ne": false},
		"$or": bson.A{
			bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}},
			bson.M{"roomType": "botDM", "isSubscribed": true},
		}}
}

// activeFilter is the active set plus the Teams-origin exclusion as one flat filter.
// Both the total count and the unread path's leading $match use it, so the two
// cannot select different rows. The returned map is freshly built and safe to mutate.
func (r *SubscriptionRepo) activeFilter(account string) bson.M {
	filter := activeSubscriptionFilter(account)
	if origin := r.originFilter(account); origin != nil {
		filter["origin"] = origin
	}
	return filter
}

// activeSubscriptionProjection is the terminal $project: exactly the fields models.ActiveSubscription decodes.
func activeSubscriptionProjection() bson.M {
	return bson.M{
		"_id":           0,
		"roomId":        1,
		"siteId":        1,
		"lastSeenAt":    1,
		"threadUnread":  1,
		"lastMsgAt":     1,
		"lastUserMsgAt": 1,
	}
}

// CountActiveSubscriptions counts the active set with a plain CountDocuments —
// subscription state only, no room lookup. Room names carry no meaning here: no
// name excludes a subscription from the count.
func (r *SubscriptionRepo) CountActiveSubscriptions(ctx context.Context, account string) (int, error) {
	n, err := r.subscriptionsSecondary.Raw().CountDocuments(ctx, r.activeFilter(account))
	if err != nil {
		return 0, fmt.Errorf("count active subscriptions: %w", err)
	}
	return int(n), nil
}

// activeSubscriptionPipeline builds the pipeline GetActiveSubscriptions runs. The
// raw-BSON guard runs this same builder, so a dropped $project fails a test.
func (r *SubscriptionRepo) activeSubscriptionPipeline(account string, limit int) bson.A {
	pipeline := bson.A{bson.M{"$match": r.activeFilter(account)}}
	// MongoDB rejects $limit:0 — treat it as "no cap".
	if limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": int64(limit)})
	}
	pipeline = append(pipeline, activeRoomsEnrichStages()...)
	pipeline = append(pipeline, bson.M{"$project": activeSubscriptionProjection()})
	return pipeline
}

// activeRoomsEnrichStages is the badge path's rooms join. It deliberately does NOT
// reuse roomsEnrichStages: the badge count reads exactly one room field, so joining
// the list path's eleven (the encKey blob among them) would materialize ten per
// joined room for the terminal $project to discard — once per account in a
// notification batch. A cross-site subscription has no local room document and,
// as in the list path, simply yields no lastMsgAt.
func activeRoomsEnrichStages() bson.A {
	return bson.A{
		// $lookup justification: the unread test compares the room's lastMsgAt
		// against the subscription's own lastSeenAt, so both sides must meet
		// somewhere. Composing app-side would add a second round trip per account
		// — an $in over the whole active set — on a path that already runs once
		// per account in a notification batch. The $limit above caps the join at
		// maxSubs rows, and the correlated $expr matches on the rooms _id index.
		// The cross-site half of this same computation necessarily uses the
		// separate-query shape (GetRoomsMeta), there being no local room document.
		bson.M{"$lookup": bson.M{
			"from": roomsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$_id", "$$rid"}}}},
				bson.M{"$project": bson.M{"_id": 0, "lastMsgAt": 1}},
			},
			"as": "room",
		}},
		bson.M{"$unwind": bson.M{"path": "$room", "preserveNullAndEmptyArrays": true}},
		bson.M{"$addFields": bson.M{"lastMsgAt": "$room.lastMsgAt"}},
	}
}

// GetActiveSubscriptions returns the active set for the unread count, capped before the join.
func (r *SubscriptionRepo) GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]models.ActiveSubscription, error) {
	return r.activeSecondary.Aggregate(ctx, r.activeSubscriptionPipeline(account, limit))
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
