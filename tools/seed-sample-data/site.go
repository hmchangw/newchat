package main

import "github.com/hmchangw/chat/pkg/model"

// Site routing: which site's database each seeded row belongs in.
//
// Two rules apply, and mixing them up produces a dataset that looks seeded but
// renders an empty chat list for the remote user:
//
//   - Room-owned rows (rooms, room_members, messages, thread_rooms, room keys)
//     live in the database of the ROOM's home site.
//   - Subscriber-owned rows (subscriptions, thread_subscriptions, the Valkey
//     restricted-rooms cache) live in the database of the SUBSCRIBER's home
//     site, while still carrying the room's siteId. That field is how a service
//     tells local rows from cross-site rows within its own database — see
//     user-service/mongorepo/subscriptions.go:35.
//
// Users and hr_employee rows are deliberately not routed: the full directory is
// written to both databases so either portal can resolve any account and tell a
// client where its home site is.

// filterBySite keeps the items whose home site matches, using the caller's
// rule for deciding an item's home site.
func filterBySite[T any](items []T, site string, homeSite func(T) string) []T {
	out := make([]T, 0, len(items))
	for _, it := range items {
		if homeSite(it) == site {
			out = append(out, it)
		}
	}
	return out
}

// userSiteByAccount indexes the seeded users by account to their home site.
func userSiteByAccount() map[string]string {
	users := BuildUsers()
	out := make(map[string]string, len(users))
	for i := range users {
		out[users[i].Account] = users[i].SiteID
	}
	return out
}

// roomSiteByID indexes the seeded rooms by ID to their home site.
func roomSiteByID() map[string]string {
	rooms := BuildRooms()
	out := make(map[string]string, len(rooms))
	for i := range rooms {
		out[rooms[i].ID] = rooms[i].SiteID
	}
	return out
}

// Home-site accessors, one per fixture type, so call sites read as intent
// rather than as an inline closure. Each rebuilds roomSiteByID/userSiteByAccount
// per call; with a handful of seeded rooms that is not worth caching.
//
//nolint:gocritic // hugeParam: params are passed by value to match filterBySite's func(T) string shape
func roomHomeSite(r model.Room) string { return r.SiteID }

//nolint:gocritic // hugeParam: params are passed by value to match filterBySite's func(T) string shape
func messageHomeSite(m model.Message) string { return roomSiteByID()[m.RoomID] }

//nolint:gocritic // hugeParam: params are passed by value to match filterBySite's func(T) string shape
func memberHomeSite(m model.RoomMember) string { return roomSiteByID()[m.RoomID] }

//nolint:gocritic // hugeParam: params are passed by value to match filterBySite's func(T) string shape
func threadRoomHomeSite(tr model.ThreadRoom) string { return roomSiteByID()[tr.RoomID] }

func roomKeyHomeSite(e RoomKeyEntry) string { return roomSiteByID()[e.RoomID] }

//nolint:gocritic // hugeParam: params are passed by value to match filterBySite's func(T) string shape
func subscriptionHomeSite(s model.Subscription) string {
	return userSiteByAccount()[s.User.Account]
}

//nolint:gocritic // hugeParam: params are passed by value to match filterBySite's func(T) string shape
func threadSubscriptionHomeSite(ts model.ThreadSubscription) string {
	return userSiteByAccount()[ts.UserAccount]
}

func restrictedCacheHomeSite(e RestrictedCacheEntry) string {
	return userSiteByAccount()[e.Account]
}

// scopeToSite returns items unchanged when site is empty — that is the
// single-site seeding path, which writes every fixture into one database
// exactly as it did before per-site routing existed. A non-empty site
// filters to that site's rows.
func scopeToSite[T any](items []T, site string, homeSite func(T) string) []T {
	if site == "" {
		return items
	}
	return filterBySite(items, site, homeSite)
}

// resolveTarget picks the database and site to seed. The --mongo-db flag
// overrides MONGO_DB when non-empty. An empty --site is passed through
// unchanged: it means unfiltered, single-site seeding — a plain `make seed`
// behaves exactly as it did before per-site routing existed. Explicit
// per-site seeding (e.g. `make fed-seed`) always passes --site itself.
func resolveTarget(envDB, flagDB, flagSite string) (db, site string) {
	db = envDB
	if flagDB != "" {
		db = flagDB
	}
	return db, flagSite
}
