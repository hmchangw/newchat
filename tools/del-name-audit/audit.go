package main

import "strings"

// delPrefix is room-service's soft-delete rename marker ("Del-"+name). It is the
// same literal user-service filters on; this tool audits whether the room-side
// marker is mirrored onto every subscription's denormalized name.
const delPrefix = "Del-"

// roomRef is the projected rooms doc the audit scans.
type roomRef struct {
	ID   string `bson:"_id"`
	Name string `bson:"name"`
	Type string `bson:"type"`
}

// subRef is the projected subscriptions doc compared against its room.
type subRef struct {
	ID       string `bson:"_id"`
	Name     string `bson:"name"`
	RoomID   string `bson:"roomId"`
	RoomType string `bson:"roomType"`
	SiteID   string `bson:"siteId"`
}

func hasDelPrefix(name string) bool { return strings.HasPrefix(name, delPrefix) }

// typeStats counts agreement for one roomType, so a DM-only divergence (where the
// subscription name is the counterpart account, not the room name) stays visible
// instead of averaging away against channels.
type typeStats struct {
	Agree    int `json:"agree"`
	Disagree int `json:"disagree"`
}

// mismatch is one printable counter-example.
type mismatch struct {
	Kind     string `json:"kind"`
	RoomID   string `json:"roomId"`
	RoomName string `json:"roomName"`
	SubID    string `json:"subId"`
	SubName  string `json:"subName"`
	RoomType string `json:"roomType"`
	SiteID   string `json:"siteId"`
}

// report is the audit verdict. Agree/Disagree answer the forward question (does a
// Del- room's every subscription carry the prefix); FalsePositive answers the
// reverse (would filtering on the subscription name drop a live room).
type report struct {
	DelRooms        int                   `json:"delRooms"`
	RoomsWithNoSubs int                   `json:"roomsWithNoSubs"`
	SubsScanned     int                   `json:"subsScanned"`
	Agree           int                   `json:"agree"`
	Disagree        int                   `json:"disagree"`
	ByRoomType      map[string]*typeStats `json:"byRoomType"`
	DelNamedSubs    int                   `json:"delNamedSubs"`
	FalsePositive   int                   `json:"falsePositive"`
	Unverifiable    int                   `json:"unverifiable"`
	Samples         []mismatch            `json:"samples"`
}

// safeToMoveFilter reports whether the data supports replacing user-service's
// rooms $lookup with a subscription-name filter: every subscription of a Del-
// room must carry the prefix (no Disagree), and no Del--named subscription may
// point at a live room (no FalsePositive). Unverifiable rows (no local room doc)
// are cross-site and are excluded either way, so they do not block.
func (r *report) safeToMoveFilter() bool {
	return r.Disagree == 0 && r.FalsePositive == 0
}

// auditInput is the fetched, in-memory result set. Splitting the fetch from the
// classification keeps the verdict logic unit-testable without a live Mongo.
type auditInput struct {
	delRooms     []roomRef
	subsForRooms []subRef
	delNamedSubs []subRef
	// roomsForSubs resolves delNamedSubs' rooms; a missing key means the room has
	// no local doc (cross-site or hard-deleted).
	roomsForSubs map[string]roomRef
	sampleCap    int
}

// audit classifies in into a report, collecting up to sampleCap counter-examples.
func audit(in *auditInput) report {
	rep := report{
		DelRooms:     len(in.delRooms),
		DelNamedSubs: len(in.delNamedSubs),
		ByRoomType:   map[string]*typeStats{},
	}

	delRoomByID := make(map[string]roomRef, len(in.delRooms))
	for _, room := range in.delRooms {
		delRoomByID[room.ID] = room
	}
	seenRooms := make(map[string]struct{}, len(in.delRooms))

	for _, sub := range in.subsForRooms {
		room, ok := delRoomByID[sub.RoomID]
		if !ok {
			// Defensive: a sub returned for a room the scan did not list.
			continue
		}
		seenRooms[sub.RoomID] = struct{}{}
		rep.SubsScanned++
		stats := rep.ByRoomType[sub.RoomType]
		if stats == nil {
			stats = &typeStats{}
			rep.ByRoomType[sub.RoomType] = stats
		}
		if hasDelPrefix(sub.Name) {
			rep.Agree++
			stats.Agree++
			continue
		}
		rep.Disagree++
		stats.Disagree++
		rep.addSample(in.sampleCap, &mismatch{
			Kind: "disagree", RoomID: room.ID, RoomName: room.Name,
			SubID: sub.ID, SubName: sub.Name, RoomType: sub.RoomType, SiteID: sub.SiteID,
		})
	}
	rep.RoomsWithNoSubs = len(in.delRooms) - len(seenRooms)

	for _, sub := range in.delNamedSubs {
		room, ok := in.roomsForSubs[sub.RoomID]
		if !ok {
			rep.Unverifiable++
			continue
		}
		if hasDelPrefix(room.Name) {
			continue
		}
		rep.FalsePositive++
		rep.addSample(in.sampleCap, &mismatch{
			Kind: "falsePositive", RoomID: room.ID, RoomName: room.Name,
			SubID: sub.ID, SubName: sub.Name, RoomType: sub.RoomType, SiteID: sub.SiteID,
		})
	}
	return rep
}

// addSample appends m while the sample budget allows; cap <= 0 disables sampling.
func (r *report) addSample(limit int, m *mismatch) {
	if limit <= 0 || len(r.Samples) >= limit {
		return
	}
	r.Samples = append(r.Samples, *m)
}

// distinctRoomIDs returns the deduped roomId set of subs, preserving first-seen order.
func distinctRoomIDs(subs []subRef) []string {
	seen := make(map[string]struct{}, len(subs))
	out := make([]string, 0, len(subs))
	for _, sub := range subs {
		if _, ok := seen[sub.RoomID]; ok {
			continue
		}
		seen[sub.RoomID] = struct{}{}
		out = append(out, sub.RoomID)
	}
	return out
}

// chunk splits in into slices of at most size, bounding each $in query.
func chunk(in []string, size int) [][]string {
	if size <= 0 || len(in) == 0 {
		return nil
	}
	out := make([][]string, 0, (len(in)+size-1)/size)
	for start := 0; start < len(in); start += size {
		end := start + size
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[start:end])
	}
	return out
}
