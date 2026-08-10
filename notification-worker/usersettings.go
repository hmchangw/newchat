package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// UserSettingsSnapshotter batches notification-settings lookups for push-eligible
// accounts. Errors are swallowed; an absent account defaults to current behaviour.
type UserSettingsSnapshotter interface {
	Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error)
}

// notifSettings is the resolved, pointer-free view of the three settings this
// worker gates on. The stored type is all *bool; resolving once at this boundary
// keeps the gate total instead of making every read site re-decide what nil means.
//
// The zero value is exactly pre-enforcement behaviour — not muted, no pierce,
// in-call suppressed. That is what makes fail-open free: a missing user, an unset
// settings sub-document, a Mongo error and the kill switch all converge here.
type notifSettings struct {
	muteAll          bool
	allowPriority    bool
	showInCall       bool
	priorityContacts map[string]struct{}
}

// isPriority reports whether account is one of this recipient's priority contacts.
// Decoded to a set at the snapshot boundary so the gate is O(1) per candidate.
func (n notifSettings) isPriority(account string) bool {
	if account == "" {
		return false
	}
	_, ok := n.priorityContacts[account]
	return ok
}

// noopUserSettings returns an empty map so every recipient takes the zero
// notifSettings. Backs USER_SETTINGS_ENABLED=false, mirroring noopPresenceSnapshotter.
type noopUserSettings struct{}

func (noopUserSettings) Snapshot(context.Context, []string) (map[string]notifSettings, error) {
	return map[string]notifSettings{}, nil
}

// userSettingsProjection takes only the four gated fields. Deliberately NOT the
// whole-sub-document {"settings":1} projection user-service's fanouts need —
// nothing here re-publishes the settings object.
var userSettingsProjection = bson.M{
	"_id":                           0,
	"account":                       1,
	"settings.muteAllNotifications": 1,
	"settings.alwaysAllowPriorityNotifications": 1,
	"settings.showNotificationsInCall":          1,
	"settings.priorityContacts":                 1,
}

// mongoUserSettings reads notification settings straight from the users
// collection — one indexed $in per chunk, no cache. Per-user keys make a Valkey
// tier strictly worse than this (one round trip per candidate, and per-account
// keys cross cluster slots), so there is no L2 here by design.
type mongoUserSettings struct {
	col       *mongo.Collection
	batchSize int
	timeout   time.Duration
}

func newMongoUserSettings(col *mongo.Collection, batchSize int, timeout time.Duration) *mongoUserSettings {
	if batchSize <= 0 {
		batchSize = 512
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &mongoUserSettings{col: col, batchSize: batchSize, timeout: timeout}
}

// Snapshot returns settings keyed by account. It never returns an error: a failed
// or timed-out read yields whatever was collected so far, and absent accounts take
// the zero notifSettings, i.e. today's behaviour. See the spec on why this gate
// fails open like its two neighbours rather than silencing the site.
func (m *mongoUserSettings) Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error) {
	out := make(map[string]notifSettings, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	// Bound the new dependency rather than inheriting the consumer's deadline.
	qctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	// Chunks run sequentially under one shared timeout, unlike bulkPresenceSource's
	// concurrent fan-out — deliberate, since this read is a single indexed $in and
	// not worth a goroutine per chunk. Consequence: if the shared timeout expires
	// mid-loop, chunks nearer the end of a very large recipient list are the ones
	// that never get read and fail open.
	for _, chunk := range chunkStrings(accounts, m.batchSize) {
		if err := m.appendChunk(qctx, chunk, out); err != nil {
			slog.Warn("user settings lookup failed, defaulting to push",
				"error", err, "chunk", len(chunk), "request_id", natsutil.RequestIDFromContext(ctx))
			return out, nil
		}
	}
	return out, nil
}

func (m *mongoUserSettings) appendChunk(ctx context.Context, chunk []string, out map[string]notifSettings) error {
	// active:{$ne:false} matches activeUserFilter in user-service so this read
	// agrees with user-service about what an active user is.
	filter := bson.M{"account": bson.M{"$in": chunk}, "active": bson.M{"$ne": false}}
	cur, err := m.col.Find(ctx, filter, options.Find().SetProjection(userSettingsProjection))
	if err != nil {
		return fmt.Errorf("find user settings: %w", err)
	}
	// Close error intentionally discarded: the cursor is being abandoned either
	// way, and Snapshot is fail-open by contract, so a close failure here
	// changes nothing observable to the caller.
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var doc struct {
			Account  string              `bson:"account"`
			Settings *model.UserSettings `bson:"settings"`
		}
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode user settings: %w", err)
		}
		if doc.Account == "" {
			continue
		}
		out[doc.Account] = resolveNotifSettings(doc.Settings)
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate user settings: %w", err)
	}
	return nil
}

// resolveNotifSettings collapses the stored pointer fields into a total value and
// decodes priorityContacts into a set, so the gate never dereferences a nil.
func resolveNotifSettings(s *model.UserSettings) notifSettings {
	if s == nil {
		return notifSettings{}
	}
	ns := notifSettings{
		muteAll:       boolValue(s.MuteAllNotifications),
		allowPriority: boolValue(s.AlwaysAllowPriorityNotifications),
		showInCall:    boolValue(s.ShowNotificationsInCall),
	}
	if len(s.PriorityContacts) > 0 {
		ns.priorityContacts = make(map[string]struct{}, len(s.PriorityContacts))
		for _, a := range s.PriorityContacts {
			if a != "" {
				ns.priorityContacts[a] = struct{}{}
			}
		}
	}
	return ns
}

// boolValue treats an unset pointer as false — an absent setting means the user
// never enabled it.
func boolValue(p *bool) bool { return p != nil && *p }
