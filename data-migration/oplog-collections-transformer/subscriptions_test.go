package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

func subEv(op, doc, updateDesc string) oplogEvent {
	ev := oplogEvent{Op: op, Collection: subsColl, EventID: "se1"}
	if doc != "" {
		ev.FullDocument = json.RawMessage(doc)
	}
	if updateDesc != "" {
		ev.UpdateDescription = json.RawMessage(updateDesc)
	}
	ev.DocumentKey = json.RawMessage(`{"_id":"sub1"}`)
	return ev
}

// eventsByType groups the published events by their InboxEvent.Type.
func eventsByType(evts []model.InboxEvent) map[model.InboxEventType]model.InboxEvent {
	m := make(map[model.InboxEventType]model.InboxEvent, len(evts))
	for _, e := range evts {
		m[e.Type] = e
	}
	return m
}

// A full source subscription doc: owner role, muted, favorited, alert, ls < lr.
const fullSubDoc = `{
	"_id":"sub1",
	"u":{"_id":"u1","username":"alice"},
	"rid":"r1",
	"t":"c",
	"name":"general",
	"fname":"General",
	"roles":["owner"],
	"open":true,
	"f":true,
	"disableNotifications":true,
	"alert":true,
	"ls":{"$date":"2024-01-15T09:00:00.000Z"},
	"lr":{"$date":"2024-01-15T10:00:00.000Z"},
	"ts":{"$date":"2024-01-01T00:00:00.000Z"},
	"_updatedAt":{"$date":"2024-01-20T00:00:00.000Z"}
}`

// updatedAtMillis is the source _updatedAt for fullSubDoc (2024-01-20T00:00:00Z) — the high-water
// mark the mute/favorite/role guards must stamp, stable across redelivery.
const updatedAtMillis = int64(1705708800000)

// lrMillis is max(ls,lr) for fullSubDoc — lr (10:00Z) is later than ls (09:00Z).
const lrMillis = int64(1705312800000) // 2024-01-15T10:00:00Z
// tsMillis is the JoinedAt for fullSubDoc.
const tsMillis = int64(1704067200000) // 2024-01-01T00:00:00Z

func TestHandleSubscription_Insert_AllEvents(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	err := h.handleSubscription(context.Background(), subEv("insert", fullSubDoc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 5)

	// 1. member_added (first in order).
	assert.Equal(t, model.InboxMemberAdded, pub.events[0].Type)
	var ma model.MemberAddEvent
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &ma))
	assert.Equal(t, "member_added", ma.Type)
	assert.Equal(t, "r1", ma.RoomID)
	assert.Equal(t, []string{"alice"}, ma.Accounts)
	assert.Equal(t, model.RoomTypeChannel, ma.RoomType)
	assert.Equal(t, "General", ma.RoomName)
	assert.Equal(t, testSiteID, ma.SiteID)
	assert.Equal(t, tsMillis, ma.JoinedAt)

	byType := eventsByType(pub.events)

	// 2. role_updated (owner → owner).
	roleEvt, ok := byType[model.InboxEventType("role_updated")]
	require.True(t, ok)
	var su model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(roleEvt.Payload, &su))
	assert.Equal(t, "alice", su.Subscription.User.Account)
	assert.Equal(t, "r1", su.Subscription.RoomID)
	assert.Equal(t, []model.Role{model.RoleOwner}, su.Subscription.Roles)

	// 3. mute (from disableNotifications=true).
	muteEvt, ok := byType[model.InboxSubscriptionMuteToggled]
	require.True(t, ok)
	var mute model.SubscriptionMuteToggledEvent
	require.NoError(t, json.Unmarshal(muteEvt.Payload, &mute))
	assert.Equal(t, "alice", mute.Account)
	assert.Equal(t, "r1", mute.RoomID)
	assert.True(t, mute.Muted)

	// 4. favorite (f=true).
	favEvt, ok := byType[model.InboxSubscriptionFavoriteToggled]
	require.True(t, ok)
	var fav model.SubscriptionFavoriteToggledEvent
	require.NoError(t, json.Unmarshal(favEvt.Payload, &fav))
	assert.True(t, fav.Favorite)

	// 5. subscription_read (LastSeenAt = max(ls,lr), Alert).
	readEvt, ok := byType[model.InboxSubscriptionRead]
	require.True(t, ok)
	var read model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(readEvt.Payload, &read))
	assert.Equal(t, "alice", read.Account)
	assert.Equal(t, "r1", read.RoomID)
	assert.Equal(t, lrMillis, read.LastSeenAt)
	assert.True(t, read.Alert)

	// All envelopes carry the home site + local dest.
	for _, e := range pub.events {
		assert.Equal(t, testSiteID, e.SiteID)
		assert.Equal(t, testSiteID, e.DestSiteID)
	}
}

func TestHandleSubscription_FieldGuards_UseSourceUpdatedAt(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	// The field-update guards (mute/favorite/roles) must stamp the event Timestamp from the
	// source _updatedAt — stable across redelivery — not from publish-time now(). Otherwise a
	// redelivered insert (inline snapshot) could out-rank a newer update at the destination.
	require.NoError(t, h.handleSubscription(context.Background(), subEv("insert", fullSubDoc, "")))
	byType := eventsByType(pub.events)

	var mute model.SubscriptionMuteToggledEvent
	require.NoError(t, json.Unmarshal(byType[model.InboxSubscriptionMuteToggled].Payload, &mute))
	assert.Equal(t, updatedAtMillis, mute.Timestamp)

	var fav model.SubscriptionFavoriteToggledEvent
	require.NoError(t, json.Unmarshal(byType[model.InboxSubscriptionFavoriteToggled].Payload, &fav))
	assert.Equal(t, updatedAtMillis, fav.Timestamp)

	var su model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(byType[model.InboxEventType("role_updated")].Payload, &su))
	assert.Equal(t, updatedAtMillis, su.Timestamp)
}

func TestHandleSubscription_FieldGuards_NoSourceUpdatedAt_FallsBackToNow(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	// No _updatedAt in source → guards fall back to now() (the test handler's fixed clock).
	doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c",` +
		`"roles":["owner"],"open":true,"disableNotifications":true,"f":true}`
	require.NoError(t, h.handleSubscription(context.Background(), subEv("insert", doc, "")))
	byType := eventsByType(pub.events)

	var mute model.SubscriptionMuteToggledEvent
	require.NoError(t, json.Unmarshal(byType[model.InboxSubscriptionMuteToggled].Payload, &mute))
	assert.Equal(t, int64(1700000000000), mute.Timestamp)
}

func TestHandleSubscription_DMInsert_CarriesRequesterAccount(t *testing.T) {
	pub := &fakePublisher{}
	// t:"d" (2 participants) → dm. RC stores the peer username in the sub's `name` field, and
	// inbox-worker names a DM subscription after RequesterAccount — so the event must carry it.
	doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"d","name":"bob","open":true}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: []byte(doc)})

	require.NoError(t, h.handleSubscription(context.Background(), subEv("insert", doc, "")))

	ma, ok := eventsByType(pub.events)[model.InboxMemberAdded]
	require.True(t, ok, "a DM insert must emit member_added")
	var e model.MemberAddEvent
	require.NoError(t, json.Unmarshal(ma.Payload, &e))
	assert.Equal(t, model.RoomTypeDM, e.RoomType)
	assert.Equal(t, "bob", e.RequesterAccount,
		"a DM sub must carry the peer username (ss.name) so inbox-worker names it — otherwise Name is empty")
}

func TestHandleSubscription_Insert_EmptyRoles_NoRoleEvent(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","fname":"General",
		"open":true,"f":false,"disableNotifications":false,"alert":false,
		"ls":{"$date":"2024-01-15T09:00:00.000Z"},"lr":{"$date":"2024-01-15T09:00:00.000Z"},
		"ts":{"$date":"2024-01-01T00:00:00.000Z"}}`
	err := h.handleSubscription(context.Background(), subEv("insert", doc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 4)
	byType := eventsByType(pub.events)
	_, hasRole := byType[model.InboxEventType("role_updated")]
	assert.False(t, hasRole)
	_, hasAdd := byType[model.InboxMemberAdded]
	assert.True(t, hasAdd)
}

func TestHandleSubscription_Insert_FederatedSiteID(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","fname":"General",
		"roles":["owner"],"open":true,
		"federation":{"origin":"0030204.tchat-test.test.company.com"},
		"ts":{"$date":"2024-01-01T00:00:00.000Z"}}`
	err := h.handleSubscription(context.Background(), subEv("insert", doc, ""))
	require.NoError(t, err)

	require.NotEmpty(t, pub.events)
	for _, e := range pub.events {
		assert.Equal(t, "0030204", e.SiteID)
	}
	var ma model.MemberAddEvent
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &ma))
	assert.Equal(t, "0030204", ma.SiteID)
}

func TestHandleSubscription_Update_FavoriteOnly(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(fullSubDoc)})

	err := h.handleSubscription(context.Background(), subEv("update", "", `{"updatedFields":{"f":true}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, model.InboxSubscriptionFavoriteToggled, pub.events[0].Type)
	var fav model.SubscriptionFavoriteToggledEvent
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &fav))
	assert.True(t, fav.Favorite)
}

func TestHandleSubscription_Update_OpenFalse_MemberRemoved(t *testing.T) {
	pub := &fakePublisher{}
	closedDoc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","fname":"General","open":false}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(closedDoc)})

	err := h.handleSubscription(context.Background(), subEv("update", "", `{"updatedFields":{"open":false}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, model.InboxMemberRemoved, pub.events[0].Type)
	var mr model.MemberRemoveEvent
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &mr))
	assert.Equal(t, "member_removed", mr.Type)
	assert.Equal(t, "r1", mr.RoomID)
	assert.Equal(t, []string{"alice"}, mr.Accounts)
	assert.Equal(t, testSiteID, mr.SiteID)
}

func TestHandleSubscription_Update_OpenTrue_Resubscribe(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(fullSubDoc)})

	err := h.handleSubscription(context.Background(), subEv("update", "", `{"updatedFields":{"open":true}}`))
	require.NoError(t, err)

	// Re-subscribe rebuilds full state: same 5 events as an insert.
	require.Len(t, pub.events, 5)
	byType := eventsByType(pub.events)
	_, hasAdd := byType[model.InboxMemberAdded]
	assert.True(t, hasAdd)
	_, hasRole := byType[model.InboxEventType("role_updated")]
	assert.True(t, hasRole)
	_, hasRead := byType[model.InboxSubscriptionRead]
	assert.True(t, hasRead)
}

func TestHandleSubscription_Update_ReadField(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(fullSubDoc)})

	err := h.handleSubscription(context.Background(), subEv("update", "", `{"updatedFields":{"ls":{"$date":"2024-01-15T09:00:00.000Z"}}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, model.InboxSubscriptionRead, pub.events[0].Type)
	var read model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &read))
	assert.Equal(t, lrMillis, read.LastSeenAt) // max(ls,lr) from current doc
	assert.True(t, read.Alert)
}

func TestHandleSubscription_Update_UnrecognizedField_Skip(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(fullSubDoc)})

	err := h.handleSubscription(context.Background(), subEv("update", "", `{"updatedFields":{"_updatedAt":{"$date":"2024-01-15T09:00:00.000Z"}}}`))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleSubscription_Delete_Skipped(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	// True row delete is un-actionable (spec §4.0/§4.3): only the source _id, which doesn't map to
	// the destination sub. A genuine leave arrives as an open:false update. Skip, publish nothing.
	err := h.handleSubscription(context.Background(), subEv("delete", "", ""))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleSubscription_MaxLsLr(t *testing.T) {
	tests := []struct {
		name string
		ls   string
		lr   string
		want int64
	}{
		{
			name: "lr later than ls",
			ls:   "2024-01-15T09:00:00.000Z",
			lr:   "2024-01-15T10:00:00.000Z",
			want: 1705312800000, // 10:00Z
		},
		{
			name: "ls later than lr",
			ls:   "2024-01-15T11:00:00.000Z",
			lr:   "2024-01-15T10:00:00.000Z",
			want: 1705316400000, // 11:00Z
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{}
			h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
			doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","fname":"General","open":true,` +
				`"alert":true,"ls":{"$date":"` + tc.ls + `"},"lr":{"$date":"` + tc.lr + `"},` +
				`"ts":{"$date":"2024-01-01T00:00:00.000Z"}}`
			err := h.handleSubscription(context.Background(), subEv("insert", doc, ""))
			require.NoError(t, err)

			byType := eventsByType(pub.events)
			readEvt, ok := byType[model.InboxSubscriptionRead]
			require.True(t, ok)
			var read model.SubscriptionReadEvent
			require.NoError(t, json.Unmarshal(readEvt.Payload, &read))
			assert.Equal(t, tc.want, read.LastSeenAt)
		})
	}
}

func TestHandleSubscription_Update_RolesAndMute(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(fullSubDoc)})

	err := h.handleSubscription(context.Background(), subEv("update", "",
		`{"updatedFields":{"roles":["owner"],"disableNotifications":true}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 2)
	byType := eventsByType(pub.events)
	_, hasRole := byType[model.InboxEventType("role_updated")]
	assert.True(t, hasRole)
	_, hasMute := byType[model.InboxSubscriptionMuteToggled]
	assert.True(t, hasMute)
}

func TestHandleSubscription_Replace_AllEvents(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	err := h.handleSubscription(context.Background(), subEv("replace", fullSubDoc, ""))
	require.NoError(t, err)
	assert.Len(t, pub.events, 5)
}

func TestHandleSubscription_PublishError(t *testing.T) {
	pub := &fakePublisher{err: errors.New("inbox down")}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	err := h.handleSubscription(context.Background(), subEv("insert", fullSubDoc, ""))
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
}

func TestHandleSubscription_ZeroTimestamps_GuardedNotYear0001(t *testing.T) {
	pub := &fakePublisher{}
	doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","disableNotifications":false}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
	require.NoError(t, h.handleSubscription(context.Background(), subEv("insert", doc, "")))
	byType := eventsByType(pub.events)

	var read model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(byType[model.InboxSubscriptionRead].Payload, &read))
	assert.Equal(t, int64(0), read.LastSeenAt, "absent ls/lr must yield 0, not a negative year-0001 millis")

	var ma model.MemberAddEvent
	require.NoError(t, json.Unmarshal(byType[model.InboxMemberAdded].Payload, &ma))
	assert.Equal(t, int64(1700000000000), ma.JoinedAt, "absent ts must fall back to now(), not year-0001")
}

func TestHandleSubscription_RolesCleared_EmitsRoleUpdated(t *testing.T) {
	pub := &fakePublisher{}
	doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","roles":[]}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: []byte(doc)})

	ev := oplogEvent{Op: "update", Collection: subsColl, EventID: "e1",
		DocumentKey:       json.RawMessage(`{"_id":"sub1"}`),
		UpdateDescription: json.RawMessage(`{"updatedFields":{"roles":[]}}`)}
	require.NoError(t, h.handleSubscription(context.Background(), ev))

	role, ok := eventsByType(pub.events)[model.InboxEventType("role_updated")]
	require.True(t, ok, "a roles-cleared update must still emit role_updated")
	var su model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(role.Payload, &su))
	// Cleared source roles must land as [member], never empty: inbox-worker permanently drops a
	// role_updated with no roles (malformed-event guard), and the new-stack floor is [member]
	// (room-service writes ["member"] after a live demotion — the migration must match).
	assert.Equal(t, []model.Role{model.RoleMember}, su.Subscription.Roles,
		"a demotion (cleared roles) must map to the [member] floor so it survives inbox-worker")
}
