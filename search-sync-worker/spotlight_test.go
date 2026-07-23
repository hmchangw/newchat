package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func makeInboxMemberEvent(t *testing.T, typ string, payload *model.InboxMemberEvent, ts int64) []byte {
	t.Helper()
	payloadData, err := json.Marshal(payload)
	require.NoError(t, err)
	evt := model.InboxEvent{
		Type:       typ,
		SiteID:     "site-a",
		DestSiteID: "site-a",
		Payload:    payloadData,
		Timestamp:  ts,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

// baseInboxMemberEvent builds a single-account event for the common unit-test
// case. Bulk-invite test cases supply their own Accounts slice.
func baseInboxMemberEvent() *model.InboxMemberEvent {
	const joinedAt int64 = 1735689600000
	return &model.InboxMemberEvent{
		RoomID:    "r-eng",
		RoomName:  "engineering",
		RoomType:  model.RoomTypeChannel,
		SiteID:    "site-a",
		Accounts:  []string{"alice"},
		JoinedAt:  joinedAt,
		Timestamp: joinedAt,
	}
}

func TestSpotlightCollection_Metadata(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)

	assert.Equal(t, "spotlight-sync", coll.ConsumerName())

	cfg := coll.StreamConfig("site-a")
	assert.Equal(t, "INBOX_site-a", cfg.Name)
	assert.Equal(t, []string{
		"chat.inbox.site-a.internal.>",
		"chat.inbox.site-a.external.>",
	}, cfg.Subjects)
	assert.Empty(t, cfg.Sources)

	filters := coll.FilterSubjects("site-a")
	assert.ElementsMatch(t, []string{
		"chat.inbox.site-a.internal.member_added",
		"chat.inbox.site-a.internal.member_removed",
		"chat.inbox.site-a.external.member_added",
		"chat.inbox.site-a.external.member_removed",
	}, filters)
}

func TestSpotlightCollection_StoredScripts(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1", false)
	assert.Empty(t, coll.StoredScripts(), "spotlight collection uses no stored scripts")
}

func TestSpotlightCollection_TemplateName_StripsVersion(t *testing.T) {
	c := newSpotlightCollection("spotlight-site-a-v1", false)
	assert.Equal(t, "spotlight-site-a_template", c.TemplateName())
}

func TestSpotlightCollection_TemplateBody_PatternStripsVersion(t *testing.T) {
	c := newSpotlightCollection("spotlight-site-a-v1", false)
	body := c.TemplateBody()
	require.NotNil(t, body)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	patterns, ok := decoded["index_patterns"].([]any)
	require.True(t, ok)
	require.Len(t, patterns, 1)
	assert.Equal(t, "spotlight-site-a-*", patterns[0])

	tmpl := decoded["template"].(map[string]any)
	mappings := tmpl["mappings"].(map[string]any)
	props := mappings["properties"].(map[string]any)
	assert.Contains(t, props, "userAccount")
	assert.Contains(t, props, "roomId")
	assert.Contains(t, props, "roomName")
	assert.Contains(t, props, "joinedAt")
	assert.Equal(t, false, mappings["dynamic"])

	roomName := props["roomName"].(map[string]any)
	assert.Equal(t, "search_as_you_type", roomName["type"])
	assert.Equal(t, "custom_analyzer", roomName["analyzer"])
}

func TestSpotlightTemplateProperties_MatchesStruct(t *testing.T) {
	props := esPropertiesFromStruct[SpotlightSearchIndex]()

	typ := reflect.TypeOf(SpotlightSearchIndex{})
	esFieldCount := 0
	for i := range typ.NumField() {
		field := typ.Field(i)
		esTag := field.Tag.Get("es")
		if esTag == "" || esTag == "-" {
			continue
		}
		esFieldCount++
		jsonTag := field.Tag.Get("json")
		name, _, _ := strings.Cut(jsonTag, ",")
		_, ok := props[name]
		assert.True(t, ok, "template missing property for field %s (json %s)", field.Name, name)
	}
	assert.Equal(t, esFieldCount, len(props))
}

func TestSpotlightCollection_BuildAction_MemberAdded(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 1000)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)

	action := actions[0]
	assert.Equal(t, searchengine.ActionIndex, action.Action)
	assert.Equal(t, "spotlight-site-a-v1-chat", action.Index)
	// DocID is synthesized as {account}_{roomID} since the new payload shape
	// doesn't carry subscription IDs.
	assert.Equal(t, "alice_r-eng", action.DocID)
	assert.Equal(t, int64(1000), action.Version)
	require.NotNil(t, action.Doc)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(action.Doc, &doc))
	assert.Equal(t, "alice", doc["userAccount"])
	assert.Equal(t, "r-eng", doc["roomId"])
	assert.Equal(t, "engineering", doc["roomName"])
	assert.Equal(t, "channel", doc["roomType"])
	assert.Equal(t, "site-a", doc["siteId"])
}

func TestSpotlightCollection_BuildAction_SkipsBots(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	// Real bots and the platform-admin pseudo-account are not searchable
	// principals; QA p_ accounts are ordinary users and ARE indexed.
	payload.Accounts = []string{"alice", "weather.bot", "p_tchatadmin_siteA", "p_qa1"}
	data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 1000)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2, "bots and the platform-admin pseudo-account must not be indexed")
	docIDs := []string{actions[0].DocID, actions[1].DocID}
	assert.ElementsMatch(t, []string{"alice_r-eng", "p_qa1_r-eng"}, docIDs)
}

func TestSpotlightCollection_BuildAction_AllBots_NoActions(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	payload.Accounts = []string{"weather.bot"}
	data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 1000)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err, "an all-bot event is a clean no-op, not an error")
	assert.Empty(t, actions)
}

func TestSpotlightCollection_BuildAction_MemberRemoved(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	data := makeInboxMemberEvent(t, model.InboxMemberRemoved, payload, 2000)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)

	action := actions[0]
	assert.Equal(t, searchengine.ActionDelete, action.Action)
	assert.Equal(t, "spotlight-site-a-v1-chat", action.Index)
	assert.Equal(t, "alice_r-eng", action.DocID)
	assert.Equal(t, int64(2000), action.Version)
	assert.Nil(t, action.Doc)
}

// TestSpotlightCollection_BuildAction_MemberRemoved_BotsCleanedUp verifies that
// bot / platform-admin removals are NOT skipped: a delete action must still be
// emitted so a bot doc indexed by legacy behavior (or during a rolling deploy)
// gets cleaned up. The delete is idempotent — a 404 on a never-indexed doc is a
// benign ack (see isBulkItemSuccess).
func TestSpotlightCollection_BuildAction_MemberRemoved_BotsCleanedUp(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	payload.Accounts = []string{"weather.bot", "p_tchatadmin_siteA"}
	data := makeInboxMemberEvent(t, model.InboxMemberRemoved, payload, 3000)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2, "bot and platform-admin removals must still emit cleanup deletes")

	docIDs := make([]string, len(actions))
	for i, action := range actions {
		assert.Equal(t, searchengine.ActionDelete, action.Action)
		assert.Equal(t, "spotlight-site-a-v1-chat", action.Index)
		assert.Equal(t, int64(3000), action.Version)
		assert.Nil(t, action.Doc)
		docIDs[i] = action.DocID
	}
	assert.ElementsMatch(t, []string{"weather.bot_r-eng", "p_tchatadmin_siteA_r-eng"}, docIDs)
}

// TestSpotlightCollection_BuildAction_MemberRemoved_MixedHumanAndBot verifies a
// mixed removal fans out to a delete for BOTH the human and the bot — the human
// because they were indexed, the bot as defensive cleanup of any stale doc.
func TestSpotlightCollection_BuildAction_MemberRemoved_MixedHumanAndBot(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	payload.Accounts = []string{"alice", "weather.bot"}
	data := makeInboxMemberEvent(t, model.InboxMemberRemoved, payload, 4000)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	docIDs := make([]string, len(actions))
	for i, action := range actions {
		assert.Equal(t, searchengine.ActionDelete, action.Action)
		assert.Nil(t, action.Doc)
		docIDs[i] = action.DocID
	}
	assert.ElementsMatch(t, []string{"alice_r-eng", "weather.bot_r-eng"}, docIDs)
}

func TestSpotlightCollection_BuildAction_RestrictedRoomIndexedLikeAnyOther(t *testing.T) {
	// See spotlightCollection.BuildAction docstring for the room-name
	// vs message-content access boundary.
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	payload.Accounts = []string{"alice", "bob"}
	hss := int64(1735689500000)
	payload.HistorySharedSince = &hss

	data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 100)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2, "restricted room must still produce one doc per account")

	docIDs := make([]string, len(actions))
	for i, a := range actions {
		docIDs[i] = a.DocID
		assert.Equal(t, searchengine.ActionIndex, a.Action)
		assert.Equal(t, "spotlight-site-a-v1-chat", a.Index)
		// evt.Timestamp → external version propagation. If a future
		// refactor silently drops Version for the restricted path,
		// spotlight would lose idempotency under redelivery.
		assert.Equal(t, int64(100), a.Version)
		require.NotNil(t, a.Doc, "restricted room must produce a populated doc, not a delete")

		// Confirm the room NAME — the discoverability payload this
		// whole change is about — actually made it into the indexed
		// body, not just that an index action was emitted.
		var body map[string]any
		require.NoError(t, json.Unmarshal(a.Doc, &body))
		assert.Equal(t, "r-eng", body["roomId"])
		assert.Equal(t, "engineering", body["roomName"])
	}
	assert.ElementsMatch(t, []string{"alice_r-eng", "bob_r-eng"}, docIDs)
}

func TestSpotlightCollection_BuildAction_Errors(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)

	t.Run("malformed inbox event", func(t *testing.T) {
		_, err := coll.BuildAction([]byte("{invalid"))
		assert.Error(t, err)
	})

	t.Run("malformed payload", func(t *testing.T) {
		evt := model.InboxEvent{
			Type:      model.InboxMemberAdded,
			Payload:   []byte("not json"),
			Timestamp: 100,
		}
		data, _ := json.Marshal(evt)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("empty accounts", func(t *testing.T) {
		payload := baseInboxMemberEvent()
		payload.Accounts = nil
		data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 100)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("empty account in list", func(t *testing.T) {
		payload := baseInboxMemberEvent()
		payload.Accounts = []string{"alice", ""}
		data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 100)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("missing roomId", func(t *testing.T) {
		payload := baseInboxMemberEvent()
		payload.RoomID = ""
		data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 100)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("missing timestamp", func(t *testing.T) {
		data := makeInboxMemberEvent(t, model.InboxMemberAdded, baseInboxMemberEvent(), 0)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("unsupported event type", func(t *testing.T) {
		data := makeInboxMemberEvent(t, "room_created", baseInboxMemberEvent(), 100)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})
}

// TestSpotlightCollection_BuildAction_BulkInvite verifies fan-out: a single
// event carrying N accounts produces N index actions, all sharing the same
// external Version (event timestamp).
func TestSpotlightCollection_BuildAction_BulkInvite(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	payload.Accounts = []string{"alice", "bob", "carol"}
	data := makeInboxMemberEvent(t, model.InboxMemberAdded, payload, 12345)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 3, "3 accounts should fan out to 3 actions")

	seenDocIDs := make(map[string]bool)
	for _, action := range actions {
		assert.Equal(t, searchengine.ActionIndex, action.Action)
		assert.Equal(t, "spotlight-site-a-v1-chat", action.Index)
		assert.Equal(t, int64(12345), action.Version,
			"all fan-out actions share the source event's timestamp as their external version")
		require.NotNil(t, action.Doc)
		seenDocIDs[action.DocID] = true
	}
	for _, account := range payload.Accounts {
		assert.True(t, seenDocIDs[fmt.Sprintf("%s_%s", account, payload.RoomID)],
			"expected DocID for %s", account)
	}
}

// TestSpotlightCollection_BuildAction_BulkRemove verifies fan-out on remove:
// N accounts → N delete actions.
func TestSpotlightCollection_BuildAction_BulkRemove(t *testing.T) {
	coll := newSpotlightCollection("spotlight-site-a-v1-chat", false)
	payload := baseInboxMemberEvent()
	payload.Accounts = []string{"alice", "bob"}
	data := makeInboxMemberEvent(t, model.InboxMemberRemoved, payload, 67890)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	for _, action := range actions {
		assert.Equal(t, searchengine.ActionDelete, action.Action)
		assert.Equal(t, int64(67890), action.Version)
		assert.Nil(t, action.Doc)
	}
}
