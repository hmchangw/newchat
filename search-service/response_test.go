package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestParseMessagesResponse_HappyPath(t *testing.T) {
	body := json.RawMessage(`{
		"hits": {
			"total": {"value": 2},
			"hits": [
				{"_source": {
					"messageId": "m1", "roomId": "r1", "siteId": "site-a",
					"userId": "u1", "userAccount": "alice", "content": "hello",
					"createdAt": "2026-04-01T12:00:00Z"
				}},
				{"_source": {
					"messageId": "m2", "roomId": "r2", "siteId": "site-b",
					"userId": "u2", "userAccount": "bob", "content": "world (edited)",
					"createdAt": "2026-04-02T12:00:00Z",
					"editedAt": "2026-04-02T12:05:00Z",
					"updatedAt": "2026-04-02T12:06:00Z",
					"threadParentMessageId": "p1",
					"threadParentMessageCreatedAt": "2026-04-02T11:00:00Z"
				}}
			]
		}
	}`)

	hits, total, err := parseMessagesResponse(body)
	require.NoError(t, err)
	assert.EqualValues(t, 2, total)
	require.Len(t, hits, 2)

	assert.Equal(t, "m1", hits[0].MessageID)
	assert.Equal(t, "alice", hits[0].UserAccount)
	assert.Empty(t, hits[0].ThreadParentID)
	assert.Nil(t, hits[0].EditedAt, "never-edited hit decodes EditedAt as nil")
	assert.Nil(t, hits[0].UpdatedAt)

	assert.Equal(t, "p1", hits[1].ThreadParentID)
	require.NotNil(t, hits[1].ThreadParentCreatedAt)
	want := time.Date(2026, 4, 2, 11, 0, 0, 0, time.UTC)
	assert.True(t, hits[1].ThreadParentCreatedAt.Equal(want))

	require.NotNil(t, hits[1].EditedAt)
	require.NotNil(t, hits[1].UpdatedAt)
	wantEdited := time.Date(2026, 4, 2, 12, 5, 0, 0, time.UTC)
	wantUpdated := time.Date(2026, 4, 2, 12, 6, 0, 0, time.UTC)
	assert.True(t, hits[1].EditedAt.Equal(wantEdited))
	assert.True(t, hits[1].UpdatedAt.Equal(wantUpdated))
}

func TestParseMessagesResponse_Empty(t *testing.T) {
	body := json.RawMessage(`{"hits":{"total":{"value":0},"hits":[]}}`)
	hits, total, err := parseMessagesResponse(body)
	require.NoError(t, err)
	assert.EqualValues(t, 0, total)
	assert.Empty(t, hits)
}

func TestParseMessagesResponse_Malformed(t *testing.T) {
	_, _, err := parseMessagesResponse(json.RawMessage(`{not json`))
	assert.Error(t, err)
}

func TestToSearchMessage_ProjectsESFields(t *testing.T) {
	hit := messageSearchHit{
		MessageID:   "m1",
		RoomID:      "r1",
		SiteID:      "site-a",
		UserID:      "u1",
		UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}

	got := toSearchMessage(&hit)

	assert.Equal(t, "m1", got.MessageID)
	assert.Equal(t, "r1", got.RoomID)
	assert.Equal(t, "alice", got.UserAccount)
	assert.Equal(t, "hello", got.Content)
	assert.Equal(t, "site-a", got.SiteID)
	// UserID is intentionally NOT exposed in the wire type.
	// EditedAt/UpdatedAt are nil on a never-edited hit.
	assert.Nil(t, got.EditedAt)
	assert.Nil(t, got.UpdatedAt)
}

func TestToSearchMessage_EditedAndUpdatedCopied(t *testing.T) {
	edited := time.Date(2026, 4, 1, 12, 5, 0, 0, time.UTC)
	updated := time.Date(2026, 4, 1, 12, 6, 0, 0, time.UTC)
	hit := messageSearchHit{
		MessageID:   "m1",
		RoomID:      "r1",
		UserAccount: "alice",
		Content:     "hello (edited)",
		EditedAt:    &edited,
		UpdatedAt:   &updated,
	}
	got := toSearchMessage(&hit)
	require.NotNil(t, got.EditedAt)
	require.NotNil(t, got.UpdatedAt)
	assert.True(t, got.EditedAt.Equal(edited))
	assert.True(t, got.UpdatedAt.Equal(updated))
}

func TestToSearchMessage_ThreadParentBothFieldsCopied(t *testing.T) {
	tp := time.Date(2026, 4, 2, 11, 0, 0, 0, time.UTC)
	hit := messageSearchHit{
		MessageID:             "m1",
		RoomID:                "r1",
		UserAccount:           "alice",
		ThreadParentID:        "p1",
		ThreadParentCreatedAt: &tp,
	}
	got := toSearchMessage(&hit)
	assert.Equal(t, "p1", got.ThreadParentMessageID)
	require.NotNil(t, got.ThreadParentMessageCreatedAt)
	assert.True(t, got.ThreadParentMessageCreatedAt.Equal(tp))
}

// Full attachment objects and the card ride the hit so the client can render
// the result (file row, tcard) without a history-service lookup.
func TestToSearchMessage_AttachmentAndCardFieldsCopied(t *testing.T) {
	hit := messageSearchHit{
		MessageID:   "m1",
		RoomID:      "r1",
		UserAccount: "alice",
		Attachments: []model.Attachment{
			{ID: "f1", Title: "q3-report.pdf", Description: "Quarterly numbers", FileType: "application/pdf", TitleLink: "api/v1/file/rooms/r1/file/f1", TitleLinkDownload: true},
			{ID: "f2", Title: "team.png", FileType: "image/png"},
		},
		Card: &model.Card{Template: "expense-approval-v1", Data: []byte(`{"amount":42}`)},
	}
	got := toSearchMessage(&hit)
	require.Len(t, got.Attachments, 2)
	assert.Equal(t, "q3-report.pdf", got.Attachments[0].Title)
	assert.Equal(t, "Quarterly numbers", got.Attachments[0].Description)
	assert.Equal(t, "api/v1/file/rooms/r1/file/f1", got.Attachments[0].TitleLink)
	require.NotNil(t, got.Card)
	assert.Equal(t, "expense-approval-v1", got.Card.Template)
	assert.Equal(t, []byte(`{"amount":42}`), got.Card.Data)
}

// A text-only hit must not grow attachment/card keys on the wire.
func TestToSearchMessage_AttachmentAndCardOmittedWhenAbsent(t *testing.T) {
	hit := messageSearchHit{MessageID: "m1", RoomID: "r1", UserAccount: "alice", Content: "hello"}
	got := toSearchMessage(&hit)

	data, err := json.Marshal(got)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	for _, key := range []string{"attachments", "card"} {
		_, present := raw[key]
		assert.False(t, present, "%s should be omitted when empty", key)
	}
}

func TestParseRooms_HappyPath(t *testing.T) {
	body := json.RawMessage(`{
		"hits": {
			"total": {"value": 2},
			"hits": [
				{"_source": {
					"roomId": "r1", "roomName": "general", "roomType": "channel",
					"userAccount": "alice", "siteId": "site-a",
					"joinedAt": "2026-04-01T12:00:00Z"
				}},
				{"_source": {
					"roomId": "r2", "roomName": "alice-bob", "roomType": "dm",
					"userAccount": "alice", "siteId": "site-b",
					"joinedAt": "2026-04-02T12:00:00Z"
				}}
			]
		}
	}`)

	rooms, err := parseRooms(body)
	require.NoError(t, err)
	require.Len(t, rooms, 2)
	assert.Equal(t, model.SearchRoom{RoomID: "r1", Name: "general", RoomType: "channel", SiteID: "site-a"}, rooms[0])
	assert.Equal(t, model.SearchRoom{RoomID: "r2", Name: "alice-bob", RoomType: "dm", SiteID: "site-b"}, rooms[1])
}

func TestParseRooms_Empty(t *testing.T) {
	body := json.RawMessage(`{"hits":{"total":{"value":0},"hits":[]}}`)
	rooms, err := parseRooms(body)
	require.NoError(t, err)
	assert.Empty(t, rooms)
	assert.NotNil(t, rooms, "must be empty slice, not nil")
}

func TestParseRooms_Malformed(t *testing.T) {
	_, err := parseRooms(json.RawMessage(`{`))
	assert.Error(t, err)
}

func TestParseRooms_PreservesOrder(t *testing.T) {
	body := json.RawMessage(`{
		"hits": {
			"total": {"value": 3},
			"hits": [
				{"_source": {"roomId": "r3", "roomName": "c"}},
				{"_source": {"roomId": "r1", "roomName": "a"}},
				{"_source": {"roomId": "r2", "roomName": "b"}}
			]
		}
	}`)
	rooms, err := parseRooms(body)
	require.NoError(t, err)
	got := []string{rooms[0].RoomID, rooms[1].RoomID, rooms[2].RoomID}
	assert.Equal(t, []string{"r3", "r1", "r2"}, got, "ES relevance order must be preserved")
}

func TestParseOrgs_HappyPath(t *testing.T) {
	body := json.RawMessage(`{
		"hits": {
			"total": {"value": 2},
			"hits": [
				{"_source": {
					"sectId": "S1", "sectName": "Engineering", "sectTCName": "工程",
					"deptId": "D1", "deptName": "Technology", "deptTCName": "科技",
					"divisionId": "DIV1"
				}},
				{"_source": {
					"sectId": "S2", "sectName": "Engineering Ops",
					"deptId": "D1", "deptName": "Technology", "divisionId": "DIV1"
				}}
			]
		}
	}`)

	orgs, err := parseOrgs(body)
	require.NoError(t, err)
	require.Len(t, orgs, 2)
	assert.Equal(t, model.SearchOrg{
		SectID: "S1", SectName: "Engineering", SectTCName: "工程",
		DeptID: "D1", DeptName: "Technology", DeptTCName: "科技", DivisionID: "DIV1",
	}, orgs[0])
	assert.Equal(t, "S2", orgs[1].SectID)
	assert.Equal(t, "Engineering Ops", orgs[1].SectName)
}

func TestParseOrgs_Empty(t *testing.T) {
	body := json.RawMessage(`{"hits":{"total":{"value":0},"hits":[]}}`)
	orgs, err := parseOrgs(body)
	require.NoError(t, err)
	assert.Empty(t, orgs)
	assert.NotNil(t, orgs, "must be empty slice, not nil")
}

func TestParseOrgs_Malformed(t *testing.T) {
	_, err := parseOrgs(json.RawMessage(`{`))
	assert.Error(t, err)
}

func TestParseOrgs_PreservesOrder(t *testing.T) {
	body := json.RawMessage(`{
		"hits": {
			"total": {"value": 3},
			"hits": [
				{"_source": {"sectId": "S3"}},
				{"_source": {"sectId": "S1"}},
				{"_source": {"sectId": "S2"}}
			]
		}
	}`)
	orgs, err := parseOrgs(body)
	require.NoError(t, err)
	got := []string{orgs[0].SectID, orgs[1].SectID, orgs[2].SectID}
	assert.Equal(t, []string{"S3", "S1", "S2"}, got, "ES relevance order must be preserved")
}

func TestToSearchOrg_ProjectsAllFields(t *testing.T) {
	hit := orgSearchHit{
		SectID: "S1", SectTCName: "工程", SectName: "Engineering", SectDescription: "d1",
		DeptID: "D1", DeptTCName: "科技", DeptName: "Technology", DeptDescription: "d2",
		DivisionID: "DIV1",
	}
	got := toSearchOrg(&hit)
	assert.Equal(t, model.SearchOrg{
		SectID: "S1", SectName: "Engineering", SectTCName: "工程", SectDescription: "d1",
		DeptID: "D1", DeptName: "Technology", DeptTCName: "科技", DeptDescription: "d2",
		DivisionID: "DIV1",
	}, got)
}
