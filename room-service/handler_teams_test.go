package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// errStubDuplicateKey is a real mongo.WriteException carrying code 11000, so
// mongo.IsDuplicateKeyError(errStubDuplicateKey) == true — the handler's
// race-detection branch keys off exactly that, so the stub must produce an
// error the production code recognizes.
var errStubDuplicateKey = mongo.WriteException{
	WriteErrors: mongo.WriteErrors{{Index: 0, Code: 11000, Message: "E11000 duplicate key error"}},
}

// fakeGraphClient is a hand-rolled msgraph.Client double. It records calls and
// returns a canned meeting or error. callCount lets idempotency tests assert
// that a second meetings call does NOT reach Graph.
type fakeGraphClient struct {
	meeting   *msgraph.OnlineMeeting
	err       error
	callCount int
	lastReq   msgraph.CreateOnlineMeetingRequest
}

func (f *fakeGraphClient) CreateOnlineMeeting(_ context.Context, req msgraph.CreateOnlineMeetingRequest) (*msgraph.OnlineMeeting, error) {
	f.callCount++
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.meeting, nil
}

// stubTeamsMeetingStore is a hand-rolled TeamsMeetingStore double backed by an
// in-memory map keyed on (roomId, siteId). It enforces the (roomId, siteId)
// unique constraint just like the real Mongo unique index, so InsertTeamsMeeting
// returns a duplicate-key error on a second insert for the same key — letting
// the handler tests exercise the concurrent/dup-key path.
//
// getErr / forceDupErr let tests inject failures: getErr makes the fast-path
// read fail; forceDupErr makes the FIRST insert return a duplicate-key error
// (simulating a concurrent winner who inserted between this caller's fast-path
// read and its own insert) without a record yet being readable — exercising the
// race branch deterministically.
type stubTeamsMeetingStore struct {
	mu      sync.Mutex
	records map[string]model.TeamsMeetingRecord
	getErr  error
	// inserts counts successful + attempted inserts that reached the store.
	insertAttempts int
}

func newStubTeamsMeetingStore() *stubTeamsMeetingStore {
	return &stubTeamsMeetingStore{records: map[string]model.TeamsMeetingRecord{}}
}

func teamsMeetingKey(roomID, siteID string) string { return siteID + "::" + roomID }

func (s *stubTeamsMeetingStore) seed(rec model.TeamsMeetingRecord) {
	s.records[teamsMeetingKey(rec.RoomID, rec.SiteID)] = rec
}

func (s *stubTeamsMeetingStore) GetTeamsMeeting(_ context.Context, roomID, siteID string) (*model.TeamsMeetingRecord, bool, error) {
	if s.getErr != nil {
		return nil, false, s.getErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[teamsMeetingKey(roomID, siteID)]
	if !ok {
		return nil, false, nil
	}
	r := rec
	return &r, true, nil
}

func (s *stubTeamsMeetingStore) InsertTeamsMeeting(_ context.Context, record model.TeamsMeetingRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertAttempts++
	k := teamsMeetingKey(record.RoomID, record.SiteID)
	if _, exists := s.records[k]; exists {
		return errStubDuplicateKey // mimics mongo.IsDuplicateKeyError == true
	}
	s.records[k] = record
	return nil
}

// raceTeamsMeetingStore models the read-then-insert race deterministically: the
// fast-path GetTeamsMeeting (first read) misses, then InsertTeamsMeeting always
// collides (a concurrent winner inserted `winner` in between), and the post-
// dup-key read-back returns `winner`.
type raceTeamsMeetingStore struct {
	winner    model.TeamsMeetingRecord
	firstRead bool
}

func (s *raceTeamsMeetingStore) GetTeamsMeeting(_ context.Context, _, _ string) (*model.TeamsMeetingRecord, bool, error) {
	if !s.firstRead {
		s.firstRead = true
		return nil, false, nil // fast-path miss
	}
	rec := s.winner
	return &rec, true, nil // read-back after dup-key hit
}

func (s *raceTeamsMeetingStore) InsertTeamsMeeting(_ context.Context, _ model.TeamsMeetingRecord) error {
	return errStubDuplicateKey // a concurrent winner already inserted
}

// createOrGetGraphStub mimics Graph createOrGet: one meeting per externalId,
// returned for every call with that key. Concurrency-safe.
type createOrGetGraphStub struct {
	mu      sync.Mutex
	byExtID map[string]*msgraph.OnlineMeeting
}

func newCreateOrGetGraphStub() *createOrGetGraphStub {
	return &createOrGetGraphStub{byExtID: map[string]*msgraph.OnlineMeeting{}}
}

func (g *createOrGetGraphStub) CreateOnlineMeeting(_ context.Context, req msgraph.CreateOnlineMeetingRequest) (*msgraph.OnlineMeeting, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.byExtID[req.ExternalID]; ok {
		return m, nil
	}
	m := &msgraph.OnlineMeeting{ID: "mtg-" + req.ExternalID, JoinURL: "https://teams.example/join/" + req.ExternalID}
	g.byExtID[req.ExternalID] = m
	return m, nil
}

func (g *createOrGetGraphStub) distinctMeetings() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.byExtID)
}

func indMember(account string) model.RoomMember {
	return model.RoomMember{
		Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: account},
	}
}

func orgMember(id string) model.RoomMember {
	return model.RoomMember{Member: model.RoomMemberEntry{Type: model.RoomMemberOrg, ID: id}}
}

// --- calls/room (teamsRoomCall) ---

func TestTeamsRoomCall_Success_ExcludesSelf(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice"), indMember("bob"), orgMember("orgX"), indMember("carol")}, nil)

	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersCallLimit: 20}

	resp, err := h.teamsRoomCall(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsRoomCallRequest{})
	require.NoError(t, err)

	// alice (self) and the org entry are excluded; bob + carol remain, order preserved.
	users := parseUsersParam(t, resp.JoinURL)
	assert.Equal(t, []string{"bob@corp.com", "carol@corp.com"}, users)
	assert.True(t, strings.HasPrefix(resp.JoinURL, "https://teams.microsoft.com/l/call/0/0?"))
}

func TestTeamsRoomCall_RequesterMissing(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersCallLimit: 20}
	_, err := h.teamsRoomCall(ctxParams(map[string]string{"account": "", "roomID": "r1"}), model.TeamsRoomCallRequest{})
	require.ErrorIs(t, err, errTeamsRequesterMissing)
}

func TestTeamsRoomCall_RoomIDMissing(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersCallLimit: 20}
	_, err := h.teamsRoomCall(ctxParams(map[string]string{"account": "alice", "roomID": ""}), model.TeamsRoomCallRequest{})
	require.ErrorIs(t, err, errTeamsRoomIDRequired)
}

func TestTeamsRoomCall_NotMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(model.ErrSubscriptionNotFound)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersCallLimit: 20}
	_, err := h.teamsRoomCall(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsRoomCallRequest{})
	require.ErrorIs(t, err, errNotRoomMember)
}

func TestTeamsRoomCall_NoOtherMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice")}, nil)

	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersCallLimit: 20}
	_, err := h.teamsRoomCall(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsRoomCallRequest{})
	require.ErrorIs(t, err, errTeamsNoCallableMembers)
	assert.Equal(t, errcode.RoomTargetNotMember, errcode.ReasonOf(err))
}

func TestTeamsRoomCall_TooManyMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	members := []model.RoomMember{indMember("alice")}
	for i := 0; i < 3; i++ {
		members = append(members, indMember(string(rune('a'+i))+"x"))
	}
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).Return(members, nil)

	// CallLimit=2, but 3 other members → over the limit.
	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersCallLimit: 2}
	_, err := h.teamsRoomCall(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsRoomCallRequest{})
	require.ErrorIs(t, err, errTeamsCallTooManyMembers)
	assert.Equal(t, errcode.RoomMaxSizeReached, errcode.ReasonOf(err))
}

// --- calls/user (teamsUserCall) ---

func TestTeamsUserCall_Success(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com"}
	resp, err := h.teamsUserCall(ctxParams(map[string]string{"account": "alice"}), model.TeamsUserCallRequest{AccountName: "bob"})
	require.NoError(t, err)
	users := parseUsersParam(t, resp.JoinURL)
	assert.Equal(t, []string{"bob@corp.com"}, users)
}

func TestTeamsUserCall_RequesterMissing(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com"}
	_, err := h.teamsUserCall(ctxParams(map[string]string{"account": ""}), model.TeamsUserCallRequest{AccountName: "bob"})
	require.ErrorIs(t, err, errTeamsRequesterMissing)
}

func TestTeamsUserCall_AccountNameRequired(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com"}
	_, err := h.teamsUserCall(ctxParams(map[string]string{"account": "alice"}), model.TeamsUserCallRequest{AccountName: ""})
	require.ErrorIs(t, err, errTeamsAccountRequired)
}

// --- meetings (teamsMeeting) ---

func TestTeamsMeeting_CreatesAndPublishes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice"), indMember("bob")}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", EngName: "Alice"}, nil)

	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "mtg-1", JoinURL: "https://teams.example/join/1"}}

	var publishedSubj string
	var publishedData []byte
	var publishedMsgID string
	meetingStore := newStubTeamsMeetingStore()
	h := &Handler{
		store:             store,
		siteID:            "site-a",
		teamsEmailDomain:  "corp.com",
		roomMembersLimit:  500,
		graphClient:       graph,
		teamsMeetingStore: meetingStore,
		publishToStream: func(_ context.Context, subj string, data []byte, msgID string) error {
			publishedSubj, publishedData, publishedMsgID = subj, data, msgID
			return nil
		},
	}

	resp, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "mtg-1", resp.ID)
	assert.Equal(t, "https://teams.example/join/1", resp.JoinURL)

	// Graph called exactly once; organizer + attendees derived from accounts;
	// externalId is the stable siteID:roomID key.
	assert.Equal(t, 1, graph.callCount)
	assert.Equal(t, "site-a:r1", graph.lastReq.ExternalID)
	assert.Equal(t, "alice@corp.com", graph.lastReq.OrganizerEmail)
	assert.ElementsMatch(t, []string{"alice@corp.com", "bob@corp.com"}, graph.lastReq.AttendeeEmails)

	// The meeting was persisted as a first-class record keyed (roomId, siteId).
	rec, found, _ := meetingStore.GetTeamsMeeting(context.Background(), "r1", "site-a")
	require.True(t, found)
	assert.Equal(t, "mtg-1", rec.MeetingID)
	assert.Equal(t, "https://teams.example/join/1", rec.JoinURL)

	// teams_meet_started published through the canonical message path.
	require.NotEmpty(t, publishedData, "expected a canonical message publish")
	assert.NotEmpty(t, publishedMsgID, "expected a dedup msg ID")
	assert.Contains(t, publishedSubj, "chat.msg.canonical.site-a.created")

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(publishedData, &evt))
	assert.Equal(t, model.MessageTypeTeamsMeetStarted, evt.Message.Type)
	assert.Equal(t, `"Alice" started a Teams meeting`, evt.Message.Content)
	var sys model.TeamsMeetStartedSysData
	require.NoError(t, json.Unmarshal(evt.Message.SysMsgData, &sys))
	assert.Equal(t, "mtg-1", sys.MeetingID)
	assert.Equal(t, "https://teams.example/join/1", sys.JoinURL)
}

// TestTeamsMeeting_Idempotent_FastPathReadHit: an existing teams_meetings record
// short-circuits the handler — no Graph create, no member list, no publish.
func TestTeamsMeeting_Idempotent_FastPathReadHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	// ListRoomMembers must NOT be called when a record already exists.

	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "should-not-be-used"}}
	meetingStore := newStubTeamsMeetingStore()
	meetingStore.seed(model.TeamsMeetingRecord{
		RoomID: "r1", SiteID: "site-a",
		MeetingID: "mtg-existing", JoinURL: "https://teams.example/join/existing",
	})
	h := &Handler{
		store:             store,
		siteID:            "site-a",
		teamsEmailDomain:  "corp.com",
		roomMembersLimit:  500,
		graphClient:       graph,
		teamsMeetingStore: meetingStore,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Error("idempotent path must not publish a new system message")
			return nil
		},
	}

	resp, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "mtg-existing", resp.ID)
	assert.Equal(t, "https://teams.example/join/existing", resp.JoinURL)
	assert.Equal(t, 0, graph.callCount, "no duplicate Graph create on the idempotent path")
}

// TestTeamsMeeting_DuplicateKey_ReturnsExisting: the fast-path read misses but a
// concurrent winner inserted the record between this caller's read and its own
// insert. The insert hits the (roomId, siteId) unique constraint; the handler
// reads back the winner's record and returns it WITHOUT publishing a second
// system message. createOrGet already guaranteed the same Graph meeting.
func TestTeamsMeeting_DuplicateKey_ReturnsExisting(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice")}, nil)

	// createOrGet returns the SAME meeting the concurrent winner got (Graph is
	// the source of truth on a true race).
	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "mtg-shared", JoinURL: "https://teams.example/join/shared"}}

	// Seed the store as if a concurrent winner already inserted the record, so
	// this caller's fast-path read still misses (we delete it first) but its
	// insert collides. Simulate by pre-populating AFTER the fast-path read would
	// run: easiest deterministic model is to seed the winner's record and force
	// the fast-path read to miss via a one-shot. Here we instead seed the record
	// and rely on the handler: fast-path read would HIT. To exercise the dup-key
	// branch specifically, use a store whose first GetTeamsMeeting misses then
	// the insert collides — modeled below.
	meetingStore := &raceTeamsMeetingStore{
		winner: model.TeamsMeetingRecord{
			RoomID: "r1", SiteID: "site-a",
			MeetingID: "mtg-shared", JoinURL: "https://teams.example/join/shared",
		},
	}

	published := false
	h := &Handler{
		store:             store,
		siteID:            "site-a",
		teamsEmailDomain:  "corp.com",
		roomMembersLimit:  500,
		graphClient:       graph,
		teamsMeetingStore: meetingStore,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			published = true
			return nil
		},
	}

	resp, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "mtg-shared", resp.ID)
	assert.Equal(t, "https://teams.example/join/shared", resp.JoinURL)
	assert.Equal(t, 1, graph.callCount, "createOrGet still called, returns the shared meeting")
	assert.False(t, published, "loser of the insert race must NOT publish a second system message")
}

// TestTeamsMeeting_Concurrent_SingleCreateSingleMessage runs two concurrent
// meetings calls against one shared in-memory store enforcing the (roomId,
// siteId) unique constraint, plus a Graph stub that mimics createOrGet
// (one meeting per externalId). It asserts: exactly one Graph meeting, exactly
// one system message published, and both callers return the same meeting.
func TestTeamsMeeting_Concurrent_SingleCreateSingleMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	// Both goroutines run the membership/room/member reads; allow any count.
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil).AnyTimes()
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil).AnyTimes()
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice")}, nil).AnyTimes()
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", EngName: "Alice"}, nil).AnyTimes()

	graph := newCreateOrGetGraphStub()
	meetingStore := newStubTeamsMeetingStore()

	var publishCount int
	var pubMu sync.Mutex
	h := &Handler{
		store:             store,
		siteID:            "site-a",
		teamsEmailDomain:  "corp.com",
		roomMembersLimit:  500,
		graphClient:       graph,
		teamsMeetingStore: meetingStore,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			pubMu.Lock()
			publishCount++
			pubMu.Unlock()
			return nil
		},
	}

	const n = 8
	var wg sync.WaitGroup
	results := make([]*model.TeamsMeetingReply, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = h.teamsMeeting(
				ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
				model.TeamsMeetingRequest{},
			)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		assert.Equal(t, results[0].ID, results[i].ID, "all concurrent callers return the same meeting")
		assert.Equal(t, results[0].JoinURL, results[i].JoinURL)
	}
	assert.Equal(t, 1, graph.distinctMeetings(), "exactly one Graph meeting for one externalId")
	assert.Equal(t, 1, publishCount, "exactly one teams_meet_started system message")
}

func TestTeamsMeeting_NotConfigured(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500} // graphClient + teamsMeetingStore nil
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errTeamsNotConfigured)
}

func TestTeamsMeeting_RequesterMissing(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: &fakeGraphClient{}, teamsMeetingStore: newStubTeamsMeetingStore()}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errTeamsRequesterMissing)
}

func TestTeamsMeeting_RoomIDMissing(t *testing.T) {
	h := &Handler{siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: &fakeGraphClient{}, teamsMeetingStore: newStubTeamsMeetingStore()}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": ""}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errTeamsRoomIDRequired)
}

func TestTeamsMeeting_NotMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(model.ErrSubscriptionNotFound)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	graph := &fakeGraphClient{}
	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, teamsMeetingStore: newStubTeamsMeetingStore()}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errNotRoomMember)
	assert.Equal(t, 0, graph.callCount)
}

func TestTeamsMeeting_TooManyMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice"), indMember("bob"), indMember("carol")}, nil)

	graph := &fakeGraphClient{}
	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 2,
		graphClient: graph, teamsMeetingStore: newStubTeamsMeetingStore()}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errTeamsMeetingTooManyMembers)
	assert.Equal(t, errcode.RoomMaxSizeReached, errcode.ReasonOf(err))
	assert.Equal(t, 0, graph.callCount, "limit gate must short-circuit before Graph")
}

func TestTeamsMeeting_GraphCreateFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice")}, nil)

	graph := &fakeGraphClient{err: errors.New("graph 500")}
	var published bool
	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, teamsMeetingStore: newStubTeamsMeetingStore(),
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { published = true; return nil },
	}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.Error(t, err)
	assert.False(t, published, "no system message on Graph failure")
}

func TestTeamsMeeting_RecordReadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	graph := &fakeGraphClient{}
	meetingStore := newStubTeamsMeetingStore()
	meetingStore.getErr = errors.New("mongo down")
	h := &Handler{store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, teamsMeetingStore: meetingStore}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.Error(t, err)
	assert.Equal(t, 0, graph.callCount)
}

// parseUsersParam extracts the comma-separated `users` query param from a Teams
// deep link and returns the individual entries.
func parseUsersParam(t *testing.T, link string) []string {
	t.Helper()
	u, err := url.Parse(link)
	require.NoError(t, err)
	raw := u.Query().Get("users")
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}
