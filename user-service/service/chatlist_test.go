package service

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// customSection builds a client-created (non-built-in) section definition.
func customSection(id, name, sortMode string) model.ChatlistSection {
	return model.ChatlistSection{ID: id, Name: name, BuiltIn: false, SortMode: sortMode}
}

// storedChatlist is a customized chatlist: the four built-ins plus one custom
// section "Work" (c1) sitting between "teams" and "chats".
func storedChatlist() *model.ChatlistState {
	return &model.ChatlistState{
		SectionOrder: []string{model.SectionFavorites, model.SectionApps, model.SectionTeams, "c1", model.SectionChats},
		Sections:     append(model.DefaultChatlistState().Sections, customSection("c1", "Work", model.SortModeCustom)),
	}
}

// expectChatlistFanout allows both post-mutation publishes (client event +
// cross-site inbox) for tests that assert on the persisted state, not the fanout.
func expectChatlistFanout(pub *mocks.MockEventPublisher, account string) {
	pub.EXPECT().Publish(gomock.Any(), subject.ChatlistUpdate(account), gomock.Any()).Return(nil).AnyTimes()
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserChatlistUpdated), gomock.Any()).
		Return(nil).AnyTimes()
}

// echoChatlistUpdate stubs the repo write to return the state it was handed —
// what Mongo's whole-object $set with return-new does — and captures it.
func echoChatlistUpdate(users *mocks.MockUserRepository, account string, got **model.ChatlistState) {
	users.EXPECT().UpdateUserChatlist(gomock.Any(), account, gomock.Any()).
		DoAndReturn(func(_ any, _ string, st *model.ChatlistState) (*model.User, error) {
			*got = st
			return &model.User{Account: account, Chatlist: st}, nil
		})
}

func TestGetChatlist(t *testing.T) {
	tests := []struct {
		name string
		user *model.User
		err  error
		want *model.ChatlistState
		code errcode.Code
	}{
		{name: "never customized falls back to the built-in default", user: &model.User{Account: "alice"}, want: model.DefaultChatlistState()},
		{name: "stored state is returned verbatim", user: &model.User{Account: "alice", Chatlist: storedChatlist()}, want: storedChatlist()},
		{name: "unknown account is not found", user: nil, code: errcode.CodeNotFound},
		{name: "repository error", err: errors.New("db unavailable"), code: errcode.CodeInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, _ := newSvc(t)
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(tt.user, tt.err)
			got, err := svc.GetChatlist(ctx("alice", "site-a"))
			if tt.code != "" {
				requireCode(t, err, tt.code)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetChatlist_StoreErrorStaysRaw(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(nil, errors.New("db unavailable"))
	_, err := svc.GetChatlist(ctx("alice", "site-a"))
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "infra errors must stay raw, not be dressed up as an errcode")
}

func TestCreateChatlistSection_PlacesNewSectionAboveChats(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)

	got, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Work"})
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, stored, got, "the handler returns the persisted state")

	require.Len(t, stored.Sections, 5)
	created := stored.Sections[4]
	assert.Equal(t, "Work", created.Name)
	assert.False(t, created.BuiltIn, "client-created sections are never built-in")
	assert.True(t, idgen.IsValidUUIDv7(created.ID), "section id must be a UUIDv7 hex, got %q", created.ID)

	idx := slices.Index(stored.SectionOrder, created.ID)
	require.GreaterOrEqual(t, idx, 0, "new section missing from SectionOrder: %v", stored.SectionOrder)
	assert.Equal(t, model.SectionChats, stored.SectionOrder[idx+1], "new section must sit directly above chats")
	assert.Positive(t, stored.LastUpdatedAt, "the write must stamp the high-water mark")
}

func TestCreateChatlistSection_SortMode(t *testing.T) {
	tests := []struct {
		name string
		req  string
		want string
	}{
		{name: "omitted defaults to custom", req: "", want: model.SortModeCustom},
		{name: "custom honored", req: model.SortModeCustom, want: model.SortModeCustom},
		{name: "mostRecent honored", req: model.SortModeMostRecent, want: model.SortModeMostRecent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, pub := newSvc(t)
			expectChatlistFanout(pub, "alice")
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
			var stored *model.ChatlistState
			echoChatlistUpdate(users, "alice", &stored)

			_, err := svc.CreateChatlistSection(ctx("alice", "site-a"),
				models.ChatlistSectionCreateRequest{Name: "Work", SortMode: tt.req})
			require.NoError(t, err)
			require.Len(t, stored.Sections, 5)
			assert.Equal(t, tt.want, stored.Sections[4].SortMode)
		})
	}
}

func TestCreateChatlistSection_TrimsName(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)

	_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "  Work  "})
	require.NoError(t, err)
	assert.Equal(t, "Work", stored.Sections[4].Name, "surrounding whitespace is stripped before validation and storage")
}

// Rejected input must never reach the repository: newSvc's users mock has no
// expectations here, so gomock fails the test if a lookup or write happens.
func TestCreateChatlistSection_InvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		req    models.ChatlistSectionCreateRequest
		code   errcode.Code
		reason errcode.Reason
	}{
		{name: "empty name", req: models.ChatlistSectionCreateRequest{Name: ""}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "whitespace-only name trims to empty", req: models.ChatlistSectionCreateRequest{Name: "   "}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "name over 50 runes", req: models.ChatlistSectionCreateRequest{Name: strings.Repeat("x", 51)}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "consecutive spaces", req: models.ChatlistSectionCreateRequest{Name: "a  b"}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "disallowed character", req: models.ChatlistSectionCreateRequest{Name: "bad!"}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "unknown sortMode", req: models.ChatlistSectionCreateRequest{Name: "Work", SortMode: "weird"}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidSortMode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), tt.req)
			requireCode(t, err, tt.code)
			assert.True(t, errcode.HasReason(err, tt.reason), "want reason %q, got %v", tt.reason, err)
		})
	}
}

// A 50-rune name is the inclusive upper bound.
func TestCreateChatlistSection_MaxLengthNameAccepted(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	name := strings.Repeat("x", model.MaxSectionName)
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)
	_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, name, stored.Sections[4].Name)
}

func TestCreateChatlistSection_DuplicateCustomName(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	// The write is never expected: the duplicate is caught inside the mutation.
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
	_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Work"})
	requireCode(t, err, errcode.CodeConflict)
	assert.True(t, errcode.HasReason(err, errcode.UserChatlistDuplicateName))
}

// Uniqueness is scoped to custom sections, so a built-in's display name is not
// reserved — "Chats" may be reused for a custom section.
func TestCreateChatlistSection_BuiltinNameIsNotReserved(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)
	_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Chats"})
	require.NoError(t, err)
	assert.Equal(t, "Chats", stored.Sections[4].Name)
}

// Names differing only in case are distinct — uniqueness is case-sensitive.
func TestCreateChatlistSection_CaseDifferingNameAllowed(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)
	_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "work"})
	require.NoError(t, err)
	assert.Len(t, stored.Sections, 6)
}

// mutateChatlist's read-modify-write failure modes, exercised through create.
func TestCreateChatlistSection_RepositoryFailures(t *testing.T) {
	tests := []struct {
		name       string
		getUser    *model.User
		getErr     error
		expectSet  bool
		updateUser *model.User
		updateErr  error
		code       errcode.Code
	}{
		{name: "read fails", getErr: errors.New("db unavailable"), code: errcode.CodeInternal},
		{name: "unknown account on read", getUser: nil, code: errcode.CodeNotFound},
		{name: "write fails", getUser: &model.User{Account: "alice"}, expectSet: true, updateErr: errors.New("write failed"), code: errcode.CodeInternal},
		{name: "account vanished between read and write", getUser: &model.User{Account: "alice"}, expectSet: true, updateUser: nil, code: errcode.CodeNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No publish expectations: a failed mutation must fan nothing out.
			svc, _, users, _, _, _, _ := newSvc(t)
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(tt.getUser, tt.getErr)
			if tt.expectSet {
				users.EXPECT().UpdateUserChatlist(gomock.Any(), "alice", gomock.Any()).Return(tt.updateUser, tt.updateErr)
			}
			_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Work"})
			requireCode(t, err, tt.code)
		})
	}
}

// The repo is the source of truth for the reply: a write that returns a state
// different from the one sent must be what the caller and the fanouts see.
func TestCreateChatlistSection_ReturnsRepositoryState(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	persisted := storedChatlist()
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	users.EXPECT().UpdateUserChatlist(gomock.Any(), "alice", gomock.Any()).
		Return(&model.User{Account: "alice", Chatlist: persisted}, nil)
	got, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Work"})
	require.NoError(t, err)
	assert.Equal(t, persisted, got)
}

// A write that comes back with no chatlist sub-document (defensive: Mongo always
// returns it after a whole-object $set) still replies with the state just applied.
func TestCreateChatlistSection_WriteReturnsUserWithoutChatlist(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	users.EXPECT().UpdateUserChatlist(gomock.Any(), "alice", gomock.Any()).
		Return(&model.User{Account: "alice"}, nil)
	got, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Work"})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Sections, 5)
	assert.Equal(t, "Work", got.Sections[4].Name, "the locally applied state is the fallback reply")
}

func TestDeleteChatlistSection_RemovesCustomSection(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)

	_, err := svc.DeleteChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionDeleteRequest{SectionID: "c1"})
	require.NoError(t, err)
	assert.Nil(t, findSection(stored, "c1"), "deleted section must be gone from Sections")
	assert.NotContains(t, stored.SectionOrder, "c1", "deleted section must be gone from SectionOrder")
	assert.Equal(t, []string{model.SectionFavorites, model.SectionApps, model.SectionTeams, model.SectionChats},
		stored.SectionOrder, "the surviving order keeps its relative sequence")
}

func TestDeleteChatlistSection_Rejected(t *testing.T) {
	tests := []struct {
		name      string
		sectionID string
		state     *model.ChatlistState
		code      errcode.Code
		reason    errcode.Reason
	}{
		{name: "unknown section id", sectionID: "nope", state: storedChatlist(), code: errcode.CodeNotFound, reason: errcode.UserChatlistSectionNotFound},
		{name: "empty section id", sectionID: "", state: storedChatlist(), code: errcode.CodeNotFound, reason: errcode.UserChatlistSectionNotFound},
		{name: "built-in section", sectionID: model.SectionChats, state: storedChatlist(), code: errcode.CodeBadRequest, reason: errcode.UserChatlistBuiltinImmutable},
		{name: "custom section on a never-customized chatlist", sectionID: "c1", state: nil, code: errcode.CodeNotFound, reason: errcode.UserChatlistSectionNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No UpdateUserChatlist expectation: a rejected mutation must not write.
			svc, _, users, _, _, _, _ := newSvc(t)
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
				Return(&model.User{Account: "alice", Chatlist: tt.state}, nil)
			_, err := svc.DeleteChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionDeleteRequest{SectionID: tt.sectionID})
			requireCode(t, err, tt.code)
			assert.True(t, errcode.HasReason(err, tt.reason), "want reason %q, got %v", tt.reason, err)
		})
	}
}

func TestRenameChatlistSection_RenamesCustomSection(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)

	_, err := svc.RenameChatlistSection(ctx("alice", "site-a"),
		models.ChatlistSectionRenameRequest{SectionID: "c1", Name: "  Projects  "})
	require.NoError(t, err)
	sec := findSection(stored, "c1")
	require.NotNil(t, sec)
	assert.Equal(t, "Projects", sec.Name, "the new name is trimmed before storage")
	assert.Equal(t, model.SortModeCustom, sec.SortMode, "rename must not disturb sortMode")
}

// Renaming a section to the name it already has is a no-op, not a self-collision.
func TestRenameChatlistSection_ToItsOwnNameSucceeds(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)
	_, err := svc.RenameChatlistSection(ctx("alice", "site-a"),
		models.ChatlistSectionRenameRequest{SectionID: "c1", Name: "Work"})
	require.NoError(t, err)
	assert.Equal(t, "Work", findSection(stored, "c1").Name)
}

func TestRenameChatlistSection_Rejected(t *testing.T) {
	twoCustom := storedChatlist()
	twoCustom.Sections = append(twoCustom.Sections, customSection("c2", "Personal", model.SortModeCustom))
	twoCustom.SectionOrder = append(twoCustom.SectionOrder, "c2")

	tests := []struct {
		name      string
		req       models.ChatlistSectionRenameRequest
		state     *model.ChatlistState
		expectGet bool
		code      errcode.Code
		reason    errcode.Reason
	}{
		{name: "invalid name is rejected before any lookup", req: models.ChatlistSectionRenameRequest{SectionID: "c1", Name: "bad!"}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "blank name is rejected before any lookup", req: models.ChatlistSectionRenameRequest{SectionID: "c1", Name: "  "}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidName},
		{name: "unknown section id", req: models.ChatlistSectionRenameRequest{SectionID: "nope", Name: "Projects"}, state: storedChatlist(), expectGet: true, code: errcode.CodeNotFound, reason: errcode.UserChatlistSectionNotFound},
		{name: "built-in section", req: models.ChatlistSectionRenameRequest{SectionID: model.SectionTeams, Name: "Squads"}, state: storedChatlist(), expectGet: true, code: errcode.CodeBadRequest, reason: errcode.UserChatlistBuiltinImmutable},
		{name: "name taken by another custom section", req: models.ChatlistSectionRenameRequest{SectionID: "c2", Name: "Work"}, state: twoCustom, expectGet: true, code: errcode.CodeConflict, reason: errcode.UserChatlistDuplicateName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, _ := newSvc(t)
			if tt.expectGet {
				users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
					Return(&model.User{Account: "alice", Chatlist: tt.state}, nil)
			}
			_, err := svc.RenameChatlistSection(ctx("alice", "site-a"), tt.req)
			requireCode(t, err, tt.code)
			assert.True(t, errcode.HasReason(err, tt.reason), "want reason %q, got %v", tt.reason, err)
		})
	}
}

func TestReorderChatlistSections_ReplacesOrder(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)

	want := []string{"c1", model.SectionChats, model.SectionTeams, model.SectionApps, model.SectionFavorites}
	_, err := svc.ReorderChatlistSections(ctx("alice", "site-a"), models.ChatlistSectionReorderRequest{SectionOrder: want})
	require.NoError(t, err)
	assert.Equal(t, want, stored.SectionOrder)
	assert.Len(t, stored.Sections, 5, "reordering must not add or drop definitions")
}

// Reordering a never-customized chatlist works off the default order.
func TestReorderChatlistSections_OnDefaultState(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectChatlistFanout(pub, "alice")
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)
	want := []string{model.SectionChats, model.SectionTeams, model.SectionApps, model.SectionFavorites}
	_, err := svc.ReorderChatlistSections(ctx("alice", "site-a"), models.ChatlistSectionReorderRequest{SectionOrder: want})
	require.NoError(t, err)
	assert.Equal(t, want, stored.SectionOrder)
}

func TestReorderChatlistSections_NonPermutation(t *testing.T) {
	full := []string{model.SectionFavorites, model.SectionApps, model.SectionTeams, "c1", model.SectionChats}
	tests := []struct {
		name  string
		order []string
	}{
		{name: "nil order", order: nil},
		{name: "empty order", order: []string{}},
		{name: "missing a section", order: full[:4]},
		{name: "unknown section id", order: []string{model.SectionFavorites, model.SectionApps, model.SectionTeams, "c1", "ghost"}},
		{name: "duplicate section id", order: []string{model.SectionFavorites, model.SectionApps, model.SectionTeams, "c1", "c1"}},
		{name: "extra section id", order: append(slices.Clone(full), "extra")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No write expectation: an invalid order must be rejected before persisting.
			svc, _, users, _, _, _, _ := newSvc(t)
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
				Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
			_, err := svc.ReorderChatlistSections(ctx("alice", "site-a"), models.ChatlistSectionReorderRequest{SectionOrder: tt.order})
			requireCode(t, err, errcode.CodeBadRequest)
			assert.True(t, errcode.HasReason(err, errcode.UserChatlistInvalidOrder), "want invalid-order reason, got %v", err)
		})
	}
}

func TestSetChatlistSectionSortMode(t *testing.T) {
	tests := []struct {
		name      string
		sectionID string
		mode      string
	}{
		{name: "custom section to mostRecent", sectionID: "c1", mode: model.SortModeMostRecent},
		{name: "built-in section to custom", sectionID: model.SectionChats, mode: model.SortModeCustom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, pub := newSvc(t)
			expectChatlistFanout(pub, "alice")
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
				Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
			var stored *model.ChatlistState
			echoChatlistUpdate(users, "alice", &stored)

			_, err := svc.SetChatlistSectionSortMode(ctx("alice", "site-a"),
				models.ChatlistSectionSetSortModeRequest{SectionID: tt.sectionID, SortMode: tt.mode})
			require.NoError(t, err)
			sec := findSection(stored, tt.sectionID)
			require.NotNil(t, sec)
			assert.Equal(t, tt.mode, sec.SortMode)
		})
	}
}

func TestSetChatlistSectionSortMode_Rejected(t *testing.T) {
	tests := []struct {
		name      string
		req       models.ChatlistSectionSetSortModeRequest
		expectGet bool
		code      errcode.Code
		reason    errcode.Reason
	}{
		{name: "unknown sortMode is rejected before any lookup", req: models.ChatlistSectionSetSortModeRequest{SectionID: "c1", SortMode: "weird"}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidSortMode},
		{name: "empty sortMode is rejected before any lookup", req: models.ChatlistSectionSetSortModeRequest{SectionID: "c1", SortMode: ""}, code: errcode.CodeBadRequest, reason: errcode.UserChatlistInvalidSortMode},
		{name: "unknown section id", req: models.ChatlistSectionSetSortModeRequest{SectionID: "nope", SortMode: model.SortModeCustom}, expectGet: true, code: errcode.CodeNotFound, reason: errcode.UserChatlistSectionNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, _ := newSvc(t)
			if tt.expectGet {
				users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
					Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
			}
			_, err := svc.SetChatlistSectionSortMode(ctx("alice", "site-a"), tt.req)
			requireCode(t, err, tt.code)
			assert.True(t, errcode.HasReason(err, tt.reason), "want reason %q, got %v", tt.reason, err)
		})
	}
}

// Both fanouts carry the full post-update state and one shared timestamp, which
// is also the stored high-water mark — client event and cross-site replica must
// agree on ordering. site-a is self and gets no inbox event.
func TestChatlistMutation_FansOutOnceToClientAndOtherSites(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	users.EXPECT().GetUserChatlist(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	var stored *model.ChatlistState
	echoChatlistUpdate(users, "alice", &stored)

	var clientEvt model.ChatlistUpdateEvent
	pub.EXPECT().Publish(gomock.Any(), subject.ChatlistUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			require.NoError(t, json.Unmarshal(data, &clientEvt))
			return nil
		})
	var inboxPayload model.UserChatlistUpdated
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserChatlistUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.InboxUserChatlistUpdated, evt.Type)
			assert.Equal(t, "site-a", evt.SiteID)
			assert.Equal(t, "site-b", evt.DestSiteID)
			require.NoError(t, json.Unmarshal(evt.Payload, &inboxPayload))
			return nil
		})

	_, err := svc.CreateChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionCreateRequest{Name: "Work"})
	require.NoError(t, err)

	assert.Equal(t, *stored, clientEvt.Chatlist, "the client event carries the full post-update state")
	assert.Equal(t, "alice", inboxPayload.Account)
	assert.Equal(t, *stored, inboxPayload.Chatlist, "the replica event carries the full post-update state")
	assert.NotZero(t, clientEvt.Timestamp)
	assert.Equal(t, clientEvt.Timestamp, inboxPayload.Timestamp, "both fanouts share one timestamp")
	assert.Equal(t, clientEvt.Timestamp, stored.LastUpdatedAt, "the stored high-water mark is that same timestamp")
}

func TestChatlistMutation_PublishFailuresAreBestEffort(t *testing.T) {
	tests := []struct {
		name      string
		clientErr error
		inboxErr  error
	}{
		{name: "client fanout fails", clientErr: errors.New("nats down")},
		{name: "inbox fanout fails", inboxErr: errors.New("no responders")},
		{name: "both fail", clientErr: errors.New("nats down"), inboxErr: errors.New("no responders")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, pub := newSvc(t)
			users.EXPECT().GetUserChatlist(gomock.Any(), "alice").
				Return(&model.User{Account: "alice", Chatlist: storedChatlist()}, nil)
			var stored *model.ChatlistState
			echoChatlistUpdate(users, "alice", &stored)
			pub.EXPECT().Publish(gomock.Any(), subject.ChatlistUpdate("alice"), gomock.Any()).Return(tt.clientErr)
			pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserChatlistUpdated), gomock.Any()).
				Return(tt.inboxErr)

			got, err := svc.DeleteChatlistSection(ctx("alice", "site-a"), models.ChatlistSectionDeleteRequest{SectionID: "c1"})
			require.NoError(t, err, "a fanout failure must not fail the mutation")
			assert.Equal(t, stored, got)
		})
	}
}

// The inbox fanout skips this site and any blank entry in ALL_SITE_IDS, and
// publishes once per remaining peer.
func TestPublishChatlistInbox_SkipsSelfAndBlankSites(t *testing.T) {
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockEventPublisher(ctrl)
	svc := &UserService{pub: pub, siteID: "site-a", allSiteIDs: []string{"site-a", "", "site-b", "site-c"}}

	var subjects []string
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Times(2).
		DoAndReturn(func(_ any, subj string, _ []byte) error {
			subjects = append(subjects, subj)
			return nil
		})
	svc.publishChatlistInbox(ctx("alice", "site-a"), "alice", model.DefaultChatlistState(), 42)
	assert.ElementsMatch(t, []string{
		subject.InboxExternal("site-b", model.InboxUserChatlistUpdated),
		subject.InboxExternal("site-c", model.InboxUserChatlistUpdated),
	}, subjects)
}

// A single-site deployment has no peers, so no inbox event is published at all.
func TestPublishChatlistInbox_SingleSitePublishesNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockEventPublisher(ctrl)
	// pub has no Publish expectation: gomock fails the test if one is attempted.
	svc := &UserService{pub: pub, siteID: "site-a", allSiteIDs: []string{"site-a"}}
	svc.publishChatlistInbox(ctx("alice", "site-a"), "alice", model.DefaultChatlistState(), 42)
}

func TestValidateSectionName(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		for _, n := range []string{"Work", "團隊", "a-b_c.d/(e)", "A B", strings.Repeat("x", model.MaxSectionName)} {
			assert.NoError(t, validateSectionName(n), "name %q must be accepted", n)
		}
	})
	t.Run("rejected", func(t *testing.T) {
		for _, n := range []string{"", strings.Repeat("x", model.MaxSectionName+1), "a  b", "bad!", "no@sign", "semi;colon"} {
			err := validateSectionName(n)
			require.Error(t, err, "name %q must be rejected", n)
			assert.True(t, errcode.HasReason(err, errcode.UserChatlistInvalidName), "name %q: wrong reason: %v", n, err)
		}
	})
}

func TestValidateSortMode(t *testing.T) {
	assert.NoError(t, validateSortMode(model.SortModeCustom))
	assert.NoError(t, validateSortMode(model.SortModeMostRecent))
	err := validateSortMode("weird")
	require.Error(t, err)
	assert.True(t, errcode.HasReason(err, errcode.UserChatlistInvalidSortMode))
}

func TestIsPermutation(t *testing.T) {
	assert.True(t, isPermutation([]string{"a", "b", "c"}, []string{"c", "a", "b"}))
	assert.False(t, isPermutation([]string{"a", "b"}, []string{"a", "a"}), "duplicates are not a permutation")
	assert.False(t, isPermutation([]string{"a", "b"}, []string{"a"}), "a short list is not a permutation")
}

func TestInsertAboveChatsAndRemove(t *testing.T) {
	st := model.DefaultChatlistState()
	insertAboveChats(st, "sec-1")
	idx := slices.Index(st.SectionOrder, "sec-1")
	require.GreaterOrEqual(t, idx, 0, "sec-1 missing from order: %v", st.SectionOrder)
	assert.Equal(t, model.SectionChats, st.SectionOrder[idx+1], "sec-1 must land directly above chats")

	st.Sections = append(st.Sections, model.ChatlistSection{ID: "sec-1", Name: "S1"})
	removeSection(st, "sec-1")
	assert.NotContains(t, st.SectionOrder, "sec-1")
	assert.Nil(t, findSection(st, "sec-1"))
}

// With no built-in "chats" section to anchor on, a new id appends to the tail.
func TestInsertAboveChats_NoChatsSection(t *testing.T) {
	st := &model.ChatlistState{SectionOrder: []string{model.SectionFavorites, model.SectionTeams}}
	insertAboveChats(st, "sec-1")
	assert.Equal(t, []string{model.SectionFavorites, model.SectionTeams, "sec-1"}, st.SectionOrder)
}

func TestFindSection(t *testing.T) {
	st := storedChatlist()
	sec := findSection(st, "c1")
	require.NotNil(t, sec)
	assert.Equal(t, "Work", sec.Name)
	sec.Name = "Renamed"
	assert.Equal(t, "Renamed", st.Sections[4].Name, "findSection must return a mutable pointer into the state")
	assert.Nil(t, findSection(st, "missing"))
	assert.Nil(t, findSection(&model.ChatlistState{}, "c1"), "an empty chatlist finds nothing")
}

func TestHasCustomName(t *testing.T) {
	st := &model.ChatlistState{Sections: []model.ChatlistSection{
		{ID: "b1", Name: "Chats", BuiltIn: true},
		customSection("c1", "Work", model.SortModeCustom),
	}}
	assert.True(t, hasCustomName(st, "Work", ""), "an existing custom name collides")
	assert.False(t, hasCustomName(st, "Work", "c1"), "a section never collides with itself on rename")
	assert.False(t, hasCustomName(st, "work", ""), "name matching is case-sensitive")
	assert.False(t, hasCustomName(st, "Chats", ""), "built-in names are not reserved for custom sections")
}
