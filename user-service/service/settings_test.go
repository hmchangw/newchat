package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

type settingsRepositoryFake struct {
	getResult *model.UserSettings
	getErr    error
	setResult *model.UserSettings
	setErr    error

	getCalls  int
	setCalls  int
	account   string
	siteID    string
	data      json.RawMessage
	ifVersion *int64
}

func (f *settingsRepositoryFake) GetUserSettings(_ context.Context, account, siteID string) (*model.UserSettings, error) {
	f.getCalls++
	f.account = account
	f.siteID = siteID
	return f.getResult, f.getErr
}

func (f *settingsRepositoryFake) SetUserSettings(_ context.Context, account, siteID string, data json.RawMessage, ifVersion *int64) (*model.UserSettings, error) {
	f.setCalls++
	f.account = account
	f.siteID = siteID
	f.data = data
	f.ifVersion = ifVersion
	return f.setResult, f.setErr
}

func newSettingsService(repo SettingsRepository) *UserService {
	return &UserService{settings: repo, siteID: "site-a", maxSettingsBytes: defaultMaxSettingsBytes}
}

// AC-3.1: a valid caller receives the stored settings view.
func TestGetUserSettings_AC_3_1_ReturnsStoredView(t *testing.T) {
	updatedAt := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	stored := &model.UserSettings{
		Account:   "alice",
		SiteID:    "site-a",
		Data:      json.RawMessage(`{"theme":"dark"}`),
		Version:   7,
		UpdatedAt: updatedAt,
	}
	repo := &settingsRepositoryFake{getResult: stored}

	got, err := newSettingsService(repo).GetUserSettings(ctx("alice", "site-a"))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, models.UserSettingsView{
		Account:   "alice",
		SiteID:    "site-a",
		Data:      json.RawMessage(`{"theme":"dark"}`),
		Version:   7,
		UpdatedAt: updatedAt,
	}, *got)
	assert.Equal(t, 1, repo.getCalls)
	assert.Equal(t, "alice", repo.account)
	assert.Equal(t, "site-a", repo.siteID)
}

// AC-3.2: a missing settings document returns not_found.
func TestGetUserSettings_AC_3_2_MissingReturnsNotFound(t *testing.T) {
	repo := &settingsRepositoryFake{}

	_, err := newSettingsService(repo).GetUserSettings(ctx("alice", "site-a"))

	requireCode(t, err, errcode.CodeNotFound)
	assert.Equal(t, 1, repo.getCalls)
}

func TestGetUserSettings_RepositoryErrorIsWrapped(t *testing.T) {
	repoErr := errors.New("database unavailable")
	repo := &settingsRepositoryFake{getErr: repoErr}

	_, err := newSettingsService(repo).GetUserSettings(ctx("alice", "site-a"))

	require.Error(t, err)
	assert.ErrorIs(t, err, repoErr)
	assert.Equal(t, errcode.CodeInternal, errcode.Classify(context.Background(), err).Code)
}

// AC-3.3: a valid object is delegated and the updated view is returned.
func TestSetUserSettings_AC_3_3_StoresValidObjectAndReturnsView(t *testing.T) {
	data := json.RawMessage(`{"theme":"dark","density":"compact"}`)
	stored := &model.UserSettings{
		Account: "alice",
		SiteID:  "site-a",
		Data:    data,
		Version: 2,
	}
	repo := &settingsRepositoryFake{setResult: stored}

	got, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: data})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "alice", got.Account)
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, data, got.Data)
	assert.Equal(t, int64(2), got.Version)
	assert.Equal(t, data, repo.data)
	assert.Nil(t, repo.ifVersion)
	assert.Equal(t, 1, repo.setCalls)
}

// AC-3.4: missing and non-object data are rejected before repository access.
func TestSetUserSettings_AC_3_4_RejectsMissingOrNonObjectDataBeforeRepository(t *testing.T) {
	tests := []struct {
		name string
		data json.RawMessage
	}{
		{name: "missing", data: nil},
		{name: "null", data: json.RawMessage("null")},
		{name: "array", data: json.RawMessage(`["dark"]`)},
		{name: "string", data: json.RawMessage(`"dark"`)},
		{name: "number", data: json.RawMessage("1")},
		{name: "boolean", data: json.RawMessage("true")},
		{name: "empty string", data: json.RawMessage("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &settingsRepositoryFake{}
			_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: tt.data})

			requireCode(t, err, errcode.CodeBadRequest)
			assert.Equal(t, 0, repo.setCalls)
		})
	}
}

// AC-3.4: empty JSON objects are accepted as valid data and reach the repository.
func TestSetUserSettings_EmptyObjectAccepts(t *testing.T) {
	repo := &settingsRepositoryFake{setResult: &model.UserSettings{Account: "alice", SiteID: "site-a", Data: json.RawMessage("{}"), Version: 1}}

	_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: json.RawMessage("{}")})

	require.NoError(t, err)
	assert.Equal(t, 1, repo.setCalls)
	assert.Equal(t, json.RawMessage("{}"), repo.data)
}

// AC-3.5: serialized data over 64 KiB returns bad_request before repository access.
func TestSetUserSettings_AC_3_5_RejectsOversizedDataBeforeRepository(t *testing.T) {
	data := json.RawMessage(`{"value":"` + string(make([]byte, 64*1024)) + `"}`)
	require.Greater(t, len(data), 64*1024)
	repo := &settingsRepositoryFake{}

	_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: data})

	requireCode(t, err, errcode.CodeBadRequest)
	var coded *errcode.Error
	require.ErrorAs(t, err, &coded)
	assert.Equal(t, "data too large", coded.Message)
	assert.Equal(t, 0, repo.setCalls)
}

// AC-3.6: a matching ifVersion is passed through and succeeds.
func TestSetUserSettings_AC_3_6_MatchingIfVersionSucceeds(t *testing.T) {
	version := int64(7)
	data := json.RawMessage(`{"theme":"dark"}`)
	repo := &settingsRepositoryFake{setResult: &model.UserSettings{Account: "alice", SiteID: "site-a", Data: data, Version: 8}}

	got, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: data, IfVersion: &version})

	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, repo.ifVersion)
	assert.Equal(t, version, *repo.ifVersion)
	assert.Equal(t, int64(8), got.Version)
}

// AC-3.6: a stale ifVersion returns conflict.
func TestSetUserSettings_AC_3_6_StaleIfVersionReturnsConflict(t *testing.T) {
	version := int64(6)
	repo := &settingsRepositoryFake{setErr: errcode.Conflict("user settings version conflict")}

	_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{
		Data:      json.RawMessage(`{"theme":"light"}`),
		IfVersion: &version,
	})

	requireCode(t, err, errcode.CodeConflict)
	assert.Equal(t, 1, repo.setCalls)
}

// AC-4.3: a configured 32 KiB limit rejects payloads above 32 KiB before persistence.
func TestSetUserSettings_AC_4_3_UsesConfiguredLimit(t *testing.T) {
	data := json.RawMessage(`{"value":"` + strings.Repeat("a", 32*1024) + `"}`)
	require.Greater(t, len(data), 32*1024)
	require.Less(t, len(data), 64*1024)
	repo := &settingsRepositoryFake{}
	svc := &UserService{settings: repo, siteID: "site-a", maxSettingsBytes: 32 * 1024}

	_, err := svc.SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: data})

	requireCode(t, err, errcode.CodeBadRequest)
	var coded *errcode.Error
	require.ErrorAs(t, err, &coded)
	assert.Equal(t, "data too large", coded.Message)
	assert.Equal(t, 0, repo.setCalls)
}

func TestSetUserSettings_RepositoryErrorIsWrapped(t *testing.T) {
	repoErr := errors.New("database unavailable")
	repo := &settingsRepositoryFake{setErr: repoErr}

	_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{
		Data: json.RawMessage(`{"theme":"dark"}`),
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, repoErr)
	assert.Equal(t, errcode.CodeInternal, errcode.Classify(context.Background(), err).Code)
}

// AC-3.7: the subject account is the caller identity used for SET.
func TestUserSettings_AC_3_7_SetUsesSubjectAccountAsCallerIdentity(t *testing.T) {
	data := json.RawMessage(`{"theme":"dark"}`)
	repo := &settingsRepositoryFake{setResult: &model.UserSettings{Account: "alice", SiteID: "site-a", Data: data, Version: 1}}

	_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: data})

	require.NoError(t, err)
	assert.Equal(t, "alice", repo.account, "the router's account subject parameter is the only caller identity available to this handler")
	assert.NotEqual(t, "bob", repo.account)
}

// AC-3.7: the subject account is the caller identity used for GET.
func TestUserSettings_AC_3_7_GetUsesSubjectAccountAsCallerIdentity(t *testing.T) {
	repo := &settingsRepositoryFake{getResult: &model.UserSettings{Account: "alice", SiteID: "site-a", Data: json.RawMessage("{}"), Version: 1}}

	_, err := newSettingsService(repo).GetUserSettings(ctx("alice", "site-a"))

	require.NoError(t, err)
	assert.Equal(t, "alice", repo.account)
	assert.Equal(t, "site-a", repo.siteID)
}

// AC (nil-repo): SetUserSettings returns internal error when repo returns nil,nil.
func TestSetUserSettings_RepoReturnsNil_YieldsInternalError(t *testing.T) {
	repo := &settingsRepositoryFake{}

	_, err := newSettingsService(repo).SetUserSettings(ctx("alice", "site-a"), models.SetUserSettingsRequest{Data: json.RawMessage(`{"k":"v"}`)})

	require.Error(t, err)
	// A raw fmt.Errorf (no *errcode.Error) classifies as CodeInternal.
	assert.Equal(t, errcode.CodeInternal, errcode.Classify(context.Background(), err).Code)
}

// AC (empty account): handlers reject empty account with BadRequest.
func TestGetUserSettings_EmptyAccountBadRequest(t *testing.T) {
	repo := &settingsRepositoryFake{}

	_, err := newSettingsService(repo).GetUserSettings(ctx("", "site-a"))

	requireCode(t, err, errcode.CodeBadRequest)
	assert.Equal(t, 0, repo.getCalls)
}

func TestSetUserSettings_EmptyAccountBadRequest(t *testing.T) {
	repo := &settingsRepositoryFake{}

	_, err := newSettingsService(repo).SetUserSettings(ctx("", "site-a"), models.SetUserSettingsRequest{Data: json.RawMessage(`{"k":"v"}`)})

	requireCode(t, err, errcode.CodeBadRequest)
	assert.Equal(t, 0, repo.setCalls)
}
