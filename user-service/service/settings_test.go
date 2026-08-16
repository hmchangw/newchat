package service

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func ptrStr(s string) *string { return &s }

// expectInbox allows the cross-site settings fanout the client-event tests don't assert on.
func expectInbox(pub *mocks.MockEventPublisher) {
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).Return(nil).AnyTimes()
}

func TestGetSettings_NeverSetReturnsEmptyObject(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserSettings(gomock.Any(), "alice").Return(&model.User{}, nil)
	resp, err := svc.GetSettings(ctx("alice", "site-a"))
	require.NoError(t, err)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.JSONEq(t, `{"permissions":{"external.image.view":false}}`, string(data),
		"never-set settings must serialize as {} — no injected defaults; permissions is always present")
}

// A nil user is the repo's "no active user" signal; a matched user with nothing
// stored has nil sub-documents and must NOT read as not-found.
func TestGetSettings_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserSettings(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.GetSettings(ctx("ghost", "site-a"))
	requireCode(t, err, errcode.CodeNotFound)
}

func TestGetSettings_ReturnsStoredSubDocument(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	stored := &model.UserSettings{FullWidth: ptrBool(true), TranslateMessageInto: ptrStr("en-US")}
	users.EXPECT().GetUserSettings(gomock.Any(), "alice").Return(&model.User{Settings: stored}, nil)
	resp, err := svc.GetSettings(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Equal(t, *stored, resp.UserSettings)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.JSONEq(t, `{"fullWidth":true,"translateMessageInto":"en-US","permissions":{"external.image.view":false}}`,
		string(data), "stored settings stay inlined at the top level, unchanged beside the new key")
}

// Windows are evaluated at read time, so a snapshot flips without any cron.
func TestGetSettings_EvaluatesPermissionsAtReadTime(t *testing.T) {
	now := time.Now().UTC()
	snapshot := func(granted bool, from, until time.Time) *model.UserPermissions {
		return &model.UserPermissions{ExternalImageView: &model.PermissionState{
			Granted: granted, EffectiveFrom: &from, ExpiresAt: &until, UpdatedAt: now,
		}}
	}
	tests := []struct {
		name  string
		perms *model.UserPermissions
		want  bool
	}{
		{"no snapshot", nil, false},
		{"granted in window", snapshot(true, now.Add(-time.Hour), now.Add(time.Hour)), true},
		{"expired", snapshot(true, now.Add(-2*time.Hour), now.Add(-time.Hour)), false},
		{"not yet effective", snapshot(true, now.Add(time.Hour), now.Add(2*time.Hour)), false},
		{"revoked", snapshot(false, now.Add(-time.Hour), now.Add(time.Hour)), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, users, _, _, _, _ := newSvc(t)
			users.EXPECT().GetUserSettings(gomock.Any(), "alice").Return(&model.User{Permissions: tt.perms}, nil)
			resp, err := svc.GetSettings(ctx("alice", "site-a"))
			require.NoError(t, err)
			assert.Equal(t, map[model.PermissionKey]bool{model.PermissionExternalImageView: tt.want}, resp.Permissions)
		})
	}
}

func TestGetSettings_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserSettings(gomock.Any(), "alice").Return(nil, errors.New("db unavailable"))
	_, err := svc.GetSettings(ctx("alice", "site-a"))
	// Raw wrapped error — classified to the generic boundary code by the router.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "store errors must stay raw, not pre-classified")
}

func TestSetSettings_PartialPassesOnlySentFields(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectInbox(pub)
	updated := &model.UserSettings{FullWidth: ptrBool(true), MuteAllNotifications: ptrBool(false)}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ any, _ string, set *model.UserSettings, _ time.Time) (*model.User, error) {
			require.NotNil(t, set.FullWidth)
			assert.True(t, *set.FullWidth)
			assert.Nil(t, set.TranslateMessageInto, "unsent fields must not reach the repo")
			assert.Nil(t, set.MuteAllNotifications)
			return &model.User{Settings: updated}, nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)
	resp, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	require.NoError(t, err)
	assert.Equal(t, updated, resp)
}

func TestSetSettings_PublishesFullPostUpdateSettings(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectInbox(pub)
	updated := &model.UserSettings{FullWidth: ptrBool(true), TranslateMessageInto: ptrStr("ja")}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
		Return(&model.User{Settings: updated}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Positive(t, evt.Timestamp)
			assert.Equal(t, *updated, evt.Settings, "event must carry the full post-update settings")
			return nil
		})
	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{TranslateMessageInto: ptrStr("ja")},
	})
	require.NoError(t, err)
}

func TestSetSettings_PublishFailureIsBestEffort(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectInbox(pub)
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{FullWidth: ptrBool(true)}}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		Return(errors.New("nats down"))
	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	require.NoError(t, err, "fanout failure must not fail the set")
}

func TestSetSettings_EmptyRequest(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSetSettings_InvalidTranslateTag(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	for _, tag := range []string{"en_US", "-en", "en-", "1en", "en US"} {
		_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
			UserSettings: model.UserSettings{TranslateMessageInto: &tag},
		})
		requireCode(t, err, errcode.CodeBadRequest)
	}
}

func TestSetSettings_ValidTranslateTags(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectInbox(pub)
	for _, tag := range []string{"en", "en-US", "zh-Hant-TW", "ja", ""} { // "" = translation off
		users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
			Return(&model.User{Settings: &model.UserSettings{TranslateMessageInto: &tag}}, nil)
		pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)
		_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
			UserSettings: model.UserSettings{TranslateMessageInto: &tag},
		})
		require.NoError(t, err, "tag %q must be accepted", tag)
	}
}

func TestSetSettings_InvalidEnums(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	bad := []model.UserSettings{
		{ThemePreference: ptrStr("blue")},
		{ThemePreference: ptrStr("")},
		{InitialChatScrollPosition: ptrStr("top")},
		{InitialChatScrollPosition: ptrStr("")},
	}
	for _, s := range bad {
		_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{UserSettings: s})
		requireCode(t, err, errcode.CodeBadRequest)
	}
}

func TestSetSettings_ValidEnums(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	expectInbox(pub)
	for _, s := range []model.UserSettings{
		{ThemePreference: ptrStr(model.ThemePreferenceSystem)},
		{ThemePreference: ptrStr(model.ThemePreferenceLight)},
		{ThemePreference: ptrStr(model.ThemePreferenceDark)},
		{InitialChatScrollPosition: ptrStr(model.InitialChatScrollLastRead)},
		{InitialChatScrollPosition: ptrStr(model.InitialChatScrollNewest)},
	} {
		settings := s
		users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
			Return(&model.User{Settings: &settings}, nil)
		pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)
		_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{UserSettings: settings})
		require.NoError(t, err)
	}
}

func TestSetSettings_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().UpdateUserSettings(gomock.Any(), "ghost", gomock.Any(), gomock.Any()).Return(nil, nil)
	_, err := svc.SetSettings(ctx("ghost", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestSetSettings_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).Return(nil, errors.New("db unavailable"))
	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	// Raw wrapped error — classified to the generic boundary code by the router.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "store errors must stay raw, not pre-classified")
}

func TestSetSettings_FansOutToOtherSitesOnly(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	mute := true
	updated := &model.UserSettings{MuteAllNotifications: &mute}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).Return(&model.User{Settings: updated}, nil)

	var clientTS int64
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			return nil
		})
	// site-a is self and must be skipped; only site-b gets an inbox event.
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, "site-a", evt.SiteID)
			assert.Equal(t, "site-b", evt.DestSiteID)
			var p model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &p))
			assert.Equal(t, "alice", p.Account)
			assert.Equal(t, *updated, p.Settings, "inbox event must carry the full post-update settings")
			assert.Equal(t, clientTS, p.Timestamp, "both fanouts must share one timestamp")
			return nil
		})

	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{MuteAllNotifications: &mute},
	})
	require.NoError(t, err)
}

// One mock backs both publishers, so the two fanouts are told apart by subject.
// They must carry the SAME timestamp: the client event and the cross-site replica
// have to agree on ordering.
func TestSetSettings_BothFanoutsShareOneTimestamp(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)

	updated := &model.UserSettings{FullWidth: ptrBool(true)}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
		Return(&model.User{Settings: updated}, nil)

	var clientTS, inboxTS int64
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			inboxTS = payload.Timestamp
			return nil
		})

	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	require.NoError(t, err)
	assert.NotZero(t, clientTS)
	assert.Equal(t, clientTS, inboxTS)
}

func TestSetSettings_InboxPublishFailureIsBestEffort(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{FullWidth: ptrBool(true)}}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		Return(errors.New("no responders"))
	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	require.NoError(t, err, "inbox fanout failure must not fail the set")
}
