package service

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

func ptrStr(s string) *string { return &s }

func TestGetSettings_NeverSetReturnsEmptyObject(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserSettings(gomock.Any(), "alice").Return(&model.User{}, nil)
	resp, err := svc.GetSettings(ctx("alice", "site-a"))
	require.NoError(t, err)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(data), "never-set settings must serialize as {} — no injected defaults")
}

func TestGetSettings_ReturnsStoredSubDocument(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	stored := &model.UserSettings{FullWidth: ptrBool(true), TranslateMessageInto: ptrStr("en-US")}
	users.EXPECT().GetUserSettings(gomock.Any(), "alice").Return(&model.User{Settings: stored}, nil)
	resp, err := svc.GetSettings(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Equal(t, stored, resp)
}

func TestGetSettings_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserSettings(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.GetSettings(ctx("ghost", "site-a"))
	requireCode(t, err, errcode.CodeNotFound)
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
	updated := &model.UserSettings{FullWidth: ptrBool(true), MuteAllNotifications: ptrBool(false)}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any()).
		DoAndReturn(func(_ any, _ string, set *model.UserSettings) (*model.User, error) {
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
	updated := &model.UserSettings{FullWidth: ptrBool(true), TranslateMessageInto: ptrStr("ja")}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any()).
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
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any()).
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
	for _, tag := range []string{"en", "en-US", "zh-Hant-TW", "ja", ""} { // "" = translation off
		users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any()).
			Return(&model.User{Settings: &model.UserSettings{TranslateMessageInto: &tag}}, nil)
		pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)
		_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
			UserSettings: model.UserSettings{TranslateMessageInto: &tag},
		})
		require.NoError(t, err, "tag %q must be accepted", tag)
	}
}

func TestSetSettings_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().UpdateUserSettings(gomock.Any(), "ghost", gomock.Any()).Return(nil, nil)
	_, err := svc.SetSettings(ctx("ghost", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestSetSettings_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any()).Return(nil, errors.New("db unavailable"))
	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	// Raw wrapped error — classified to the generic boundary code by the router.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "store errors must stay raw, not pre-classified")
}
