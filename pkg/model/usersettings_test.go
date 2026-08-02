package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Never-set settings must marshal as {} — the server never injects defaults.
func TestUserSettings_EmptyMarshalsAsEmptyObject(t *testing.T) {
	data, err := json.Marshal(UserSettings{})
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(data))
}

// A partial settings object round-trips with only the sent fields present.
func TestUserSettings_PartialRoundTrip(t *testing.T) {
	fw, lang := true, "en-US"
	in := UserSettings{FullWidth: &fw, TranslateMessageInto: &lang}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	assert.JSONEq(t, `{"fullWidth":true,"translateMessageInto":"en-US"}`, string(data))

	var out UserSettings
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
	assert.Nil(t, out.MuteAllNotifications)
}

// The renamed + added fields marshal under their new wire keys.
func TestUserSettings_ThemeAndScrollWireKeys(t *testing.T) {
	theme, scroll, preview := ThemePreferenceDark, InitialChatScrollNewest, true
	in := UserSettings{ThemePreference: &theme, InitialChatScrollPosition: &scroll, MessagePreviewEnabled: &preview}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	assert.JSONEq(t, `{"themePreference":"dark","initialChatScrollPosition":"newest","messagePreviewEnabled":true}`, string(data))

	var out UserSettings
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

// BSON round-trip guards the bson tags used by the Mongo store + cross-site replica —
// a JSON-only test wouldn't catch a bson-tag regression on the new/renamed fields.
func TestUserSettings_BSONRoundTrip(t *testing.T) {
	theme, scroll := ThemePreferenceDark, InitialChatScrollLastRead
	preview, priority, inCall := true, true, false
	in := UserSettings{
		ThemePreference:                  &theme,
		MessagePreviewEnabled:            &preview,
		InitialChatScrollPosition:        &scroll,
		AlwaysAllowPriorityNotifications: &priority,
		ShowNotificationsInCall:          &inCall,
	}
	data, err := bson.Marshal(in)
	require.NoError(t, err)
	var out UserSettings
	require.NoError(t, bson.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

func TestSettingsUpdateEvent_Shape(t *testing.T) {
	fw := false
	data, err := json.Marshal(SettingsUpdateEvent{Timestamp: 123, Settings: UserSettings{FullWidth: &fw}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"timestamp":123,"settings":{"fullWidth":false}}`, string(data))
}
