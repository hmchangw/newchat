package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSettingsUpdateEvent_Shape(t *testing.T) {
	fw := false
	data, err := json.Marshal(SettingsUpdateEvent{Timestamp: 123, Settings: UserSettings{FullWidth: &fw}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"timestamp":123,"settings":{"fullWidth":false}}`, string(data))
}
