package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The embedded UserSettings must inline its fields at the top level of the
// request body — clients send {"fullWidth":true}, not {"userSettings":{...}}.
func TestSettingsSetRequest_FieldsInlineAtTopLevel(t *testing.T) {
	var req SettingsSetRequest
	require.NoError(t, json.Unmarshal([]byte(`{"fullWidth":true,"translateMessageInto":"en-US"}`), &req))
	require.NotNil(t, req.FullWidth)
	assert.True(t, *req.FullWidth)
	require.NotNil(t, req.TranslateMessageInto)
	assert.Equal(t, "en-US", *req.TranslateMessageInto)
	assert.Nil(t, req.ScrollToBottomInChat)
}
