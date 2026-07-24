package searchindex_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestEsPropertiesFromStruct_MessageDoc_MatchesStruct(t *testing.T) {
	props := searchindex.EsPropertiesFromStruct[searchindex.MessageDoc]()

	// Every MessageDoc field with an es tag must have a corresponding template property.
	typ := reflect.TypeOf(searchindex.MessageDoc{})
	for i := range typ.NumField() {
		field := typ.Field(i)
		esTag := field.Tag.Get("es")
		if esTag == "" || esTag == "-" {
			continue
		}
		jsonTag := field.Tag.Get("json")
		name, _, _ := strings.Cut(jsonTag, ",")

		prop, ok := props[name]
		assert.True(t, ok, "template missing property for struct field %s (json: %s)", field.Name, name)

		esType, _, _ := strings.Cut(esTag, ",")
		propMap := prop.(map[string]any)
		// object_disabled expands to a stored-only object mapping.
		if esType == "object_disabled" {
			assert.Equal(t, "object", propMap["type"], "type mismatch for field %s", name)
			assert.Equal(t, false, propMap["enabled"], "field %s must not be indexed", name)
			continue
		}
		assert.Equal(t, esType, propMap["type"], "type mismatch for field %s", name)
	}

	// Template should have exactly as many properties as struct fields with es tags.
	esFieldCount := 0
	for i := range typ.NumField() {
		if tag := typ.Field(i).Tag.Get("es"); tag != "" && tag != "-" {
			esFieldCount++
		}
	}
	assert.Equal(t, esFieldCount, len(props), "template property count should match struct es-tagged field count")
}

func TestEsPropertiesFromStruct_SpotlightDoc_MatchesStruct(t *testing.T) {
	props := searchindex.EsPropertiesFromStruct[searchindex.SpotlightDoc]()

	typ := reflect.TypeOf(searchindex.SpotlightDoc{})
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

func TestMessageTemplateName(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{"strips version suffix", "messages-site1-v1", "messages-site1_template"},
		{"bare base fallback", "messages-site1", "messages-site1_template"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, searchindex.MessageTemplateName(tt.prefix))
		})
	}
}

func TestMessageTemplateBody(t *testing.T) {
	body := searchindex.MessageTemplateBody("messages-site1-v1", false)
	require.NotNil(t, body)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))

	patterns, ok := parsed["index_patterns"].([]any)
	require.True(t, ok)
	require.Len(t, patterns, 1)
	assert.Equal(t, "messages-site1-*", patterns[0])

	tmpl := parsed["template"].(map[string]any)
	mappings := tmpl["mappings"].(map[string]any)
	props := mappings["properties"].(map[string]any)
	assert.Contains(t, props, "messageId")
	assert.Contains(t, props, "roomId")
	assert.Contains(t, props, "siteId")
	assert.Contains(t, props, "userId")
	assert.Contains(t, props, "userAccount")
	assert.Contains(t, props, "content")
	assert.Contains(t, props, "createdAt")
	assert.Contains(t, props, "tshow")
	assert.Equal(t, "boolean", props["tshow"].(map[string]any)["type"])
	assert.Equal(t, false, mappings["dynamic"])

	settings := tmpl["settings"].(map[string]any)
	analysis := settings["analysis"].(map[string]any)
	analyzers := analysis["analyzer"].(map[string]any)
	assert.Contains(t, analyzers, "custom_analyzer")

	idx := settings["index"].(map[string]any)
	assert.Equal(t, float64(4), idx["number_of_shards"])
	assert.Equal(t, float64(2), idx["number_of_replicas"])

	// Render payloads are stored but never indexed: object + enabled:false.
	for _, key := range []string{"attachments", "card"} {
		prop, ok := props[key].(map[string]any)
		require.True(t, ok, "%s must be mapped", key)
		assert.Equal(t, "object", prop["type"], key)
		assert.Equal(t, false, prop["enabled"], "%s must not be indexed", key)
	}
}

func TestMessageTemplateBody_DevMode(t *testing.T) {
	body := searchindex.MessageTemplateBody("messages-site1-v1", true)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(1), idx["number_of_shards"])
	assert.Equal(t, float64(0), idx["number_of_replicas"])
}

func TestSpotlightTemplateName(t *testing.T) {
	assert.Equal(t, "spotlight-site-a_template", searchindex.SpotlightTemplateName("spotlight-site-a-v1"))
}

func TestSpotlightTemplateBody(t *testing.T) {
	body := searchindex.SpotlightTemplateBody("spotlight-site-a-v1", false)
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

	idx := tmpl["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(3), idx["number_of_shards"])
	assert.Equal(t, float64(1), idx["number_of_replicas"])
}

func TestSpotlightTemplateBody_DevMode(t *testing.T) {
	body := searchindex.SpotlightTemplateBody("spotlight-site-a-v1", true)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	idx := decoded["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(1), idx["number_of_shards"])
	assert.Equal(t, float64(0), idx["number_of_replicas"])
}

func TestUserRoomTemplateName(t *testing.T) {
	// Unlike messages/spotlight, user-room index names are unversioned — no
	// version-stripping applies.
	assert.Equal(t, "user-room-mv-site-a_template", searchindex.UserRoomTemplateName("user-room-mv-site-a"))
}

func TestUserRoomTemplateBody(t *testing.T) {
	body := searchindex.UserRoomTemplateBody("user-room-site-a")
	require.NotNil(t, body)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))

	patterns, ok := parsed["index_patterns"].([]any)
	require.True(t, ok)
	require.Len(t, patterns, 1)
	assert.Equal(t, "user-room-site-a", patterns[0])

	tmpl := parsed["template"].(map[string]any)
	mappings := tmpl["mappings"].(map[string]any)
	props := mappings["properties"].(map[string]any)

	assert.Contains(t, props, "userAccount")
	assert.Contains(t, props, "rooms")
	assert.Contains(t, props, "restrictedRooms")
	assert.Contains(t, props, "roomTimestamps")
	assert.Contains(t, props, "createdAt")
	assert.Contains(t, props, "updatedAt")
	assert.Equal(t, false, mappings["dynamic"])

	rt := props["roomTimestamps"].(map[string]any)
	assert.Equal(t, "flattened", rt["type"])

	rr := props["restrictedRooms"].(map[string]any)
	assert.Equal(t, "flattened", rr["type"])

	rooms := props["rooms"].(map[string]any)
	assert.Equal(t, "text", rooms["type"])

	idx := tmpl["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(1), idx["number_of_shards"])
	assert.Equal(t, float64(1), idx["number_of_replicas"])
}
