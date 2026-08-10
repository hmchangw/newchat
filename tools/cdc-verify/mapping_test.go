package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyFrom_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want KeyFrom
	}{
		{"bare string", `"u._id"`, KeyFrom{From: []string{"u._id"}}},
		{"object single from", `{"from":"ts","transform":"unixMilli"}`, KeyFrom{From: []string{"ts"}, Transform: "unixMilli"}},
		{"object multi from", `{"from":["a","b"],"transform":"dmRoomID"}`, KeyFrom{From: []string{"a", "b"}, Transform: "dmRoomID"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got KeyFrom
			require.NoError(t, json.Unmarshal([]byte(tt.in), &got))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDestRef_UnmarshalJSON(t *testing.T) {
	var short DestRef
	require.NoError(t, json.Unmarshal([]byte(`"msgById.body"`), &short))
	assert.Equal(t, DestRef{Dest: "msgById.body"}, short)

	var full DestRef
	require.NoError(t, json.Unmarshal([]byte(`{"dest":"msgById.created_at","transform":"unixMilli","required":true}`), &full))
	assert.Equal(t, DestRef{Dest: "msgById.created_at", Transform: "unixMilli", Required: true}, full)
}

func TestDestRef_Split(t *testing.T) {
	alias, field := DestRef{Dest: "msgById.meta.count"}.Split()
	assert.Equal(t, "msgById", alias)
	assert.Equal(t, "meta.count", field)
}

func TestSourceMapping_Action(t *testing.T) {
	s := SourceMapping{Ops: map[string]OpAction{"insert": OpVerify, "delete": OpVerifyAbsent}}
	assert.Equal(t, OpVerify, s.Action("insert"))
	assert.Equal(t, OpVerifyAbsent, s.Action("delete"))
	assert.Equal(t, OpSkip, s.Action("update"))
}

func writeMapping(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "m.json")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

const validMappingJSON = `{
  "sources": [{
    "collection": "rocketchat_message",
    "ops": {"insert": "verify", "delete": "verify-absent"},
    "resolvers": {
      "user": {"db": "source", "collection": "users", "key": {"_id": "u._id"}, "fields": ["username"]}
    },
    "targets": {
      "msgById": {"kind": "cassandra", "table": "messages_by_id", "key": {"message_id": "_id"}}
    },
    "fields": {
      "msg": ["msgById.body"],
      "ts": [{"dest": "msgById.created_at", "transform": "unixMilli"}]
    },
    "derived": [{"from": ["u._id"], "transform": "toString", "dest": ["msgById.sender_account"]}]
  }]
}`

func TestLoadMapping_Valid(t *testing.T) {
	m, err := loadMapping(writeMapping(t, validMappingJSON))
	require.NoError(t, err)
	require.Len(t, m.Sources, 1)
	src := m.Sources[0]
	assert.Equal(t, "rocketchat_message", src.Collection)
	assert.Equal(t, []string{"u._id"}, src.Resolvers["user"].Key["_id"].From)
	assert.Equal(t, "cassandra", src.Targets["msgById"].Kind)
	assert.Equal(t, []DestRef{{Dest: "msgById.body"}}, src.Fields["msg"])
	assert.Equal(t, "unixMilli", src.Fields["ts"][0].Transform)
}

func TestLoadMapping_FileMissing(t *testing.T) {
	_, err := loadMapping("/nonexistent/m.json")
	assert.Error(t, err)
}

func TestLoadMapping_BadJSON(t *testing.T) {
	_, err := loadMapping(writeMapping(t, `{`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse mapping")
}

func TestMappingExampleFileIsValid(t *testing.T) {
	_, err := loadMapping("mapping.example.json")
	assert.NoError(t, err)
}
