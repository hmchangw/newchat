package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseSource() SourceMapping {
	return SourceMapping{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			"msgById": {Kind: "cassandra", Table: "messages_by_id", Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
		},
		Fields: map[string][]DestRef{"msg": {{Dest: "msgById.body"}}},
	}
}

func TestValidateMapping(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*SourceMapping)
		wantErr string
	}{
		{"valid", func(s *SourceMapping) {}, ""},
		{"no targets", func(s *SourceMapping) { s.Targets = nil }, "no targets"},
		{"unknown op", func(s *SourceMapping) { s.Ops["upsert"] = OpVerify }, "unknown op"},
		{"unknown action", func(s *SourceMapping) { s.Ops["insert"] = "observe" }, "unknown action"},
		{"bad kind", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "redis", Key: map[string]KeyFrom{"k": {From: []string{"_id"}}}}
		}, "kind"},
		{"cassandra without table", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "cassandra", Key: map[string]KeyFrom{"k": {From: []string{"_id"}}}}
		}, "table"},
		{"mongo without collection", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "mongo", Key: map[string]KeyFrom{"k": {From: []string{"_id"}}}}
		}, "collection"},
		{"empty target key", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "mongo", Collection: "c", Key: nil}
		}, "empty key"},
		{"destref unknown alias", func(s *SourceMapping) {
			s.Fields["msg"] = append(s.Fields["msg"], DestRef{Dest: "ghost.body"})
		}, "unknown target"},
		{"destref into verbatim", func(s *SourceMapping) {
			tgt := s.Targets["msgById"]
			tgt.Mode = "verbatim"
			s.Targets["msgById"] = tgt
		}, "verbatim"},
		{"resolver ref missing", func(s *SourceMapping) {
			s.Targets["msgById"] = Target{Kind: "cassandra", Table: "t",
				Key: map[string]KeyFrom{"user_id": {From: []string{"@user.username"}}}}
		}, "resolver"},
		{"resolver chaining", func(s *SourceMapping) {
			s.Resolvers = map[string]Resolver{
				"a": {DB: "source", Collection: "users", Fields: []string{"x"},
					Key: map[string]KeyFrom{"_id": {From: []string{"@b.x"}}}},
				"b": {DB: "source", Collection: "users", Fields: []string{"x"},
					Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}},
			}
		}, "chain"},
		{"resolver bad db", func(s *SourceMapping) {
			s.Resolvers = map[string]Resolver{"u": {DB: "cassandra", Collection: "users",
				Fields: []string{"x"}, Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}}}
		}, "db"},
		{"derived missing transform", func(s *SourceMapping) {
			s.Derived = []Derived{{From: []string{"a"}, Dest: []string{"msgById.x"}}}
		}, "transform"},
		{"unknown transform", func(s *SourceMapping) {
			s.Fields["msg"] = []DestRef{{Dest: "msgById.body", Transform: "nope"}}
		}, "unknown transform"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := baseSource()
			tt.mutate(&src)
			err := validateMapping(&Mapping{Sources: []SourceMapping{src}})
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.wantErr), "error %q should contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateMapping_DuplicateCollection(t *testing.T) {
	err := validateMapping(&Mapping{Sources: []SourceMapping{baseSource(), baseSource()}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestMapping_SourceAndNeedsCassandra(t *testing.T) {
	m := &Mapping{Sources: []SourceMapping{baseSource()}}
	src, ok := m.Source("rocketchat_message")
	require.True(t, ok)
	assert.Equal(t, "rocketchat_message", src.Collection)
	_, ok = m.Source("nope")
	assert.False(t, ok)
	assert.True(t, m.NeedsCassandra())

	mongoOnly := baseSource()
	mongoOnly.Targets = map[string]Target{"t": {Kind: "mongo", Collection: "c",
		Key: map[string]KeyFrom{"_id": {From: []string{"_id"}}}}}
	mongoOnly.Fields = map[string][]DestRef{"msg": {{Dest: "t.body"}}}
	assert.False(t, (&Mapping{Sources: []SourceMapping{mongoOnly}}).NeedsCassandra())
}
