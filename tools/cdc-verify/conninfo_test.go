package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactURI(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no credentials", "mongodb://mongo1:27017/rocketchat", "mongodb://mongo1:27017/rocketchat"},
		{"credentials stripped", "mongodb://admin:s3cret@mongo1:27017/rocketchat", "mongodb://***@mongo1:27017/rocketchat"},
		{"multi-host with creds", "mongodb://u:p@mongo1:27017,mongo2:27017/db?replicaSet=rs0", "mongodb://***@mongo1:27017,mongo2:27017/db?replicaSet=rs0"},
		{"nats url plain", "nats://nats:4222", "nats://nats:4222"},
		{"nats url with token", "nats://tok3n@nats:4222", "nats://***@nats:4222"},
		{"at-sign after path untouched", "mongodb://h:27017/db@weird", "mongodb://h:27017/db@weird"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, redactURI(tt.in))
		})
	}
}

func TestBuildConnInfo_NeverCarriesSecrets(t *testing.T) {
	cfg := config{
		SiteID:               "site1",
		NATSURL:              "nats://tok3n@nats:4222",
		CredsFile:            "/etc/creds/verify.creds",
		SourceMongoURI:       "mongodb://root:sup3rs3cret@src:27017",
		SourceMongoUsername:  "srcuser",
		SourceMongoPassword:  "srcpass",
		SourceDB:             "rocketchat",
		TargetMongoURI:       "mongodb://tgt:27017",
		TargetMongoPassword:  "tgtpass",
		TargetDB:             "chat",
		CassandraHosts:       "cass1:9042,cass2:9042",
		CassandraKeyspace:    "chat",
		CassandraUsername:    "cassuser",
		CassandraPassword:    "casspass",
		MappingFile:          "/etc/cdc-verify/mapping.json",
		SourceReadPreference: "secondaryPreferred",
	}
	info := buildConnInfo(&cfg, "MIGRATION-OPLOG-site1", true)

	assert.Equal(t, "site1", info.SiteID)
	assert.Equal(t, "MIGRATION-OPLOG-site1", info.Stream)
	assert.Equal(t, "nats://***@nats:4222", info.NATS.URL)
	assert.Equal(t, "mongodb://***@src:27017", info.SourceMongo.URI)
	assert.Equal(t, "rocketchat", info.SourceMongo.DB)
	assert.Equal(t, "secondaryPreferred", info.SourceMongo.ReadPreference)
	assert.Equal(t, "mongodb://tgt:27017", info.TargetMongo.URI)
	assert.Equal(t, "chat", info.TargetMongo.DB)
	assert.True(t, info.Cassandra.InUse)
	assert.Equal(t, "cass1:9042,cass2:9042", info.Cassandra.Hosts)
	assert.Equal(t, "chat", info.Cassandra.Keyspace)

	// No secret — and no credential/auth signal at all — survives
	// serialization: the JSON is what reaches the browser.
	b, err := json.Marshal(info)
	require.NoError(t, err)
	for _, secret := range []string{"sup3rs3cret", "srcpass", "tgtpass", "casspass", "tok3n", "srcuser", "cassuser"} {
		assert.False(t, strings.Contains(string(b), secret), "connInfo JSON must not contain %q", secret)
	}
	for _, field := range []string{"auth", "creds", "username", "password"} {
		assert.False(t, strings.Contains(strings.ToLower(string(b)), field),
			"connInfo JSON must not carry auth-related field %q", field)
	}
}

func TestBuildConnInfo_CassandraUnused(t *testing.T) {
	info := buildConnInfo(&config{SiteID: "s"}, "MIGRATION-OPLOG-s", false)
	assert.False(t, info.Cassandra.InUse)
	assert.Empty(t, info.Cassandra.Hosts)
}
