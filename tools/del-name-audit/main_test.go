package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := parseConfig(map[string]string{})
	require.NoError(t, err)

	assert.Equal(t, "mongodb://localhost:27017/?directConnection=true", cfg.MongoURI)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Empty(t, cfg.MongoUsername)
	assert.Empty(t, cfg.MongoPassword)
	assert.Equal(t, 10*time.Minute, cfg.Timeout)
}

func TestParseConfig_Overrides(t *testing.T) {
	cfg, err := parseConfig(map[string]string{
		"MONGO_URI":      "mongodb://db:27017",
		"MONGO_DB":       "chat_remote",
		"MONGO_USERNAME": "auditor",
		"MONGO_PASSWORD": "secret",
		"AUDIT_TIMEOUT":  "45s",
	})
	require.NoError(t, err)

	assert.Equal(t, "mongodb://db:27017", cfg.MongoURI)
	assert.Equal(t, "chat_remote", cfg.MongoDB)
	assert.Equal(t, "auditor", cfg.MongoUsername)
	assert.Equal(t, 45*time.Second, cfg.Timeout)
}

func TestParseConfig_InvalidDuration(t *testing.T) {
	_, err := parseConfig(map[string]string{"AUDIT_TIMEOUT": "not-a-duration"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse env")
}

func TestRender_JSONRoundTrips(t *testing.T) {
	rep := report{
		DelRooms: 2, SubsScanned: 5, Agree: 4, Disagree: 1,
		ByRoomType: map[string]*typeStats{"channel": {Agree: 4, Disagree: 1}},
		Samples:    []mismatch{{Kind: "disagree", RoomID: "r1", SubID: "s2"}},
	}
	var buf bytes.Buffer
	require.NoError(t, render(&buf, &rep, true))

	var got report
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Equal(t, 2, got.DelRooms)
	assert.Equal(t, 1, got.Disagree)
	require.Len(t, got.Samples, 1)
	assert.Equal(t, "s2", got.Samples[0].SubID)
}

func TestRenderText_SafeVerdict(t *testing.T) {
	rep := report{
		DelRooms: 1, SubsScanned: 3, Agree: 3,
		ByRoomType: map[string]*typeStats{"channel": {Agree: 3}},
	}
	var buf bytes.Buffer
	require.NoError(t, render(&buf, &rep, false))

	out := buf.String()
	assert.Contains(t, out, "VERDICT: SAFE")
	assert.NotContains(t, out, "NOT SAFE")
	assert.Contains(t, out, "channel")
	assert.NotContains(t, out, "Counter-examples")
}

func TestRenderText_UnsafeVerdictListsCounterExamples(t *testing.T) {
	rep := report{
		DelRooms: 1, SubsScanned: 2, Agree: 1, Disagree: 1,
		FalsePositive: 1,
		ByRoomType:    map[string]*typeStats{"dm": {Disagree: 1}},
		Samples: []mismatch{
			{Kind: "disagree", RoomID: "r1", RoomName: "Del-general", SubID: "s2", SubName: "general", RoomType: "dm"},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, render(&buf, &rep, false))

	out := buf.String()
	assert.Contains(t, out, "VERDICT: NOT SAFE")
	assert.Contains(t, out, "Counter-examples")
	assert.Contains(t, out, "Del-general")
	assert.Contains(t, out, "disagree")
}

// Deterministic output: roomType rows are sorted, so two runs over the same
// report never differ by map iteration order.
func TestRenderText_RoomTypeOrderIsStable(t *testing.T) {
	rep := report{ByRoomType: map[string]*typeStats{
		"channel": {Agree: 1}, "botDM": {Agree: 2}, "dm": {Agree: 3},
	}}
	var first bytes.Buffer
	require.NoError(t, render(&first, &rep, false))
	for i := 0; i < 20; i++ {
		var next bytes.Buffer
		require.NoError(t, render(&next, &rep, false))
		require.Equal(t, first.String(), next.String())
	}
	assert.Less(t, // botDM < channel < dm
		bytes.Index(first.Bytes(), []byte("botDM")),
		bytes.Index(first.Bytes(), []byte("channel")))
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestRender_WriteErrorIsWrapped(t *testing.T) {
	rep := report{}
	err := render(failingWriter{}, &rep, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write report")

	err = render(failingWriter{}, &rep, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode json report")
}

func TestEnvMap(t *testing.T) {
	t.Setenv("DEL_NAME_AUDIT_PROBE", "value=with=equals")
	got := envMap()
	assert.Equal(t, "value=with=equals", got["DEL_NAME_AUDIT_PROBE"])
}
