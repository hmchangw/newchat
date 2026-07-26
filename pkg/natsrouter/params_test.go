package natsrouter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePattern_SingleParam(t *testing.T) {
	r := parsePattern("chat.user.{account}.request")

	assert.Equal(t, "chat.user.*.request", r.natsSubject)
	assert.Equal(t, map[int]string{2: "account"}, r.params)
}

func TestParsePattern_MultipleParams(t *testing.T) {
	r := parsePattern("chat.user.{account}.request.room.{roomID}.{siteID}.msg.history")

	assert.Equal(t, "chat.user.*.request.room.*.*.msg.history", r.natsSubject)
	assert.Equal(t, map[int]string{2: "account", 5: "roomID", 6: "siteID"}, r.params)
}

func TestParsePattern_NoParams(t *testing.T) {
	r := parsePattern("chat.user.request.rooms.list")

	assert.Equal(t, "chat.user.request.rooms.list", r.natsSubject)
	assert.Empty(t, r.params)
}

func TestParsePattern_AdjacentParams(t *testing.T) {
	r := parsePattern("fanout.{siteID}.{roomID}")

	assert.Equal(t, "fanout.*.*", r.natsSubject)
	assert.Equal(t, map[int]string{1: "siteID", 2: "roomID"}, r.params)
}

func TestParsePattern_AllParams(t *testing.T) {
	r := parsePattern("{a}.{b}.{c}")

	assert.Equal(t, "*.*.*", r.natsSubject)
	assert.Equal(t, map[int]string{0: "a", 1: "b", 2: "c"}, r.params)
}

func TestExtractParams(t *testing.T) {
	r := parsePattern("chat.user.{account}.request.room.{roomID}.{siteID}.msg.history")
	params := r.extractParams("chat.user.alice.request.room.room-42.site-1.msg.history")

	assert.Equal(t, "alice", params.Get("account"))
	assert.Equal(t, "room-42", params.Get("roomID"))
	assert.Equal(t, "site-1", params.Get("siteID"))
}

func TestExtractParams_DecodesBotAccount(t *testing.T) {
	r := parsePattern("chat.user.{account}.request.room.{roomID}.{siteID}.msg.history")
	params := r.extractParams("chat.user.weather_bot.request.room.room-42.site-1.msg.history")

	// The {account} token is decoded from its NATS transport form back to the
	// real ".bot" account so handlers get the requester's true identity.
	// roomID/siteID are never decoded.
	assert.Equal(t, "weather.bot", params.Get("account"))
	assert.Equal(t, "room-42", params.Get("roomID"))
	assert.Equal(t, "site-1", params.Get("siteID"))
}

func TestExtractParams_NoParams(t *testing.T) {
	r := parsePattern("chat.static.subject")
	params := r.extractParams("chat.static.subject")

	assert.Equal(t, "", params.Get("anything"))
}

func TestParams_Get_NotFound(t *testing.T) {
	p := Params{values: map[string]string{"account": "abc"}}

	assert.Equal(t, "abc", p.Get("account"))
	assert.Equal(t, "", p.Get("nonexistent"))
}

func TestParams_MustGet_Success(t *testing.T) {
	p := Params{values: map[string]string{"account": "abc"}}

	assert.Equal(t, "abc", p.MustGet("account"))
}

func TestParams_MustGet_Panics(t *testing.T) {
	p := Params{values: map[string]string{}}

	require.Panics(t, func() {
		p.MustGet("nonexistent")
	})
}

func TestParams_Require_Success(t *testing.T) {
	p := Params{values: map[string]string{"account": "abc"}}
	v, err := p.Require("account")
	require.NoError(t, err)
	assert.Equal(t, "abc", v)
}

func TestParams_Require_Missing(t *testing.T) {
	p := Params{values: map[string]string{}}
	_, err := p.Require("account")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required param")
}

func TestParams_Require_Empty(t *testing.T) {
	p := Params{values: map[string]string{"account": ""}}
	_, err := p.Require("account")
	require.Error(t, err)
}
