package natsmetrics

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMethodFromPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		siteID  string
		want    RPCMethod
	}{
		{
			name:    "client request keeps the service family and the tail",
			pattern: "chat.user.{account}.request.user.site1.me",
			siteID:  "site1",
			want:    "user_me",
		},
		{
			name:    "room-scoped request drops the roomID param",
			pattern: "chat.user.{account}.request.room.{roomID}.site1.msg.history",
			siteID:  "site1",
			want:    "room_msg_history",
		},
		{
			name:    "siteID as a placeholder derives the same name as the literal",
			pattern: "chat.user.{account}.request.room.{roomID}.{siteID}.msg.history",
			siteID:  "site1",
			want:    "room_msg_history",
		},
		{
			name:    "hyphenated token becomes an underscore",
			pattern: "chat.user.{account}.request.room.{roomID}.site1.message.read-receipt",
			siteID:  "site1",
			want:    "room_message_read_receipt",
		},
		{
			name:    "camelCase token splits, trailing initialism stays whole",
			pattern: "chat.user.{account}.request.user.site1.subscription.getByRoomID",
			siteID:  "site1",
			want:    "user_subscription_get_by_room_id",
		},
		{
			name:    "two-letter trailing initialism splits once",
			pattern: "chat.user.{account}.request.user.site1.subscription.getDM",
			siteID:  "site1",
			want:    "user_subscription_get_dm",
		},
		{
			name:    "camelCase mid-path token splits",
			pattern: "chat.user.{account}.request.user.site1.settings.priorityContacts.add",
			siteID:  "site1",
			want:    "user_settings_priority_contacts_add",
		},
		{
			name:    "server lane keeps its transport token",
			pattern: "chat.server.request.room.site1.info.batch",
			siteID:  "site1",
			want:    "server_room_info_batch",
		},
		{
			name:    "bot lane is retained so it cannot collide with the user lane",
			pattern: "chat.server.bot.request.room.site1.create",
			siteID:  "site1",
			want:    "server_bot_room_create",
		},
		{
			name:    "user lane create stays distinct from the bot lane create",
			pattern: "chat.user.{account}.request.room.site1.create",
			siteID:  "site1",
			want:    "room_create",
		},
		{
			name:    "the two presence query lanes stay distinct",
			pattern: "chat.server.request.presence.site1.query.batch",
			siteID:  "site1",
			want:    "server_presence_query_batch",
		},
		{
			name:    "the client presence query lane drops its user token",
			pattern: "chat.user.presence.site1.query.batch",
			siteID:  "site1",
			want:    "presence_query_batch",
		},
		{
			name:    "migration lane is retained, internal is structural",
			pattern: "chat.migration.internal.site1.msg.edit",
			siteID:  "site1",
			want:    "migration_msg_edit",
		},
		{
			name:    "event lane is structural, the family survives",
			pattern: "chat.user.{account}.event.presence.site1.hello",
			siteID:  "site1",
			want:    "presence_hello",
		},
		{
			name:    "a trailing user token is kept; only index 1 is the transport lane",
			pattern: "chat.user.{account}.request.teams.site1.call.user",
			siteID:  "site1",
			want:    "teams_call_user",
		},
		{
			name:    "only the first siteID-valued token is dropped",
			pattern: "chat.user.{account}.request.room.room.msg.get",
			siteID:  "room",
			want:    "room_msg_get",
		},
		{
			name:    "a pattern of nothing but structural tokens falls back",
			pattern: "chat.request",
			siteID:  "site1",
			want:    MethodOther,
		},
		{
			name:    "an empty pattern falls back",
			pattern: "",
			siteID:  "site1",
			want:    MethodOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, MethodFromPattern(tt.pattern, tt.siteID))
		})
	}
}

// labelShape is what a Prometheus label value may look like here: the derivation
// must never leak a placeholder brace, a hyphen or an upper-case token.
var labelShape = regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*$`)

// TestMethodFromPattern_Fleet is the review surface. It derives a method for
// every route the fleet registers and pins the two properties that make
// rpc_method usable as a primary key: every route gets a real name, and no two
// routes share one. testdata/fleet_routes.tsv is the input, not the expectation.
func TestMethodFromPattern_Fleet(t *testing.T) {
	routes := loadFleetRoutes(t)
	require.Len(t, routes, 96, "fleet inventory changed; regenerate testdata/fleet_routes.tsv")

	seen := make(map[RPCMethod]string, len(routes))
	for _, rt := range routes {
		method := MethodFromPattern(rt.pattern, "site1")

		assert.NotEqual(t, MethodOther, method, "%s: %s derived no name", rt.service, rt.pattern)
		assert.Regexp(t, labelShape, string(method), "%s: %s", rt.service, rt.pattern)

		if prior, dup := seen[method]; dup {
			assert.Failf(t, "collision", "%q derived from both %s and %s", method, prior, rt.pattern)
			continue
		}
		seen[method] = rt.pattern
	}
	assert.Len(t, seen, len(routes), "every route must own a distinct method")
}

type fleetRoute struct {
	service string
	pattern string
}

func loadFleetRoutes(t *testing.T) []fleetRoute {
	t.Helper()

	f, err := os.Open("testdata/fleet_routes.tsv")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	var routes []fleetRoute
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		require.Len(t, fields, 3, "malformed fixture line: %s", line)
		routes = append(routes, fleetRoute{service: fields[0], pattern: fields[2]})
	}
	require.NoError(t, scanner.Err())

	return routes
}

// updateGolden regenerates testdata/fleet_methods.golden instead of asserting.
var updateGolden = flag.Bool("update", false, "rewrite the fleet method golden file")

// TestMethodFromPattern_Golden records the label every route derives. It is the
// human review surface for the derivation rule: a subject change that renames a
// metric series shows up here as a diff rather than as a silently broken panel.
func TestMethodFromPattern_Golden(t *testing.T) {
	routes := loadFleetRoutes(t)

	var b strings.Builder
	b.WriteString("# Generated by TestMethodFromPattern_Golden -- go test ./pkg/natsmetrics/ -update\n")
	b.WriteString("# rpc.method\tservice\tsubject pattern\n")

	lines := make([]string, 0, len(routes))
	for _, rt := range routes {
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s", MethodFromPattern(rt.pattern, "site1"), rt.service, rt.pattern))
	}
	sort.Strings(lines)
	b.WriteString(strings.Join(lines, "\n") + "\n")

	const golden = "testdata/fleet_methods.golden"
	if *updateGolden {
		require.NoError(t, os.WriteFile(golden, []byte(b.String()), 0o600))
		return
	}

	want, err := os.ReadFile(golden)
	require.NoError(t, err, "run: go test ./pkg/natsmetrics/ -update")
	assert.Equal(t, string(want), b.String(), "derived methods drifted; re-review then run -update")
}

func TestNormalizeRPCMethod(t *testing.T) {
	tests := []struct {
		name   string
		method RPCMethod
		want   RPCMethod
	}{
		{name: "a derived method passes through", method: "room_msg_history", want: "room_msg_history"},
		{name: "the zero value is bounded", method: "", want: MethodOther},
		{name: "MethodOther stays itself", method: MethodOther, want: MethodOther},
		{name: "a raw subject is bounded", method: "chat.user.alice.request.room.r1", want: MethodOther},
		{name: "an upper-case value is bounded", method: "RoomMsgHistory", want: MethodOther},
		{name: "a trailing underscore is bounded", method: "room_", want: MethodOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeRPCMethod(tt.method))
		})
	}
}
