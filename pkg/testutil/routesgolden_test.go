package testutil

import (
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// The fallback must be refused before the golden file is touched, so a
// degraded route can never be written into one or hand-added to one.
func TestRejectFallbackMethod(t *testing.T) {
	tests := []struct {
		name      string
		routes    []natsmetrics.RPCRoute
		wantErr   bool
		wantNamed []string
	}{
		{
			name:   "only declared methods",
			routes: []natsmetrics.RPCRoute{{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.open"}},
		},
		{
			name:   "empty table",
			routes: nil,
		},
		{
			name:    "degraded route alone",
			routes:  []natsmetrics.RPCRoute{{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo"}},
			wantErr: true,
		},
		{
			name: "degraded route beside a good one",
			routes: []natsmetrics.RPCRoute{
				{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.open"},
				{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo"},
			},
			wantErr: true,
		},
		{
			name: "two degraded routes are both named",
			routes: []natsmetrics.RPCRoute{
				{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo"},
				{Method: natsmetrics.MethodOther, Pattern: "chat.user.{account}.request.room.typo2"},
			},
			wantErr:   true,
			wantNamed: []string{"chat.user.{account}.request.room.typo", "chat.user.{account}.request.room.typo2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectFallbackMethod(tt.routes)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), string(natsmetrics.MethodOther))
			named := tt.wantNamed
			if named == nil {
				named = []string{"chat.user.{account}.request.room.typo"}
			}
			for _, pattern := range named {
				assert.Contains(t, err.Error(), pattern,
					"the message must name every offending pattern so each route is findable")
			}
		})
	}
}

// Two patterns that differ only in placeholder spelling subscribe to one
// subject, so both handlers are live and NATS splits the traffic. Comparing
// Pattern passed this pair; comparing NATSSubject does not.
func TestRejectDuplicateSubject(t *testing.T) {
	const subject = "chat.user.*.request.settings.get"

	err := rejectDuplicateSubject([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodGetSettings, Pattern: "chat.user.{account}.request.settings.get", NATSSubject: subject},
		{Method: natsmetrics.MethodGetChatlist, Pattern: "chat.user.{user}.request.settings.get", NATSSubject: subject},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), subject)
	assert.Contains(t, err.Error(), "{account}")
	assert.Contains(t, err.Error(), "{user}",
		"both patterns must be named, or the reader cannot tell which registration to remove")

	require.NoError(t, rejectDuplicateSubject([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodGetSettings, Pattern: "chat.user.{account}.request.settings.get", NATSSubject: subject},
		{Method: natsmetrics.MethodGetChatlist, Pattern: "chat.user.{account}.request.chatlist.get", NATSSubject: "chat.user.*.request.chatlist.get"},
	}), "distinct subjects must pass")
}

// service_name + rpc_method must identify one handler. Two routes sharing a
// method merge into a series no dashboard can split, so the golden file
// showing it is not enough — a regeneration would absorb it.
func TestRejectDuplicateMethod(t *testing.T) {
	err := rejectDuplicateMethod([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.{roomID}.site-a.open", NATSSubject: "a"},
		{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.{roomID}.site-a.archive", NATSSubject: "b"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), string(natsmetrics.MethodOpenRoom))
	assert.Contains(t, err.Error(), "archive", "both routes must be named")

	require.NoError(t, rejectDuplicateMethod([]natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: "a", NATSSubject: "a"},
		{Method: natsmetrics.MethodGetRoomAppTabs, Pattern: "b", NATSSubject: "b"},
	}))
}

// AssertRoutesGolden's own branches were reachable only through the ten service
// tests, so its regeneration path — write the file, then fail once — had never
// run under test. The ordering it depends on is load-bearing: the fallback and
// duplicate checks must run *before* the file is touched, or a degraded table
// gets written into a fresh golden and becomes the thing the gate asserts.
//
// The subtests run in a temp working directory so the real testdata/ is never
// written, and drive the helper through a *testing.T of their own.
func TestAssertRoutesGoldenFileHandling(t *testing.T) {
	good := []natsmetrics.RPCRoute{{
		Method:      natsmetrics.MethodOpenRoom,
		Pattern:     "chat.user.{account}.request.room.{roomID}.site-a.open",
		NATSSubject: "chat.user.*.request.room.*.site-a.open",
	}}

	t.Run("missing golden is written and the run fails once", func(t *testing.T) {
		chdirToTemp(t)

		got := runGolden(t, good)
		assert.True(t, got.failed, "a generated golden must fail the run that generated it")

		// #nosec G304 -- routesGoldenPath is a package-level const, not a variable a caller can influence
		// nosemgrep: gosec.G304-1
		written, err := os.ReadFile(routesGoldenPath)
		require.NoError(t, err, "the golden must exist after the failing run")
		assert.Equal(t, "open_room chat.user.{account}.request.room.{roomID}.site-a.open\n", string(written))

		assert.False(t, runGolden(t, good).failed, "the second run compares against what was written")
	})

	t.Run("mismatch fails without rewriting the golden", func(t *testing.T) {
		chdirToTemp(t)
		require.True(t, runGolden(t, good).failed)
		// #nosec G304 -- routesGoldenPath is a package-level const, not a variable a caller can influence
		// nosemgrep: gosec.G304-1
		before, err := os.ReadFile(routesGoldenPath)
		require.NoError(t, err)

		changed := []natsmetrics.RPCRoute{{
			Method:      natsmetrics.MethodGetRoomAppTabs,
			Pattern:     good[0].Pattern,
			NATSSubject: good[0].NATSSubject,
		}}
		assert.True(t, runGolden(t, changed).failed, "a changed table must not be absorbed")

		// #nosec G304 -- routesGoldenPath is a package-level const, not a variable a caller can influence
		// nosemgrep: gosec.G304-1
		after, err := os.ReadFile(routesGoldenPath)
		require.NoError(t, err)
		assert.Equal(t, string(before), string(after), "a mismatch must never overwrite the golden")
	})

	t.Run("a degraded route is refused before the golden is written", func(t *testing.T) {
		chdirToTemp(t)

		degraded := []natsmetrics.RPCRoute{{
			Method:      natsmetrics.MethodOther,
			Pattern:     "chat.user.{account}.request.room.typo",
			NATSSubject: "chat.user.*.request.room.typo",
		}}
		assert.True(t, runGolden(t, degraded).failed)

		_, err := os.Stat(routesGoldenPath)
		assert.True(t, errors.Is(err, fs.ErrNotExist),
			"the fallback must be refused before any file is created, or _OTHER becomes an accepted spelling")
	})
}

// chdirToTemp points the helper's relative testdata/ path at a scratch
// directory for the duration of one subtest.
func chdirToTemp(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
}

// runGolden drives AssertRoutesGolden through its own *testing.T so a failure
// is a value this test can assert on rather than a failure of this test.
func runGolden(t *testing.T, routes []natsmetrics.RPCRoute) (result struct{ failed bool }) {
	t.Helper()
	inner := &testing.T{}
	done := make(chan bool, 1)
	go func() {
		it := inner
		defer func() {
			_ = recover() // require.* calls runtime.Goexit; the t.Failed() read below is what matters
			done <- it.Failed()
		}()
		AssertRoutesGolden(it, routes)
		done <- it.Failed()
	}()
	result.failed = <-done
	return result
}
