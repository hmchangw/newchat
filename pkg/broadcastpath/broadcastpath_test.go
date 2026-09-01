package broadcastpath_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/broadcastpath"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestClassify(t *testing.T) {
	for _, tc := range testutil.BroadcastPathCases() {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Want,
				broadcastpath.Classify(tc.ThreadParentMessageID, tc.TShow, tc.RoomType))
		})
	}
}

// TestCasesCoverEveryPath keeps the shared table honest. It is the table both
// message-gatekeeper and broadcast-worker assert against, so a path with no case
// in it is a path neither service is checked on.
func TestCasesCoverEveryPath(t *testing.T) {
	seen := make(map[broadcastpath.Path]int)
	for _, tc := range testutil.BroadcastPathCases() {
		seen[tc.Want]++
	}
	for _, p := range broadcastpath.All {
		assert.NotZero(t, seen[p], "no shared case produces %q", p)
	}
}

// TestClassifyUnknownRoomTypes pins the fail-open shape the gatekeeper depends
// on: an unresolvable room type is a label, never an error.
func TestClassifyUnknownRoomTypes(t *testing.T) {
	for _, rt := range []model.RoomType{"", "thread", "nonsense"} {
		assert.Equal(t, broadcastpath.Unknown, broadcastpath.Classify("", false, rt),
			"room type %q", rt)
	}
}

// TestThreadWinsOverEveryRoomType is the ordering the contract calls out: a
// hidden thread reply in a channel room routes to per-account thread fan-out,
// so the room type must not get a vote.
func TestThreadWinsOverEveryRoomType(t *testing.T) {
	for _, rt := range []model.RoomType{model.RoomTypeChannel, model.RoomTypeDM, model.RoomTypeBotDM, ""} {
		assert.Equal(t, broadcastpath.Thread, broadcastpath.Classify("parent-1", false, rt),
			"room type %q", rt)
	}
}

func TestPathIsValid(t *testing.T) {
	for _, p := range broadcastpath.All {
		assert.True(t, p.Valid(), "%q is in All but not Valid", p)
	}
	assert.False(t, broadcastpath.Path("channel").Valid())
	assert.False(t, broadcastpath.Path("").Valid())
}
