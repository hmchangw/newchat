package subject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestDROplog(t *testing.T) {
	assert.Equal(t, "chat.dr.oplog.site1.rooms.insert",
		subject.DROplog("site1", "rooms", "insert"))
	assert.Equal(t, "chat.dr.oplog.site1.subscriptions.delete",
		subject.DROplog("site1", "subscriptions", "delete"))
	assert.Equal(t, "chat.dr.oplog.site2.room_members.update",
		subject.DROplog("site2", "room_members", "update"))
}

func TestDROplogWildcard(t *testing.T) {
	assert.Equal(t, "chat.dr.oplog.site1.>", subject.DROplogWildcard("site1"))
}
