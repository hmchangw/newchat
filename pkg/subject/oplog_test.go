package subject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestMigrationOplog(t *testing.T) {
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_message.insert",
		subject.MigrationOplog("site1", "rocketchat_message", "insert"))
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_room.delete",
		subject.MigrationOplog("site1", "rocketchat_room", "delete"))
}

func TestMigrationOplogWildcard(t *testing.T) {
	assert.Equal(t, "chat.migration.oplog.site1.>", subject.MigrationOplogWildcard("site1"))
}

func TestMigrationInternalSubjects(t *testing.T) {
	assert.Equal(t, "chat.migration.internal.site1.msg.edit", subject.MigrationInternalMsgEdit("site1"))
	assert.Equal(t, "chat.migration.internal.site1.msg.delete", subject.MigrationInternalMsgDelete("site1"))
}
