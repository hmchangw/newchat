package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestMigrationOplog(t *testing.T) {
	cfg := stream.MigrationOplog("site1")
	assert.Equal(t, "MIGRATION_OPLOG_site1", cfg.Name)
	assert.Equal(t, []string{"chat.migration.oplog.site1.>"}, cfg.Subjects)
}
