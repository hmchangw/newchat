package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestDROplog(t *testing.T) {
	cfg := stream.DROplog("site1")
	assert.Equal(t, "DR_OPLOG_site1", cfg.Name)
	assert.Equal(t, []string{"chat.dr.oplog.site1.>"}, cfg.Subjects)
}
