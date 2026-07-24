package cassrepo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// The walker's preview skip-set must be exactly the canonical model system set, so a new
// system type can't leak into previews. MessageTypeRemoved is excluded earlier in scanBucket
// (from both pointer and preview), so it must NOT be in this set.
func TestLastMessageSkipTypes_TracksModelSystemSet(t *testing.T) {
	systemSet := model.SystemMessageTypeSet()
	assert.Len(t, lastMessageSkipTypes, len(systemSet),
		"skip set = exactly model.SystemMessageTypeSet, nothing else")
	for typ := range systemSet {
		_, ok := lastMessageSkipTypes[typ]
		assert.True(t, ok, "system type %q missing from walker skip set", typ)
	}
	_, ok := lastMessageSkipTypes[MessageTypeRemoved]
	assert.False(t, ok, "removed-parent placeholder is handled by scanBucket, not the skip-set")
}
