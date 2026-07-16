package cassrepo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// The walker's skip-set must be derived from the canonical model set (plus the
// repo-local removed-parent placeholder) so a newly added system type can never
// silently leak into room previews.
func TestLastMessageSkipTypes_TracksModelSystemSet(t *testing.T) {
	assert.Len(t, lastMessageSkipTypes, len(model.SystemMessageTypes)+1,
		"skip set = model.SystemMessageTypes + MessageTypeRemoved, nothing else")
	for typ := range model.SystemMessageTypes {
		_, ok := lastMessageSkipTypes[typ]
		assert.True(t, ok, "system type %q missing from walker skip set", typ)
	}
	_, ok := lastMessageSkipTypes[MessageTypeRemoved]
	assert.True(t, ok, "removed-parent placeholder must stay excluded from previews")
}

