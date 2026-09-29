package reqid_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/reqid"
)

func TestFrom_RoundTripsWhatWithStored(t *testing.T) {
	ctx := reqid.With(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")

	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", reqid.From(ctx))
}

func TestFrom_EmptyWhenUnset(t *testing.T) {
	assert.Empty(t, reqid.From(context.Background()))
}

// An empty id must not shadow one already on the context: the mint-on-missing
// helpers above this package call With unconditionally.
func TestWith_EmptyIDLeavesAnExistingOneAlone(t *testing.T) {
	ctx := reqid.With(context.Background(), "abc")

	assert.Equal(t, "abc", reqid.From(reqid.With(ctx, "")))
}

// The key is package-private, so no other package's context value can be
// mistaken for a request id.
func TestFrom_IgnoresAForeignKeyOfTheSameShape(t *testing.T) {
	type otherKey int
	ctx := context.WithValue(context.Background(), otherKey(0), "not-a-request-id")

	assert.Empty(t, reqid.From(ctx))
}
