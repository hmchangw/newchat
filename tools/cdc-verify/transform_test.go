package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func testRegistry() transformRegistry {
	return newTransformRegistry(msgbucket.New(72 * time.Hour))
}

func TestTransform_UnixMilli(t *testing.T) {
	r := testRegistry()
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	got, err := r.apply("unixMilli", []any{ts})
	require.NoError(t, err)
	assert.Equal(t, ts.UnixMilli(), got)

	got, err = r.apply("unixMilli", []any{float64(1700000000000)})
	require.NoError(t, err)
	assert.Equal(t, int64(1700000000000), got)

	_, err = r.apply("unixMilli", []any{"not-a-time"})
	assert.Error(t, err)
}

func TestTransform_ToString(t *testing.T) {
	r := testRegistry()
	got, err := r.apply("toString", []any{42})
	require.NoError(t, err)
	assert.Equal(t, "42", got)

	_, err = r.apply("toString", []any{map[string]any{}})
	assert.Error(t, err)
}

func TestTransform_MsgBucket(t *testing.T) {
	r := testRegistry()
	sizer := msgbucket.New(72 * time.Hour)
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	got, err := r.apply("msgBucket", []any{ts})
	require.NoError(t, err)
	assert.Equal(t, sizer.Of(ts), got)
}

func TestTransform_IdentityAndUnknown(t *testing.T) {
	r := testRegistry()
	got, err := r.apply("", []any{"passthrough"})
	require.NoError(t, err)
	assert.Equal(t, "passthrough", got)

	_, err = r.apply("nope", []any{1})
	assert.Error(t, err)

	_, err = r.apply("unixMilli", nil)
	assert.Error(t, err)
}

func TestKnownTransform(t *testing.T) {
	assert.True(t, knownTransform("unixMilli"))
	assert.True(t, knownTransform("toString"))
	assert.True(t, knownTransform("msgBucket"))
	assert.False(t, knownTransform("nope"))
}
