package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockTranslator_Translate(t *testing.T) {
	got, err := mockTranslator{}.Translate(context.Background(), "Hello", "zhTW")
	require.NoError(t, err)
	assert.Equal(t, "[zhTW] Hello", got)
}
