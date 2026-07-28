package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunAllCollections_AggregatesErrorsAcrossCollections(t *testing.T) {
	callOrder := []string{}
	err := runAllCollections(context.Background(),
		func(context.Context) error { callOrder = append(callOrder, "messages"); return nil },
		func(context.Context) error {
			callOrder = append(callOrder, "spotlight")
			return errors.New("spotlight failed")
		},
		func(context.Context) error { callOrder = append(callOrder, "user-room"); return nil },
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "spotlight failed")
	assert.ElementsMatch(t, []string{"messages", "spotlight", "user-room"}, callOrder, "every collection must run even if an earlier one failed")
}

func TestRunAllCollections_NilOnAllSuccess(t *testing.T) {
	noop := func(context.Context) error { return nil }
	err := runAllCollections(context.Background(), noop, noop, noop)
	require.NoError(t, err)
}
