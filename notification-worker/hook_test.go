package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

func TestNoopHook_AlwaysAllows(t *testing.T) {
	h := noopVetoer{}
	allow, err := h.Allow(context.Background(), &model.Message{}, roomsubcache.Member{Account: "a"})
	assert.NoError(t, err)
	assert.True(t, allow)
}
