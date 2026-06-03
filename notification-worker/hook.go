package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// Vetoer is the in-process suppress-only veto (Stage 2). Allow returns false to drop a recipient.
// Must not issue per-recipient I/O; batch-load data before the loop. Errors are fail-open.
type Vetoer interface {
	Allow(ctx context.Context, msg *model.Message, member roomsubcache.Member) (bool, error)
}

type noopVetoer struct{}

func (noopVetoer) Allow(context.Context, *model.Message, roomsubcache.Member) (bool, error) {
	return true, nil
}
