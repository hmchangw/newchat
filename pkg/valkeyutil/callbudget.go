package valkeyutil

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// callBudgetHook caps every command and pipeline at Profile.CallBudget.
//
// A hook rather than a wrapper on each Client method: go-redis reaches the wire
// through two entry points (single command and pipeline), and both are covered
// here once. Wrapping the twelve Client methods instead would leave whichever
// one was added next unbounded, and IncrEx — the bot rate limiter — is a
// pipeline, so the split is not hypothetical.
//
// The budget is applied outside go-redis's retry loops, which is the whole
// point: ReadTimeout bounds one socket read and the loops multiply it, so only a
// deadline spanning the retries can bound the call.
type callBudgetHook struct{ budget time.Duration }

// newProfiledClusterClient is the only way a profiled client is built: it
// applies the profile's call budget, so a client constructed from
// ClusterOptionsFor without it cannot reach production by omission.
func newProfiledClusterClient(opts *redis.ClusterOptions, p Profile) *redis.ClusterClient {
	c := redis.NewClusterClient(opts)
	c.AddHook(callBudgetHook{budget: p.CallBudget})
	return c
}

// withBudget returns ctx capped at the budget. It is a ceiling, never an
// extension — context.WithTimeout keeps the earlier deadline, so a caller
// already close to its own guard is not pushed past it by a cache read.
func (h callBudgetHook) withBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	if h.budget <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, h.budget)
}

// DialHook takes no part in the budget; redis.Hook just requires all three
// methods, so this is the identity.
//
// Returning next rather than a closure around it is not only tidier: the chain
// is rebuilt per AddHook and then called per dial, so a pass-through closure
// would add a frame to every dial for nothing.
//
// Note this does NOT leave dialing on DialTimeout alone. A dial raised by a
// command runs withConn -> getConn -> dialConn on the command's context, which
// ProcessHook has already capped, so connection setup on the hot path shares
// the call budget. Only background dials — pool warm-up and ConnPool.tryDial —
// use a context of their own, and those were never in scope here.
func (h callBudgetHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h callBudgetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		ctx, cancel := h.withBudget(ctx)
		defer cancel()
		return next(ctx, cmd)
	}
}

func (h callBudgetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		ctx, cancel := h.withBudget(ctx)
		defer cancel()
		return next(ctx, cmds)
	}
}
