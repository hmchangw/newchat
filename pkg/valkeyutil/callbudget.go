package valkeyutil

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// callBudgetHook caps every command and pipeline at Profile.CallBudget, from
// outside go-redis's retry loops — the only place that can bound their product.
type callBudgetHook struct{ budget time.Duration }

// newProfiledClusterClient is the only way a profiled client is built, so one
// cannot reach production unbudgeted by omission.
func newProfiledClusterClient(opts *redis.ClusterOptions, p Profile) *redis.ClusterClient {
	c := redis.NewClusterClient(opts)
	c.AddHook(callBudgetHook{budget: p.CallBudget})
	return c
}

// withBudget caps ctx at the budget. A ceiling, never an extension: WithTimeout
// keeps the earlier deadline, so a caller near its own guard is not pushed past it.
func (h callBudgetHook) withBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	if h.budget <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, h.budget)
}

// DialHook is the identity; redis.Hook merely requires it. Dialing is still
// budgeted: a command's dial runs on the context ProcessHook already capped.
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
