package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

const (
	inCallIndexKey = "presence:status:index:azure"
	idMapKey       = "presence:idmap:azure"
)

// --- in-call index ---

type valkeyInCallIndex struct{ c *redis.ClusterClient }

func newValkeyInCallIndex(c *redis.ClusterClient) *valkeyInCallIndex { return &valkeyInCallIndex{c: c} }

func (v *valkeyInCallIndex) Members(ctx context.Context) ([]string, error) {
	m, err := v.c.SMembers(ctx, inCallIndexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("smembers in-call index: %w", err)
	}
	return m, nil
}

func (v *valkeyInCallIndex) Add(ctx context.Context, account string) error {
	if err := v.c.SAdd(ctx, inCallIndexKey, account).Err(); err != nil {
		return fmt.Errorf("sadd in-call index %q: %w", account, err)
	}
	return nil
}

func (v *valkeyInCallIndex) Remove(ctx context.Context, account string) error {
	if err := v.c.SRem(ctx, inCallIndexKey, account).Err(); err != nil {
		return fmt.Errorf("srem in-call index %q: %w", account, err)
	}
	return nil
}

// --- id map ---

type valkeyIDMap struct{ c *redis.ClusterClient }

func newValkeyIDMap(c *redis.ClusterClient) *valkeyIDMap { return &valkeyIDMap{c: c} }

// Store permanently records account -> azureObjectID entries (no expiry — the
// mapping is immutable). Only missing accounts are ever passed in.
func (v *valkeyIDMap) Store(ctx context.Context, mapping map[string]string) error {
	if len(mapping) == 0 {
		return nil
	}
	vals := make([]any, 0, len(mapping)*2)
	for account, id := range mapping {
		vals = append(vals, account, id)
	}
	if err := v.c.HSet(ctx, idMapKey, vals...).Err(); err != nil {
		return fmt.Errorf("hset id map: %w", err)
	}
	return nil
}

// Resolve returns account -> id for the accounts present in the hash.
func (v *valkeyIDMap) Resolve(ctx context.Context, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	vals, err := v.c.HMGet(ctx, idMapKey, accounts...).Result()
	if err != nil {
		return nil, fmt.Errorf("hmget id map: %w", err)
	}
	for i, raw := range vals {
		if id, ok := raw.(string); ok && id != "" {
			out[accounts[i]] = id
		}
	}
	return out, nil
}

// --- publisher ---

type natsPublisher struct {
	publish presencestore.PublishFunc
	siteID  string
}

func (n natsPublisher) Publish(ctx context.Context, account string, status model.PresenceStatus) {
	presencestore.PublishState(ctx, n.publish, n.siteID, account, status, time.Now())
}
