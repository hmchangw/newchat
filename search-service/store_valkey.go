package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/cachekeys"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// valkeyCache keys are namespaced under `searchservice:restrictedrooms:`
// so the cache can coexist with other services on the same Valkey.
type valkeyCache struct {
	client valkeyutil.Client
}

func newValkeyCache(client valkeyutil.Client) *valkeyCache {
	return &valkeyCache{client: client}
}

func restrictedKey(account string) string {
	return cachekeys.SearchRestrictedRooms(account)
}

// GetRestricted returns the cached restricted-rooms map for `account`.
// hit=false covers both "key absent" and "transport failure" — the handler
// treats both as cache misses and falls through to ES. Transport-level
// errors are returned via `err` so the handler can log them for visibility;
// hit=true implies err==nil.
func (c *valkeyCache) GetRestricted(ctx context.Context, account string) (map[string]int64, bool, error) {
	var rooms map[string]int64
	err := valkeyutil.GetJSON(ctx, c.client, restrictedKey(account), &rooms)
	if errors.Is(err, valkeyutil.ErrCacheMiss) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache get restricted: %w", err)
	}
	if rooms == nil {
		rooms = map[string]int64{}
	}
	return rooms, true, nil
}

func (c *valkeyCache) SetRestricted(ctx context.Context, account string, rooms map[string]int64, ttl time.Duration) error {
	if rooms == nil {
		rooms = map[string]int64{}
	}
	if err := valkeyutil.SetJSONWithTTL(ctx, c.client, restrictedKey(account), rooms, ttl); err != nil {
		return fmt.Errorf("cache set restricted: %w", err)
	}
	return nil
}
