// Package main: Valkey write helpers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// restrictedCacheTTL matches search-service's RESTRICTED_ROOMS_CACHE_TTL default.
const restrictedCacheTTL = 5 * time.Minute

func restrictedCacheKey(account string) string {
	return fmt.Sprintf("searchservice:restrictedrooms:%s", account)
}

func restrictedCachePayload(e RestrictedCacheEntry) (string, error) {
	rooms := e.Rooms
	if rooms == nil {
		rooms = map[string]int64{}
	}
	b, err := json.Marshal(rooms)
	if err != nil {
		return "", fmt.Errorf("marshal restricted cache entry: %w", err)
	}
	return string(b), nil
}

// valkeyCounts captures the number of Valkey writes for the summary log.
type valkeyCounts struct {
	RoomKeys     int
	CacheEntries int
}

// writeValkey writes every seeded room key (via roomkeystore.Set, which uses
// the exact production hash format) and every restricted-rooms cache entry
// (via valkeyutil.Client.Set with restrictedCacheTTL).
func writeValkey(ctx context.Context, keys roomkeystore.RoomKeyStore, client valkeyutil.Client) (valkeyCounts, error) {
	var c valkeyCounts
	for _, k := range BuildRoomKeys() {
		if _, err := keys.Set(ctx, k.RoomID, k.KeyPair); err != nil {
			return c, fmt.Errorf("set room key for %s: %w", k.RoomID, err)
		}
		c.RoomKeys++
	}
	for _, e := range BuildRestrictedCache() {
		payload, err := restrictedCachePayload(e)
		if err != nil {
			return c, fmt.Errorf("encode restricted cache for %s: %w", e.Account, err)
		}
		if err := client.Set(ctx, restrictedCacheKey(e.Account), payload, restrictedCacheTTL); err != nil {
			return c, fmt.Errorf("set restricted cache for %s: %w", e.Account, err)
		}
		c.CacheEntries++
	}
	return c, nil
}

// deleteValkey removes every key this seeder owns from Valkey.
// Order: room keys first (current + previous slot via roomkeystore.Delete),
// then cache entries via client.Del.
func deleteValkey(ctx context.Context, keys roomkeystore.RoomKeyStore, client valkeyutil.Client) error {
	for _, k := range BuildRoomKeys() {
		if err := keys.Delete(ctx, k.RoomID); err != nil {
			return fmt.Errorf("delete room key for %s: %w", k.RoomID, err)
		}
	}
	for _, e := range BuildRestrictedCache() {
		if err := client.Del(ctx, restrictedCacheKey(e.Account)); err != nil {
			return fmt.Errorf("delete restricted cache for %s: %w", e.Account, err)
		}
	}
	return nil
}
