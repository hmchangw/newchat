package main

import "fmt"

// shardSlice returns the accounts this shard owns. With T =
// min(target, len(accounts)) (target 0 = whole pool), shard i of n owns
// accounts[floor(T*i/n) : floor(T*(i+1)/n)] — the shards partition exactly
// T accounts with no overlap and sizes differing by at most one (spec §5.1).
func shardSlice(accounts []string, target, index, count int) ([]string, error) {
	if count <= 0 || index < 0 || index >= count || target < 0 {
		return nil, fmt.Errorf("invalid shard parameters: target=%d index=%d count=%d", target, index, count)
	}
	t := target
	if t == 0 || t > len(accounts) {
		t = len(accounts)
	}
	start := t * index / count
	end := t * (index + 1) / count
	return accounts[start:end], nil
}
