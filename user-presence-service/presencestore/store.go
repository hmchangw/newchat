package presencestore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

const keyPrefix = "presence:"
const sweepKey = keyPrefix + "sweep"

func connsKey(account string) string  { return keyPrefix + "{" + account + "}:conns" }
func manualKey(account string) string { return keyPrefix + "{" + account + "}:manual" }
func statusKey(account string) string { return keyPrefix + "{" + account + "}:status" }
func azureKey(account string) string  { return keyPrefix + "{" + account + "}:azure" }

// StatusChange is an account whose effective status was (re)computed.
type StatusChange struct {
	Account   string
	Effective model.PresenceStatus
}

// PublishFunc publishes data to a subject (core NATS).
type PublishFunc func(ctx context.Context, subj string, data []byte) error

// PublishState marshals + publishes a PresenceState to the account's state
// subject. Failures are logged (best-effort fan-out, no caller to surface to).
func PublishState(ctx context.Context, publish PublishFunc, siteID, account string, status model.PresenceStatus, now time.Time) {
	st := model.PresenceState{Account: account, SiteID: siteID, Status: status, Timestamp: now.UTC().UnixMilli()}
	data, err := natsutil.MarshalResponse(st)
	if err != nil {
		slog.Error("publish presence state failed: marshal", "error", err, "account", account)
		return
	}
	if err := publish(ctx, subject.PresenceState(account), data); err != nil {
		slog.Error("publish presence state failed", "error", err, "account", account)
	}
}

// Each connection is stored in the conns hash as field=connID,
// value="<away01>:<lastSeenMs>" where away01 is "0" (active) or "1" (inactive).
// The scripts are split per operation (per review): ping is a cheap liveness
// refresh that skips recompute for known connections, while activity/bye/manual/
// sweep/external recompute the aggregate via the shared computeLua tail.

// luaHeader binds now/stale from ARGV[1]/ARGV[2] for every script.
const luaHeader = `
local now   = tonumber(ARGV[1])
local stale = tonumber(ARGV[2])
`

// computeLua prunes stale connections, derives availability (online if any
// active connection, away if all inactive, offline if none), overlays the
// manual override and external (Teams) status per the precedence in the project
// spec, CAS-writes the materialized status, and returns
// {changed(0/1), effective, nextDeadlineMs(-1 if none)}.
// Precedence (only while live):
//  1. no live connection            -> offline
//  2. appear_offline                -> offline
//  3. manual away / brb             -> away
//  4. external in-call              -> in-call
//  5. manual online / busy / dnd    -> manual
//  6. all inactive                  -> away
//  7. otherwise                     -> online
//
// KEYS[1]=conns hash  KEYS[2]=manual  KEYS[3]=status  KEYS[4]=azure
const computeLua = `
local conns = redis.call('HGETALL', KEYS[1])
local anyLive, anyActive = false, false
local nextDeadline = -1
for i = 1, #conns, 2 do
  local field, val = conns[i], conns[i+1]
  local sep = string.find(val, ':')
  local away = string.sub(val, 1, sep - 1)
  local lastSeen = tonumber(string.sub(val, sep + 1))
  if now - lastSeen > stale then
    redis.call('HDEL', KEYS[1], field)
  else
    anyLive = true
    if away == '0' then anyActive = true end
    local d = lastSeen + stale
    if nextDeadline == -1 or d < nextDeadline then nextDeadline = d end
  end
end

local manual = redis.call('GET', KEYS[2])
local azure  = redis.call('GET', KEYS[4])
local m = ''
if type(manual) == 'string' then m = manual end
local a = ''
if type(azure) == 'string' then a = azure end

local effective
if not anyLive then
  effective = 'offline'
elseif m == 'appear_offline' then
  effective = 'offline'
elseif m == 'away' or m == 'brb' then
  effective = 'away'
elseif a == 'in-call' then
  effective = 'in-call'
elseif m == 'online' or m == 'busy' or m == 'dnd' then
  effective = m
elseif anyActive then
  effective = 'online'
else
  effective = 'away'
end

local prev = redis.call('GET', KEYS[3])
local changed = 0
if prev ~= effective then
  redis.call('SET', KEYS[3], effective)
  changed = 1
end
return {changed, effective, nextDeadline}
`

// pingScript refreshes a connection's lastSeen + TTL. For an already-known
// connection it skips recompute and returns changed=0 with an empty status —
// the caller never publishes on an unchanged ping, so it doesn't read the
// status, saving a GET round trip. Only a brand-new connection (the
// offline->online edge) runs the full recompute.
// ARGV[3]=connID  ARGV[4]=conns_ttl_ms
var pingScript = redis.NewScript(luaHeader + `
local connID = ARGV[3]
if redis.call('HEXISTS', KEYS[1], connID) == 1 then
  local v = redis.call('HGET', KEYS[1], connID)
  local sep = string.find(v, ':')
  local away = string.sub(v, 1, sep - 1)
  redis.call('HSET', KEYS[1], connID, away .. ':' .. now)
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
  return {0, '', now + stale}
end
redis.call('HSET', KEYS[1], connID, '0:' .. now)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
` + computeLua)

// activityScript upserts a connection with the given away flag, then recomputes.
// ARGV[3]=connID  ARGV[4]=away01  ARGV[5]=conns_ttl_ms
var activityScript = redis.NewScript(luaHeader + `
redis.call('HSET', KEYS[1], ARGV[3], ARGV[4] .. ':' .. now)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[5]))
` + computeLua)

// byeScript drops a connection, then recomputes (shared logic with sweep).
// ARGV[3]=connID
var byeScript = redis.NewScript(luaHeader + `
redis.call('HDEL', KEYS[1], ARGV[3])
` + computeLua)

// manualScript sets the manual override string, or clears it when ARGV[3] is
// the empty string, then recomputes.  ARGV[3]=status
var manualScript = redis.NewScript(luaHeader + `
if ARGV[3] == '' then
  redis.call('DEL', KEYS[2])
else
  redis.call('SET', KEYS[2], ARGV[3])
end
` + computeLua)

// externalScript sets the external (Teams) status key with a TTL safety-net, or
// clears it when ARGV[3] is the empty string, then recomputes.
// ARGV[3]=status  ARGV[4]=external_ttl_ms
var externalScript = redis.NewScript(luaHeader + `
if ARGV[3] == '' then
  redis.call('DEL', KEYS[4])
else
  redis.call('SET', KEYS[4], ARGV[3])
  redis.call('PEXPIRE', KEYS[4], tonumber(ARGV[4]))
end
` + computeLua)

// sweepScript only prunes stale connections and recomputes (no mutation).
var sweepScript = redis.NewScript(luaHeader + computeLua)

// Store is the Valkey-backed presence state.
type Store struct {
	c        *redis.ClusterClient
	staleMs  int64
	connsTTL int64 // ms
}

// ClusterConfig holds Valkey cluster connection config.
type ClusterConfig struct {
	Addrs    []string
	Password string
}

// NewValkeyStore dials the cluster, pings it, and returns a Store.
func NewValkeyStore(cfg ClusterConfig, staleThreshold, connsTTL time.Duration) (*Store, error) {
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: cfg.Addrs, Password: cfg.Password})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed connect", "error", closeErr)
		}
		return nil, fmt.Errorf("valkey cluster connect: %w", err)
	}
	return NewValkeyStoreFromClient(c, staleThreshold, connsTTL), nil
}

// NewValkeyStoreFromClient wraps a pre-built client (tests inject a
// ClusterSlots-override client).
func NewValkeyStoreFromClient(c *redis.ClusterClient, staleThreshold, connsTTL time.Duration) *Store {
	return &Store{c: c, staleMs: staleThreshold.Milliseconds(), connsTTL: connsTTL.Milliseconds()}
}

// run executes one presence script and parses its {changed, effective,
// nextDeadline} reply. now/stale are prepended as ARGV[1]/ARGV[2]; callers pass
// any op-specific args after.
func (s *Store) run(ctx context.Context, script *redis.Script, account string, now int64, args ...any) (bool, model.PresenceStatus, int64, error) {
	argv := append([]any{strconv.FormatInt(now, 10), strconv.FormatInt(s.staleMs, 10)}, args...)
	res, err := script.Run(ctx, s.c,
		[]string{connsKey(account), manualKey(account), statusKey(account), azureKey(account)}, argv...,
	).Slice()
	if err != nil {
		return false, "", 0, fmt.Errorf("presence script %q: %w", account, err)
	}
	if len(res) != 3 {
		return false, "", 0, fmt.Errorf("presence script %q: unexpected result arity %d", account, len(res))
	}
	changed, _ := res[0].(int64)
	effective, _ := res[1].(string)
	nextDeadline, _ := res[2].(int64)
	return changed == 1, model.PresenceStatus(effective), nextDeadline, nil
}

// reschedule updates the sweep ZSET for an account based on its next deadline.
func (s *Store) reschedule(ctx context.Context, account string, nextDeadline int64) error {
	if nextDeadline < 0 {
		if err := s.c.ZRem(ctx, sweepKey, account).Err(); err != nil {
			return fmt.Errorf("sweep zrem %q: %w", account, err)
		}
		return nil
	}
	if err := s.c.ZAdd(ctx, sweepKey, redis.Z{Score: float64(nextDeadline), Member: account}).Err(); err != nil {
		return fmt.Errorf("sweep zadd %q: %w", account, err)
	}
	return nil
}

// mutate runs a mutating script and reschedules the account's sweep deadline.
func (s *Store) mutate(ctx context.Context, account string, script *redis.Script, args ...any) (bool, model.PresenceStatus, error) {
	now := time.Now().UTC().UnixMilli()
	changed, eff, next, err := s.run(ctx, script, account, now, args...)
	if err != nil {
		return false, "", err
	}
	if err := s.reschedule(ctx, account, next); err != nil {
		return false, "", err
	}
	return changed, eff, nil
}

func (s *Store) Ping(ctx context.Context, account, connID string) (bool, model.PresenceStatus, error) {
	return s.mutate(ctx, account, pingScript, connID, strconv.FormatInt(s.connsTTL, 10))
}

func (s *Store) SetActivity(ctx context.Context, account, connID string, away bool) (bool, model.PresenceStatus, error) {
	flag := "0"
	if away {
		flag = "1"
	}
	return s.mutate(ctx, account, activityScript, connID, flag, strconv.FormatInt(s.connsTTL, 10))
}

func (s *Store) RemoveConnection(ctx context.Context, account, connID string) (bool, model.PresenceStatus, error) {
	return s.mutate(ctx, account, byeScript, connID)
}

func (s *Store) SetManual(ctx context.Context, account string, status model.PresenceStatus) (bool, model.PresenceStatus, error) {
	// Stored as the plain status string (per review) — the script only needs the
	// status, so there is no JSON to decode in Lua.
	return s.mutate(ctx, account, manualScript, string(status))
}

// SetExternal sets (status == StatusInCall) or clears (status == StatusNone)
// the external Teams override and recomputes. ttl bounds the external key's
// lifetime so a dead sync self-heals.
func (s *Store) SetExternal(ctx context.Context, account string, status model.PresenceStatus, ttl time.Duration) (bool, model.PresenceStatus, error) {
	statusArg := string(status)
	if status == model.StatusNone {
		statusArg = ""
	}
	return s.mutate(ctx, account, externalScript, statusArg, strconv.FormatInt(ttl.Milliseconds(), 10))
}

// ActiveAccounts returns every account with at least one live connection — the
// members of the sweep index (an account is ZREM'd once it fully disconnects).
// The Teams sync uses this to scope reconciliation to users who can actually be
// shown in-call (a disconnected user is offline regardless of Teams state).
func (s *Store) ActiveAccounts(ctx context.Context) ([]string, error) {
	accounts, err := s.c.ZRange(ctx, sweepKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("active accounts zrange: %w", err)
	}
	return accounts, nil
}

func (s *Store) BatchGet(ctx context.Context, accounts []string) (map[string]model.PresenceStatus, error) {
	out := make(map[string]model.PresenceStatus, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	pipe := s.c.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(accounts))
	for _, a := range accounts {
		if _, seen := cmds[a]; seen {
			continue
		}
		cmds[a] = pipe.Get(ctx, statusKey(a))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("batch get: %w", err)
	}
	for a, cmd := range cmds {
		v, err := cmd.Result()
		if err == redis.Nil || v == "" {
			out[a] = model.StatusOffline
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("batch get %q: %w", a, err)
		}
		out[a] = model.PresenceStatus(v)
	}
	return out, nil
}

func (s *Store) Sweep(ctx context.Context, now time.Time) ([]StatusChange, error) {
	nowMs := now.UTC().UnixMilli()
	accounts, err := s.c.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: sweepKey, ByScore: true, Start: "-inf", Stop: strconv.FormatInt(nowMs, 10), Offset: 0, Count: 500,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("sweep zrange: %w", err)
	}
	var changes []StatusChange
	for _, a := range accounts {
		changed, eff, next, rerr := s.run(ctx, sweepScript, a, nowMs)
		if rerr != nil {
			return changes, rerr
		}
		if rerr := s.reschedule(ctx, a, next); rerr != nil {
			return changes, rerr
		}
		if changed {
			changes = append(changes, StatusChange{Account: a, Effective: eff})
		}
	}
	return changes, nil
}

func (s *Store) Close() error { return s.c.Close() }
