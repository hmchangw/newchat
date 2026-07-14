package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// SustainedMembersConfig is the parameter bundle for an open-loop members
// generator.
type SustainedMembersConfig struct {
	Preset         *MembersPreset
	Fixtures       *Fixtures
	Pools          CandidatePools
	Owners         map[string]string
	Rate           int
	UsersPerAdd    int
	Inject         InjectMode
	Shape          Shape
	Publisher      MemberPublisher
	Metrics        *Metrics
	Collector      *MemberCollector
	WarmupDeadline time.Time
	MaxInFlight    int
}

// SustainedMembersGenerator publishes member-add requests at a target rate
// round-robin across the preset's rooms until ctx is cancelled or the pools
// run dry.
type SustainedMembersGenerator struct {
	cfg         SustainedMembersConfig
	presetLabel string
	injectLabel string
	shapeLabel  string
	mu          sync.Mutex
	pools       map[string][]string
	cursor      int
	roomIDs     []string
	rng         *rand.Rand
}

// ErrPoolsExhausted is returned by Run when every room's candidate pool has
// fewer than UsersPerAdd accounts remaining.
var ErrPoolsExhausted = errors.New("candidate pool exhausted on every room: rate * duration * usersPerAdd exceeded preset CandidatePool")

// ErrInsufficientPool is returned by ValidateSustainedCapacity (and wrapped
// with the specifics) when the requested rate/duration would consume more
// candidates than the preset's pools can supply. It exists so callers can
// match the preflight failure with errors.Is.
var ErrInsufficientPool = errors.New("insufficient candidate pool for requested workload")

// SustainableOps returns the maximum number of add-member publishes the given
// candidate pools can supply at usersPerAdd accounts each. A candidate is
// single-use (once added, the account is a room member and cannot be re-added),
// so this is a hard ceiling on the publishes a sustained run can ever make.
func SustainableOps(pools CandidatePools, usersPerAdd int) int {
	if usersPerAdd <= 0 {
		return 0
	}
	total := 0
	for _, pool := range pools {
		total += len(pool) / usersPerAdd
	}
	return total
}

// ValidateSustainedCapacity is the preflight guard for members-sustained. It
// fails fast (before any NATS/store work) when rate*duration would exhaust the
// preset's candidate pools, returning an actionable error wrapping
// ErrInsufficientPool that names the achievable max --rate and --duration.
func ValidateSustainedCapacity(presetName string, pools CandidatePools, rate int, duration time.Duration, usersPerAdd int) error {
	capacity := SustainableOps(pools, usersPerAdd)
	durSecs := duration.Seconds()
	demand := int(float64(rate) * durSecs)
	if demand <= capacity {
		return nil
	}
	maxRate := 0
	if durSecs > 0 {
		maxRate = int(float64(capacity) / durSecs)
	}
	maxDuration := time.Duration(0)
	if rate > 0 {
		maxDuration = (time.Duration(capacity) * time.Second) / time.Duration(rate)
	}
	// When the achievable bounds round to zero the preset simply cannot sustain
	// this workload (a smoke preset asked to run a real load). Steer to a bigger
	// preset / smaller --users-per-add instead of suggesting "--rate 0".
	var advice string
	if maxRate >= 1 && maxDuration >= time.Second {
		advice = fmt.Sprintf("lower to --rate %d (at this duration) or --duration %s (at this rate), or pick a larger preset",
			maxRate, maxDuration.Round(time.Second))
	} else {
		advice = fmt.Sprintf("too small for this workload: at --rate %d it sustains only ~%s — pick a larger preset or lower --users-per-add (and --duration)",
			rate, maxDuration.Round(time.Millisecond))
	}
	return fmt.Errorf(
		"preset %q candidate pools supply at most %d add-member ops at users-per-add=%d, "+
			"but rate=%d/s × duration=%s needs ~%d ops; %s: %w",
		presetName, capacity, usersPerAdd, rate, duration, demand, advice, ErrInsufficientPool,
	)
}

// ValidateCapacityTarget is the preflight guard for members-capacity. Capacity
// mode grows each room to targetSize by adding usersPerAdd members per batch and
// never sends a final partial batch, so a room with pool P reaches at most
// baseline + ⌊P/usersPerAdd⌋·usersPerAdd. The binding constraint is per-room
// pool depth (not aggregate throughput), so this fails fast when any room can't
// reach targetSize, naming how many rooms fall short and the reachable ceiling.
func ValidateCapacityTarget(presetName string, pools CandidatePools, baselineSize, targetSize, usersPerAdd int) error {
	if usersPerAdd <= 0 || targetSize <= baselineSize {
		return nil // nothing to grow, or usersPerAdd validated by the caller
	}
	batchesNeeded := (targetSize - baselineSize + usersPerAdd - 1) / usersPerAdd
	short := 0
	minBatches := -1
	for _, pool := range pools {
		batches := len(pool) / usersPerAdd
		if batches < batchesNeeded {
			short++
			if minBatches < 0 || batches < minBatches {
				minBatches = batches
			}
		}
	}
	if short == 0 {
		return nil
	}
	maxTarget := baselineSize + minBatches*usersPerAdd
	return fmt.Errorf(
		"preset %q cannot grow %d/%d rooms to target-size=%d at users-per-add=%d: "+
			"reaching it needs %d add batches/room but the thinnest pool supplies %d; "+
			"lower --target-size to %d or pick a larger preset: %w",
		presetName, short, len(pools), targetSize, usersPerAdd,
		batchesNeeded, minBatches, maxTarget, ErrInsufficientPool,
	)
}

// NewSustainedMembersGenerator clones the candidate pools so the input is
// not mutated.
func NewSustainedMembersGenerator(cfg *SustainedMembersConfig, seed int64) *SustainedMembersGenerator {
	pools := make(map[string][]string, len(cfg.Pools))
	roomIDs := make([]string, 0, len(cfg.Fixtures.Rooms))
	for i := range cfg.Fixtures.Rooms {
		r := &cfg.Fixtures.Rooms[i]
		pools[r.ID] = append([]string(nil), cfg.Pools[r.ID]...)
		roomIDs = append(roomIDs, r.ID)
	}
	return &SustainedMembersGenerator{
		cfg:         *cfg,
		presetLabel: cfg.Preset.Name,
		injectLabel: string(cfg.Inject),
		shapeLabel:  string(cfg.Shape),
		pools:       pools,
		roomIDs:     roomIDs,
		rng:         rand.New(rand.NewSource(seed)),
	}
}

// Run drives the publish loop. Returns ErrPoolsExhausted if every room runs
// out of candidates before ctx is cancelled. The arrival rate is paced by the
// batched pacer (events released per coarse tick), so achieved RPS is not
// capped by single-ticker resolution; events the pacer cannot release on
// schedule are tallied as the "underrun" publish-error reason (a load-box
// diagnostic, not a real publish failure).
func (g *SustainedMembersGenerator) Run(ctx context.Context) error {
	if g.cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if g.cfg.UsersPerAdd <= 0 {
		return fmt.Errorf("usersPerAdd must be > 0")
	}

	p := newPacer(g.cfg.Rate, time.Now())
	tick := time.NewTicker(p.interval)
	defer tick.Stop()

	var sem chan struct{}
	if g.cfg.MaxInFlight > 0 {
		sem = make(chan struct{}, g.cfg.MaxInFlight)
	}
	var wg sync.WaitGroup
	drain := func() {
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(drainGracePeriod):
		}
	}

	for {
		select {
		case <-ctx.Done():
			drain()
			return nil
		case <-tick.C:
			emit, underrun := p.tick(time.Now())
			if underrun > 0 {
				g.cfg.Metrics.MemberPublishErrors.WithLabelValues("underrun").Add(float64(underrun))
			}
			for i := 0; i < emit; i++ {
				roomID, accounts, ok := g.takeNext()
				if !ok {
					// Drain in-flight before returning so prior publishes complete.
					drain()
					return ErrPoolsExhausted
				}
				if sem == nil {
					g.publishOne(ctx, roomID, accounts)
					continue
				}
				select {
				case sem <- struct{}{}:
					wg.Add(1)
					go func() {
						defer func() { <-sem; wg.Done() }()
						g.publishOne(ctx, roomID, accounts)
					}()
				default:
					g.cfg.Metrics.MemberPublishErrors.WithLabelValues("saturated").Inc()
					g.giveBack(roomID, accounts)
				}
			}
		}
	}
}

// takeNext rotates through rooms looking for one with at least UsersPerAdd
// candidates. Returns (_, _, false) when every room is below the threshold.
func (g *SustainedMembersGenerator) takeNext() (string, []string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := len(g.roomIDs)
	for tried := 0; tried < n; tried++ {
		idx := (g.cursor + tried) % n
		roomID := g.roomIDs[idx]
		if len(g.pools[roomID]) < g.cfg.UsersPerAdd {
			continue
		}
		accounts := g.pools[roomID][:g.cfg.UsersPerAdd]
		g.pools[roomID] = g.pools[roomID][g.cfg.UsersPerAdd:]
		g.cursor = (idx + 1) % n
		return roomID, append([]string(nil), accounts...), true
	}
	return "", nil, false
}

func (g *SustainedMembersGenerator) giveBack(roomID string, accounts []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pools[roomID] = append(accounts, g.pools[roomID]...)
}

func (g *SustainedMembersGenerator) publishOne(ctx context.Context, roomID string, accounts []string) {
	owner := g.cfg.Owners[roomID]
	now := time.Now()
	req := &model.AddMembersRequest{
		RoomID:           roomID,
		Users:            accounts,
		RequesterAccount: owner,
		Timestamp:        now.UTC().UnixMilli(),
	}
	corrID := idgen.GenerateRequestID()
	g.cfg.Collector.RecordPublish(corrID, roomID, accounts, now)

	if err := g.cfg.Publisher.Publish(ctx, owner, roomID, req, corrID); err != nil {
		g.cfg.Collector.RecordPublishFailed(corrID, roomID, accounts)
		g.cfg.Metrics.MemberPublishErrors.WithLabelValues("publish").Inc()
		g.giveBack(roomID, accounts)
		return
	}
	phase := "measured"
	if now.Before(g.cfg.WarmupDeadline) {
		phase = "warmup"
	}
	g.cfg.Metrics.MemberPublished.WithLabelValues(g.presetLabel, phase, g.injectLabel, g.shapeLabel).Inc()
}

// CapacityMembersConfig parameterizes the per-room sequential growth generator.
type CapacityMembersConfig struct {
	Preset      *MembersPreset
	Fixtures    *Fixtures
	Pools       CandidatePools
	Owners      map[string]string
	UsersPerAdd int
	Inject      InjectMode
	Shape       Shape
	TargetSize  int
	MaxRate     int
	Publisher   MemberPublisher
	Metrics     *Metrics
	Collector   *MemberCollector
	E2Timeout   time.Duration
}

// CapacityMembersGenerator drives each room to TargetSize sequentially. Per-
// room loops run concurrently so a slow room does not gate the others.
type CapacityMembersGenerator struct {
	cfg         CapacityMembersConfig
	presetLabel string
	injectLabel string
	shapeLabel  string
}

// NewCapacityMembersGenerator creates a new capacity-mode generator.
func NewCapacityMembersGenerator(cfg *CapacityMembersConfig) *CapacityMembersGenerator {
	return &CapacityMembersGenerator{
		cfg:         *cfg,
		presetLabel: cfg.Preset.Name,
		injectLabel: string(cfg.Inject),
		shapeLabel:  string(cfg.Shape),
	}
}

// Run runs each room until TargetSize or pool exhaustion. Returns nil when
// every room has finished (or ctx cancelled).
func (g *CapacityMembersGenerator) Run(ctx context.Context) error {
	if g.cfg.UsersPerAdd <= 0 {
		return fmt.Errorf("usersPerAdd must be > 0")
	}
	if g.cfg.TargetSize <= 0 {
		return fmt.Errorf("targetSize must be > 0")
	}

	perRoom := make(map[string]chan struct{}, len(g.cfg.Fixtures.Rooms))
	for i := range g.cfg.Fixtures.Rooms {
		perRoom[g.cfg.Fixtures.Rooms[i].ID] = make(chan struct{}, 1)
	}
	g.cfg.Collector.OnMemberEvent(func(roomID string, _ []string) {
		if ch, ok := perRoom[roomID]; ok {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})

	var wg sync.WaitGroup
	for i := range g.cfg.Fixtures.Rooms {
		wg.Add(1)
		room := g.cfg.Fixtures.Rooms[i]
		go func() {
			defer wg.Done()
			g.runRoom(ctx, &room, perRoom[room.ID])
		}()
	}
	wg.Wait()
	return nil
}

func (g *CapacityMembersGenerator) runRoom(ctx context.Context, room *model.Room, ack <-chan struct{}) {
	size := g.cfg.Preset.BaselineSize
	pool := append([]string(nil), g.cfg.Pools[room.ID]...)
	owner := g.cfg.Owners[room.ID]

	var interval time.Duration
	if g.cfg.MaxRate > 0 {
		interval = time.Second / time.Duration(g.cfg.MaxRate)
	}
	var lastSent time.Time

	for size < g.cfg.TargetSize {
		if len(pool) < g.cfg.UsersPerAdd {
			return
		}
		if interval > 0 {
			if delay := interval - time.Since(lastSent); delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
			}
		}
		accounts := pool[:g.cfg.UsersPerAdd]
		pool = pool[g.cfg.UsersPerAdd:]

		now := time.Now()
		lastSent = now
		req := &model.AddMembersRequest{
			RoomID:           room.ID,
			Users:            accounts,
			RequesterAccount: owner,
			Timestamp:        now.UTC().UnixMilli(),
		}
		corrID := idgen.GenerateRequestID()
		g.cfg.Collector.RecordPublish(corrID, room.ID, accounts, now)

		if err := g.cfg.Publisher.Publish(ctx, owner, room.ID, req, corrID); err != nil {
			g.cfg.Collector.RecordPublishFailed(corrID, room.ID, accounts)
			g.cfg.Metrics.MemberPublishErrors.WithLabelValues("publish").Inc()
			return
		}
		g.cfg.Metrics.MemberPublished.WithLabelValues(g.presetLabel, "measured", g.injectLabel, g.shapeLabel).Inc()

		select {
		case <-ack:
			size += g.cfg.UsersPerAdd
			g.cfg.Metrics.MemberRoomSize.WithLabelValues(room.ID).Set(float64(size))
		case <-time.After(g.cfg.E2Timeout):
			g.cfg.Metrics.MemberPublishErrors.WithLabelValues("timeout").Inc()
			return
		case <-ctx.Done():
			return
		}
	}
}
