package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// verifyConfig is the parsed CLI surface for the verify scenario.
type verifyConfig struct {
	Preset             string
	Users              int
	ProbeRooms         int
	ReserveUsers       int
	MemberChurn        float64
	Settle             time.Duration
	Warmup             time.Duration
	Steady             time.Duration
	Drain              time.Duration
	ProbeRate          float64
	MinProbes          int
	LargeRoomThreshold int
	Lane               string
	DirectOnly         bool
	Seed               int64
	JSONPath           string
}

// parseVerifyFlags parses the verify subcommand's flags and validates them.
func parseVerifyFlags(args []string) (verifyConfig, error) {
	var vc verifyConfig
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.StringVar(&vc.Preset, "preset", "daily-heavy", "daily-light | daily-heavy | daily-power")
	fs.IntVar(&vc.Users, "users", 0, "total activation count (0 = preset default)")
	fs.IntVar(&vc.ProbeRooms, "probe-rooms", 50, "number of probe rooms")
	fs.IntVar(&vc.ReserveUsers, "reserve-users", 200, "direct-connected floaters for membership changes")
	fs.Float64Var(&vc.MemberChurn, "member-churn", 0.2, "membership changes per probe room per minute (0 disables)")
	fs.DurationVar(&vc.Settle, "settle", 5*time.Second, "post-change quiet window per room")
	fs.DurationVar(&vc.Warmup, "warmup", 30*time.Second, "pre-measurement settle")
	fs.DurationVar(&vc.Steady, "steady", 120*time.Second, "probe-generating window")
	fs.DurationVar(&vc.Drain, "drain", 30*time.Second, "post-quiesce wait for in-flight probes")
	fs.Float64Var(&vc.ProbeRate, "probe-rate", 0.01, "fraction of probe-room sends tracked")
	fs.IntVar(&vc.MinProbes, "min-probes", 50, "below this, the verdict is INCONCLUSIVE")
	fs.IntVar(&vc.LargeRoomThreshold, "large-room-threshold", 500, "must match the gatekeeper's setting")
	fs.StringVar(&vc.Lane, "lane", "both", "global | local | both")
	fs.BoolVar(&vc.DirectOnly, "direct-only", false, "disable multiplex; every user gets a dedicated conn")
	fs.Int64Var(&vc.Seed, "seed", 42,
		"drives probe-room choice and probe selection; same seed ⇒ same probe set. "+
			"Fixtures are NOT seeded from this — they are pinned to the seed 'loadgen seed' uses, "+
			"because a different fixture seed regenerates every user and room ID")
	fs.StringVar(&vc.JSONPath, "json", "", "full violation detail output path")

	if err := fs.Parse(args); err != nil {
		return vc, fmt.Errorf("parse verify flags: %w", err)
	}
	switch vc.Lane {
	case "global", "local", "both":
	default:
		return vc, fmt.Errorf("invalid --lane %q: want global, local, or both", vc.Lane)
	}
	if vc.ProbeRate < 0 || vc.ProbeRate > 1 {
		return vc, fmt.Errorf("invalid --probe-rate %v: want a fraction in [0,1]", vc.ProbeRate)
	}
	if vc.ProbeRooms <= 0 {
		return vc, fmt.Errorf("invalid --probe-rooms %d: want a positive count", vc.ProbeRooms)
	}
	if vc.ReserveUsers < 0 {
		// A negative count reaches r.Perm(len(candidates))[:n] in selectReserve
		// and panics with a slice bounds error instead of a usage message.
		return vc, fmt.Errorf("invalid --reserve-users %d: want a non-negative count", vc.ReserveUsers)
	}
	if vc.MinProbes < 1 {
		return vc, fmt.Errorf(
			"invalid --min-probes %d: must be at least 1 — at 0 the probe floor in evaluateVerify "+
				"(tracked < min-probes) can never fire, so a run that tracked zero probes would "+
				"report PASS instead of INCONCLUSIVE", vc.MinProbes)
	}
	return vc, nil
}

// preflightVerify fails fast on configurations that would produce phantom
// violations. Seconds spent here beat a ten-minute run reporting a fake bug.
func preflightVerify(_ context.Context, vc verifyConfig, prs ProbeRoomSet, directSize int) error { //nolint:gocritic // hugeParam: verifyConfig is 152 bytes, but the by-value signature is fixed by this task's brief and its pinned test call sites (e.g. preflightVerify(t.Context(), vc, prs, 1)), not by any interface conformance
	for i := range prs.Rooms {
		r := &prs.Rooms[i]
		if r.UserCount >= vc.LargeRoomThreshold {
			return fmt.Errorf(
				"probe room %s has %d members, at or above --large-room-threshold=%d: "+
					"the gatekeeper rejects non-thread sends from member-role users there, "+
					"which is indistinguishable from message loss",
				r.ID, r.UserCount, vc.LargeRoomThreshold)
		}
	}
	if directSize < len(prs.Members) {
		return fmt.Errorf(
			"direct pool holds %d of %d probe-room members: a member without a dedicated "+
				"connection cannot be observed, which would corrupt the completeness verdict",
			directSize, len(prs.Members))
	}
	return nil
}

// orderForActivation puts designated users first so they land in the direct
// pool, preserving the original relative order of everyone else. With an empty
// designated set the input order is returned unchanged, which is exactly
// daily's existing behaviour.
func orderForActivation(users []*userState, designated []string) []string {
	want := make(map[string]struct{}, len(designated))
	for _, id := range designated {
		want[id] = struct{}{}
	}

	first := make([]string, 0, len(designated))
	rest := make([]string, 0, len(users))
	for _, u := range users {
		if _, ok := want[u.ID]; ok {
			first = append(first, u.ID)
			continue
		}
		rest = append(rest, u.ID)
	}
	return append(first, rest...)
}

// runVerify is the verify subcommand entry point.
func runVerify(ctx context.Context, cfg *config, args []string) int {
	vc, err := parseVerifyFlags(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0 // -h / --help already printed usage
		}
		slog.Error("verify flag parse failed", "err", err)
		return 2
	}

	// BuiltinPreset returns a value; BuildFixtures takes a pointer.
	preset, ok := BuiltinPreset(vc.Preset)
	if !ok {
		slog.Error("unknown preset", "preset", vc.Preset)
		return 2
	}
	if vc.Users > 0 {
		preset.Users = vc.Users
	}

	// Same preflight daily runs, for the same reason and a worse consequence:
	// against an unseeded (or mis-seeded) database the gatekeeper rejects every
	// send with "user X is not subscribed to room Y", yet the publishes still
	// succeed — so every probe would reach zero recipients and the run would
	// report a confident FAIL with a total_loss per probe.
	if err := verifyDailySeeded(ctx, cfg, dailyConfig{Preset: vc.Preset, Users: vc.Users}); err != nil {
		slog.Error("verify pre-flight", "err", err)
		return 2
	}

	// Fixtures are pinned to dailyRunSeed, NOT vc.Seed. BuildFixtures is
	// deterministic in (preset, seed, siteID), so a different seed here would
	// generate entirely different user and room IDs from the ones `loadgen
	// seed` wrote — and verifyDailySeeded cannot catch that, since the user
	// count still matches. vc.Seed drives probe-room choice and probe
	// selection only, which is what reproducibility actually needs.
	fx := BuildFixtures(&preset, dailyRunSeed, cfg.SiteID)

	prs, err := selectProbeRooms(fx, vc.ProbeRooms, vc.LargeRoomThreshold, vc.Seed)
	if err != nil {
		slog.Error("probe room selection failed", "err", err)
		return 2
	}
	reserve := selectReserve(fx, prs, vc.ReserveUsers, vc.Seed)
	designated := append(append([]string(nil), prs.Members...), reserve...)

	slog.Info("verify starting",
		"preset", vc.Preset,
		"probeRooms", len(prs.Rooms),
		"probeMembers", len(prs.Members),
		"reserve", len(reserve),
		"probeRate", vc.ProbeRate,
		"memberChurn", vc.MemberChurn)

	rep, err := executeVerify(ctx, cfg, &vc, &fx, prs, designated)
	if err != nil {
		slog.Error("verify run failed", "err", err)
		return 2
	}

	fmt.Print(renderVerifyConsole(rep))

	if vc.JSONPath != "" {
		raw, err := renderVerifyJSON(rep)
		if err != nil {
			slog.Error("render json report failed", "err", err)
			return 2
		}
		if err := os.WriteFile(vc.JSONPath, raw, 0o600); err != nil {
			slog.Error("write json report failed", "path", vc.JSONPath, "err", err)
			return 2
		}
	}

	return rep.Result.Verdict.ExitCode()
}

// verifyProbeContent is the body of every send verify issues. Fixed, opaque,
// and never logged — verify asserts on metadata only (spec §14).
const verifyProbeContent = "loadtest content"

const (
	// verifyProbeHeadroom multiplies --min-probes when sizing the probe-send
	// rate, so a run that loses some sends to settle windows still clears the
	// floor instead of reporting INCONCLUSIVE.
	verifyProbeHeadroom = 3.0
	// verifyMaxProbeSendRate caps probe sends/sec so a tiny --probe-rate can't
	// turn the correctness run into an unintended throughput test.
	verifyMaxProbeSendRate = 200.0
	// verifyChurnPoll is how often the churn driver wakes to issue a change or
	// harvest a settled one. Well below the 5s default --settle.
	verifyChurnPoll = 500 * time.Millisecond
	// verifyChurnObservation is the budget one settled change needs AFTER its
	// settle window: harvest has to page subscription.list and run a send/reply
	// round trip before the steady window closes. The effective tailroom is
	// this PLUS --settle — see churnTailroom.
	verifyChurnObservation = 10 * time.Second
	// verifyChurnTailroom is the floor on that tailroom, so a zero --settle
	// still leaves room for the two observations.
	verifyChurnTailroom = 10 * time.Second
	// verifyReadbackAttempts / verifyReadbackBackoff bound the persistence
	// retry budget — message-worker is asynchronous, so a first-read miss is
	// often lag rather than loss (spec §8).
	verifyReadbackAttempts = 4
	verifyReadbackBackoff  = 4 * time.Second
	// verifyOracleMaxPages bounds subscription.list paging so a runaway HasMore
	// can't spin the churn driver.
	verifyOracleMaxPages = 20
	verifyOraclePageSize = 200
	// verifyGCPauseMaxMs matches daily's self-saturation guard: above this the
	// load box, not the system under test, is the likely explanation.
	verifyGCPauseMaxMs = 50.0
)

// verifyRun is the shared state of one verify run: the probe emitter, the churn
// driver, and the finalizer all read it.
type verifyRun struct {
	vc      *verifyConfig
	env     *stepEnv
	prs     ProbeRoomSet
	tracker *ProbeTracker
	mm      *MembershipModel
	siteID  string

	byID    map[string]*userState // userID -> runtime state (account lookup)
	idxByID map[string]int        // userID -> fixture index, for shouldProbe
	reserve []string              // floaters, the only churn targets

	// oracleNC is a dedicated connection used to observe the gatekeeper's
	// reply to a post-settle send. The gatekeeper answers on
	// chat.user.{account}.response.{requestId} rather than a request inbox, so
	// acceptance is only observable with a real subscription.
	oracleNC *nats.Conn

	mu        sync.Mutex
	targets   []ReadbackTarget
	oracleErr error
}

// executeVerify runs the full lifecycle: build pools, activate, preflight,
// warmup, steady (probes + churn), quiesce, drain, readback, finalize.
func executeVerify(
	ctx context.Context,
	cfg *config,
	vc *verifyConfig,
	fx *Fixtures,
	prs ProbeRoomSet,
	designated []string,
) (VerifyReport, error) {
	users := buildVerifyUsers(fx)
	activate := len(users)
	if vc.Users > 0 && vc.Users < activate {
		activate = vc.Users
	}
	if activate < len(designated) {
		return VerifyReport{}, fmt.Errorf(
			"--users=%d activates fewer than the %d designated users (probe-room members plus reserve): "+
				"every probe recipient must be live on a dedicated connection", vc.Users, len(designated))
	}

	env := (&prodEnvFactory{baseCfg: cfg}).Build(verifyDailyConfig(vc, len(users), len(designated)), users)
	defer closePools(env)
	env.runSeed = vc.Seed
	env.designated = designated

	// Lane selection and the delivery sink must both be wired before the first
	// Add: Add subscribes and flushes, so a broadcast can arrive the instant it
	// returns, and the lane set decides what it subscribes to.
	env.direct.setLanes(laneFlags(vc.Lane))
	tracker := NewProbeTracker()
	env.direct.attachSink(tracker)

	mm := NewMembershipModel(prs)
	mm.SetSettle(vc.Settle)

	run := &verifyRun{
		vc: vc, env: env, prs: prs, tracker: tracker, mm: mm, siteID: env.siteID,
		byID:    indexUsersByID(users),
		idxByID: indexPositions(users),
		// designated is the probe-room member union followed by the reserve
		// floaters (see runVerify); the tail is what churn may target.
		reserve: designated[min(len(designated), len(prs.Members)):],
	}

	// Anchor the diurnal envelope once, spanning warmup+steady, so the daily
	// emitters produce background load for the whole run.
	env.setHold(time.Now(), vc.Warmup+vc.Steady)

	emitterCtx, cancelEmitters := context.WithCancel(ctx)
	defer cancelEmitters()
	activateUsers(emitterCtx, env, 0, activate)
	slog.Info("verify activated",
		"activated", env.activatedCount.Load(),
		"skipped", env.skippedCount.Load(),
		"direct", env.direct.Size())

	// Preflight runs after activation: it asserts the direct pool actually
	// holds every probe-room member, which is only knowable once they are in.
	if err := preflightVerify(ctx, *vc, prs, env.direct.Size()); err != nil {
		return VerifyReport{}, err
	}
	// Size alone is not the assertion spec §16.5 promises: activateUsers skips
	// a user whose Add failed and the next user backfills the slot, so the
	// count can be right with a probe recipient absent. That user would then
	// miss every probe in its rooms and register as missing_recipient against
	// a healthy system. Check the actual membership.
	if missing := env.direct.MissingUsers(prs.Members); len(missing) > 0 {
		return VerifyReport{}, fmt.Errorf(
			"%d of %d probe-room members are absent from the direct pool (first: %s): "+
				"an unobserved recipient would be reported as missing_recipient",
			len(missing), len(prs.Members), missing[0])
	}
	// A reserve floater that failed to connect cannot be a churn target — its
	// SubscribeRoom would fail and abort churn. Drop it instead.
	run.reserve = run.dropAbsentReserve()

	if err := run.openOracleConn(cfg); err != nil {
		return VerifyReport{}, err
	}
	defer run.closeOracleConn()

	_ = waitOrCancel(ctx, vc.Warmup)

	steadyCtx, cancelSteady := context.WithCancel(ctx)
	defer cancelSteady()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		run.emitProbes(steadyCtx)
	}()
	if vc.MemberChurn > 0 {
		// Stop issuing changes before the window closes so the last one still
		// gets its settle window and its two observations inside the run.
		tailroom := churnTailroom(vc.Settle)
		if vc.Steady <= tailroom {
			slog.Warn("steady window too short for membership churn; no changes will be issued",
				"steady", vc.Steady, "settle", vc.Settle, "tailroom", tailroom)
		}
		issueUntil := time.Now().Add(vc.Steady - tailroom)
		wg.Add(1)
		go func() {
			defer wg.Done()
			run.driveChurn(steadyCtx, issueUntil)
		}()
	}

	_ = waitOrCancel(ctx, vc.Steady)

	// Quiesce: stop every publisher, keep every subscription live.
	cancelSteady()
	cancelEmitters()
	wg.Wait()

	// Drain: in-flight probes land against still-listening receivers.
	_ = waitOrCancel(ctx, vc.Drain)

	readbackViolations, readbackErr := run.readback(ctx)

	violations := tracker.Finalize()
	violations = append(violations, mm.Finalize()...)
	violations = append(violations, readbackViolations...)

	in := VerifyInputs{
		Violations:     violations,
		Counts:         tracker.Counts(),
		Changes:        mm.Counts(),
		MinProbes:      vc.MinProbes,
		MultiplexDrops: env.collector.MultiplexDrops(),
		// A recipient whose connection went away mid-run cannot have its
		// non-delivery attributed to the system under test (spec §10).
		DroppedRecipients: int(env.direct.DroppedRecipients()),
		ReadbackErr:       readbackErr,
		OracleErr:         run.takeOracleErr(),
		Cancelled:         ctx.Err() != nil,
		GCPauseP99:        snapshotSelfMetrics().GCPauseP99Ms,
		GCPauseMax:        verifyGCPauseMaxMs,
	}

	directSize := env.direct.Size()
	return VerifyReport{
		ProbeRooms:     len(prs.Rooms),
		ProbeMembers:   len(prs.Members),
		DirectPoolSize: directSize,
		ReserveSize:    len(run.reserve),
		BackgroundSize: int(env.activatedCount.Load()) - directSize,
		Counts:         in.Counts,
		Changes:        in.Changes,
		Result:         evaluateVerify(in),
	}, nil
}

// verifyDailyConfig maps verify's flags onto the dailyConfig prodEnvFactory
// consumes. The direct cap is sized to cover every designated user so probe
// recipients never spill into the multiplex pool (spec §6.2).
func verifyDailyConfig(vc *verifyConfig, totalUsers, designated int) dailyConfig {
	dc := dailyConfig{
		Preset:            vc.Preset,
		Users:             vc.Users,
		Warmup:            vc.Warmup,
		Hold:              vc.Steady,
		MaxDirectUsers:    designated,
		MultiplexPoolSize: 200,
	}
	if vc.DirectOnly {
		dc.MaxDirectUsers = totalUsers
		dc.MultiplexPoolSize = 0
	}
	return dc
}

// laneFlags maps --lane onto directPool's lane selection (true = global,
// false = local). An unrecognised value keeps both lanes; parseVerifyFlags
// already rejects those, so this is only the safe default.
func laneFlags(lane string) []bool {
	switch lane {
	case "global":
		return []bool{true}
	case "local":
		return []bool{false}
	default:
		return []bool{true, false}
	}
}

// dropAbsentReserve removes floaters that never made it into the direct pool,
// so churn only ever targets a user it can observe.
func (r *verifyRun) dropAbsentReserve() []string {
	missing := r.env.direct.MissingUsers(r.reserve)
	if len(missing) == 0 {
		return r.reserve
	}
	absent := make(map[string]struct{}, len(missing))
	for _, id := range missing {
		absent[id] = struct{}{}
	}
	kept := make([]string, 0, len(r.reserve)-len(missing))
	for _, id := range r.reserve {
		if _, ok := absent[id]; !ok {
			kept = append(kept, id)
		}
	}
	slog.Warn("reserve floaters absent from the direct pool; excluded from membership churn",
		"absent", len(missing), "remaining", len(kept))
	return kept
}

func buildVerifyUsers(fx *Fixtures) []*userState {
	userRooms := groupSubsByUser(fx.Subscriptions)
	users := make([]*userState, len(fx.Users))
	for i := range fx.Users {
		u := &fx.Users[i]
		users[i] = newUserState(u.ID, u.Account, userRooms[u.ID], int64(i))
	}
	return users
}

func indexUsersByID(users []*userState) map[string]*userState {
	out := make(map[string]*userState, len(users))
	for _, u := range users {
		out[u.ID] = u
	}
	return out
}

func indexPositions(users []*userState) map[string]int {
	out := make(map[string]int, len(users))
	for i, u := range users {
		out[u.ID] = i
	}
	return out
}

// openOracleConn dials the connection used to observe gatekeeper replies. Only
// needed when membership churn is on.
func (r *verifyRun) openOracleConn(cfg *config) error {
	if r.vc.MemberChurn <= 0 {
		return nil
	}
	nc, err := connectWithCreds(cfg.NatsURL, "loadgen-verify-oracle", cfg.NatsCredsFile)
	if err != nil {
		return fmt.Errorf("connect verify oracle: %w", err)
	}
	r.oracleNC = nc
	return nil
}

func (r *verifyRun) closeOracleConn() {
	if r.oracleNC != nil {
		_ = r.oracleNC.Drain()
	}
}

// recordOracleErr keeps the first error from a membership-supporting query.
// Any such failure forces INCONCLUSIVE: an unreachable service says nothing
// about whether the membership write happened (spec §10).
func (r *verifyRun) recordOracleErr(err error) {
	r.mu.Lock()
	if r.oracleErr == nil {
		r.oracleErr = err
	}
	r.mu.Unlock()
}

func (r *verifyRun) takeOracleErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.oracleErr
}

// probeSendRate returns the sends/sec verify itself injects into probe rooms.
// It is derived from --min-probes and --probe-rate rather than hard-coded, so
// the default flag set reliably clears the probe floor inside --steady. These
// sends are ordinary traffic on the ordinary path; only the accounting is
// sampled (spec §5). The daily emitters keep running alongside and supply the
// realistic mixed background workload.
func probeSendRate(vc *verifyConfig) float64 {
	if vc.ProbeRate <= 0 || vc.Steady <= 0 {
		return 1
	}
	rate := (float64(vc.MinProbes) * verifyProbeHeadroom / vc.ProbeRate) / vc.Steady.Seconds()
	if rate < 1 {
		return 1
	}
	if rate > verifyMaxProbeSendRate {
		return verifyMaxProbeSendRate
	}
	return rate
}

// emitProbes walks the probe rooms round-robin, sending as one of the room's
// own fixture members, and tracks the deterministic --probe-rate fraction.
// Exits on ctx.Done.
func (r *verifyRun) emitProbes(ctx context.Context) {
	rooms := r.prs.RoomIDs()
	if len(rooms) == 0 {
		return
	}
	interval := time.Duration(float64(time.Second) / probeSendRate(r.vc))
	if interval <= 0 {
		interval = time.Millisecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()

	rnd := rand.New(rand.NewSource(r.vc.Seed ^ 0x50B0B0B0))
	seq := make(map[string]uint64, len(r.prs.Members))
	next := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		roomID := rooms[next%len(rooms)]
		next++
		sender := r.pickSender(roomID, rnd)
		if sender == nil {
			continue
		}
		seq[sender.ID]++
		r.emitOneProbe(ctx, sender, roomID, seq[sender.ID])
	}
}

// pickSender chooses a fixture member of roomID. Senders are drawn from the
// room's original membership, never from the reserve: a floater removed by
// churn would get Forbidden on readback (its access window no longer covers
// the message) and turn a healthy run INCONCLUSIVE (spec §8).
func (r *verifyRun) pickSender(roomID string, rnd *rand.Rand) *userState {
	members := r.prs.MembersOf(roomID)
	if len(members) == 0 {
		return nil
	}
	return r.byID[members[rnd.Intn(len(members))]]
}

func (r *verifyRun) emitOneProbe(ctx context.Context, u *userState, roomID string, seqNo uint64) {
	now := time.Now()
	// Gate on Has: MembershipModel.Epoch returns 0 for a room it never
	// registered, which is indistinguishable from a genuine never-churned
	// room and would hand the tracker a nil expected set (scored total_loss).
	track := r.prs.Has(roomID) && shouldProbe(r.vc.Seed, r.idxByID[u.ID], seqNo, r.vc.ProbeRate)
	if track && r.mm.InSettle(roomID, now) {
		// Inside a settle window either delivery outcome is legitimate, so the
		// send still goes out as load but is never adjudicated. Counting it
		// keeps a churn-starved run honest: suppressed probes do not count
		// toward --min-probes, so the run reports INCONCLUSIVE (spec §9.2).
		r.tracker.RecordSuppressed()
		track = false
	}

	msgID, err := r.publishSend(ctx, u.Account, roomID)
	if err != nil {
		slog.Warn("probe send failed", "user", u.ID, "room", roomID, "err", err)
		return
	}
	if !track {
		return
	}
	// Registered after the publish returns, not before: a failed publish would
	// otherwise leave a registered probe nothing could ever deliver, i.e. a
	// manufactured total_loss. The reverse race is not reachable — a delivery
	// has to traverse MESSAGES → gatekeeper → canonical → broadcast-worker →
	// back, which no map insert loses to.
	epoch := r.mm.Epoch(roomID)
	r.tracker.RegisterProbe(msgID, roomID, epoch, r.mm.MembersAtEpoch(roomID, epoch))
	r.mu.Lock()
	r.targets = append(r.targets, ReadbackTarget{MsgID: msgID, RoomID: roomID, SenderID: u.ID})
	r.mu.Unlock()
}

// publishSend issues one ordinary send on the frontdoor subject and returns the
// message ID it minted.
func (r *verifyRun) publishSend(ctx context.Context, account, roomID string) (string, error) {
	msgID := idgen.GenerateMessageID()
	reqID := idgen.GenerateRequestID()
	data, err := json.Marshal(model.SendMessageRequest{ID: msgID, Content: verifyProbeContent, RequestID: reqID})
	if err != nil {
		return "", fmt.Errorf("marshal verify send: %w", err)
	}
	if r.env.collector != nil {
		r.env.collector.RecordPublish(reqID, msgID, time.Now())
	}
	if err := r.env.publish(ctx, subject.MsgSend(account, roomID, r.siteID), data); err != nil {
		if r.env.collector != nil {
			r.env.collector.RecordPublishFailed(reqID, msgID)
		}
		return "", fmt.Errorf("publish verify send: %w", err)
	}
	return msgID, nil
}

// pendingChange is a membership change awaiting its post-settle observation.
type pendingChange struct {
	kind    changeKind
	roomID  string
	userID  string
	account string
	epoch   int
	due     time.Time
}

// churnRooms returns the probe rooms membership changes may target. DMs are
// excluded: room-service rejects member add/remove on a non-channel room, so
// churning one would only ever produce harness errors.
func (r *verifyRun) churnRooms() []string {
	var out []string
	for _, id := range r.prs.RoomIDs() {
		if bandOf(id) != bandDM {
			out = append(out, id)
		}
	}
	return out
}

// churnTailroom returns how long before the steady window closes driveChurn
// must stop issuing changes.
//
// It has to scale with --settle. A change is only ever observed if its due time
// (issued + Settle) falls inside the steady window, so a fixed tailroom silently
// discards every change issued after Steady-Settle: at --steady=120s
// --settle=30s that is ~18% of them. Those changes still increment
// ChangeCounts.Total but never Applied or Effective, so the report reads
// "22 changes / 18 applied / 18 effective" with no violation and verdict PASS —
// visually identical to a real membership_not_applied symptom, but silent. A
// correctness tool reporting a false clean is its worst failure mode.
func churnTailroom(settle time.Duration) time.Duration {
	if settle < 0 {
		settle = 0
	}
	if tr := settle + verifyChurnObservation; tr > verifyChurnTailroom {
		return tr
	}
	return verifyChurnTailroom
}

// churnInterval converts --member-churn (changes per room per minute) into the
// gap between successive changes across all churnable rooms.
func churnInterval(perRoomPerMinute float64, rooms int) time.Duration {
	if perRoomPerMinute <= 0 || rooms <= 0 {
		return 0
	}
	return time.Duration(float64(time.Minute) / (perRoomPerMinute * float64(rooms)))
}

// driveChurn issues membership changes until issueUntil, and harvests each
// change's post-settle observations. Exits on ctx.Done.
func (r *verifyRun) driveChurn(ctx context.Context, issueUntil time.Time) {
	rooms := r.churnRooms()
	interval := churnInterval(r.vc.MemberChurn, len(rooms))
	if interval <= 0 {
		return
	}
	rnd := rand.New(rand.NewSource(r.vc.Seed ^ 0x0C0FFEE0))
	tick := time.NewTicker(verifyChurnPoll)
	defer tick.Stop()

	var pending []pendingChange
	nextIssue := time.Now().Add(interval)
	roomIdx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := time.Now()
		if now.After(nextIssue) && now.Before(issueUntil) {
			nextIssue = now.Add(interval)
			pc, ok, err := r.applyChange(ctx, rooms[roomIdx%len(rooms)], rnd)
			roomIdx++
			if err != nil {
				// The pool and the system have diverged; every later
				// observation in this run is suspect.
				r.recordOracleErr(err)
				return
			}
			if ok {
				slog.Info("membership change applied", "kind", changeName(pc.kind),
					"room", pc.roomID, "user", pc.userID, "epoch", pc.epoch)
				pending = append(pending, pc)
			}
		}
		pending = r.harvest(ctx, pending, now)
	}
}

// applyChange issues one add or remove against roomID. The bool reports whether
// a change was actually applied; a non-nil error is fatal to churn.
func (r *verifyRun) applyChange(ctx context.Context, roomID string, rnd *rand.Rand) (pendingChange, bool, error) {
	if !r.prs.Has(roomID) {
		return pendingChange{}, false, nil
	}
	current := make(map[string]struct{})
	for _, id := range r.mm.Members(roomID) {
		current[id] = struct{}{}
	}

	var joined []string
	for _, id := range r.reserve {
		if _, ok := current[id]; ok {
			joined = append(joined, id)
		}
	}
	// Prefer an add when no floater is in the room yet. Otherwise coin-flip, so
	// both directions get exercised. Targets are always drawn against the
	// current model, never blindly: an add of a user already in the room is a
	// no-op server-side but still bumps the epoch and opens a full settle
	// window, silently suppressing probes there for nothing.
	if len(joined) > 0 && rnd.Intn(2) == 0 {
		return r.removeMember(ctx, roomID, joined[rnd.Intn(len(joined))])
	}
	target := r.pickJoinTarget(current, rnd)
	if target == "" {
		return pendingChange{}, false, nil
	}
	return r.addMember(ctx, roomID, target)
}

// pickJoinTarget returns a reserve floater that is not currently in the room.
func (r *verifyRun) pickJoinTarget(current map[string]struct{}, rnd *rand.Rand) string {
	if len(r.reserve) == 0 {
		return ""
	}
	start := rnd.Intn(len(r.reserve))
	for i := range r.reserve {
		id := r.reserve[(start+i)%len(r.reserve)]
		if _, ok := current[id]; !ok {
			return id
		}
	}
	return ""
}

func (r *verifyRun) addMember(ctx context.Context, roomID, targetID string) (pendingChange, bool, error) {
	target := r.byID[targetID]
	requester := r.roomRequester(roomID)
	if target == nil || requester == nil {
		return pendingChange{}, false, nil
	}
	body, err := json.Marshal(model.AddMembersRequest{RoomID: roomID, Users: []string{target.Account}})
	if err != nil {
		return pendingChange{}, false, fmt.Errorf("marshal member add: %w", err)
	}
	if err := r.requestAccepted(ctx, subject.MemberAdd(requester.Account, roomID, r.siteID), body); err != nil {
		slog.Warn("member add rejected", "room", roomID, "target", targetID, "err", err)
		return pendingChange{}, false, nil
	}
	// A real client subscribes on joining. A half-subscribed user silently
	// misses every later broadcast, which would register as missing_recipient
	// against a system that did nothing wrong — so this failure is fatal, not
	// logged-and-continued.
	if err := r.env.direct.SubscribeRoom(targetID, roomID); err != nil {
		return pendingChange{}, false, fmt.Errorf(
			"membership churn aborted, added user is unobservable: subscribe %s to %s: %w",
			targetID, roomID, err)
	}
	now := time.Now()
	r.mm.ApplyAdd(roomID, targetID, now)
	return r.pendingFor(changeAdd, roomID, target, now), true, nil
}

func (r *verifyRun) removeMember(ctx context.Context, roomID, targetID string) (pendingChange, bool, error) {
	target := r.byID[targetID]
	if target == nil {
		return pendingChange{}, false, nil
	}
	// Self-removal: room-service only demands the owner role when the
	// requester and the target differ.
	body, err := json.Marshal(model.RemoveMemberRequest{RoomID: roomID, Account: target.Account})
	if err != nil {
		return pendingChange{}, false, fmt.Errorf("marshal member remove: %w", err)
	}
	if err := r.requestAccepted(ctx, subject.MemberRemove(target.Account, roomID, r.siteID), body); err != nil {
		slog.Warn("member remove rejected", "room", roomID, "target", targetID, "err", err)
		return pendingChange{}, false, nil
	}
	// The room subscription is deliberately left in place. Room-lane delivery
	// follows whoever subscribed, so an unsubscribe would test loadgen's own
	// bookkeeping; §7.3 checks a removed member via authorization instead.
	now := time.Now()
	r.mm.ApplyRemove(roomID, targetID, now)
	return r.pendingFor(changeRemove, roomID, target, now), true, nil
}

func (r *verifyRun) pendingFor(kind changeKind, roomID string, target *userState, now time.Time) pendingChange {
	return pendingChange{
		kind: kind, roomID: roomID, userID: target.ID, account: target.Account,
		epoch: r.mm.Epoch(roomID), due: now.Add(r.vc.Settle),
	}
}

// roomRequester returns a fixture member of the room to issue the add as. It is
// always an original member, so it is never the target of churn itself.
func (r *verifyRun) roomRequester(roomID string) *userState {
	members := r.prs.MembersOf(roomID)
	if len(members) == 0 {
		return nil
	}
	return r.byID[members[0]]
}

// requestAccepted issues a request/reply and reports the errcode envelope as an
// error. room-service replies with an ordinary message body on error, which the
// requestFn returns with a nil Go error.
func (r *verifyRun) requestAccepted(ctx context.Context, subj string, body []byte) error {
	raw, err := r.env.request(ctx, subj, body, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	if envErr, ok := errcode.Parse(raw); ok {
		return fmt.Errorf("rejected: %w", envErr)
	}
	return nil
}

// harvest observes every change whose settle window has elapsed and returns
// those still waiting.
func (r *verifyRun) harvest(ctx context.Context, pending []pendingChange, now time.Time) []pendingChange {
	keep := pending[:0]
	for _, pc := range pending {
		if now.Before(pc.due) {
			keep = append(keep, pc)
			continue
		}
		r.observe(ctx, &pc)
	}
	return keep
}

// observe runs both membership oracles for one settled change: what
// subscription.list reports, and whether the target's own send is authorized.
func (r *verifyRun) observe(ctx context.Context, pc *pendingChange) {
	has, err := r.oracleHasRoom(ctx, pc.account, pc.roomID)
	if err != nil {
		r.recordOracleErr(err)
	} else {
		var observed []string
		if has {
			observed = []string{pc.userID}
		}
		r.mm.RecordOracle(pc.roomID, observed, pc.epoch)
	}

	accepted, seen := r.sendAsTarget(ctx, pc)
	if seen {
		r.mm.RecordSendResult(pc.roomID, pc.userID, accepted, pc.epoch)
	}
}

// verifySubscriptionListRequest / verifySubscriptionRow mirror user-service's
// subscription.list wire shapes. Local copies, like the history request in
// verify_readback.go: the service's models live under an internal package.
type verifySubscriptionListRequest struct {
	Type               string `json:"type"`
	Offset             int    `json:"offset,omitempty"`
	Limit              int    `json:"limit,omitempty"`
	IncludeLastMessage *bool  `json:"includeLastMessage,omitempty"`
}

type verifySubscriptionRow struct {
	RoomID string `json:"roomId"`
}

type verifySubscriptionListResponse struct {
	Subscriptions []verifySubscriptionRow `json:"subscriptions"`
	HasMore       bool                    `json:"hasMore"`
}

// oracleHasRoom reports whether the system lists roomID among account's
// subscriptions. Paged, because a daily fixture user carries more rooms than
// one page holds and the churned room could sit on any of them.
func (r *verifyRun) oracleHasRoom(ctx context.Context, account, roomID string) (bool, error) {
	includeLastMessage := false
	offset := 0
	for page := 0; page < verifyOracleMaxPages; page++ {
		body, err := json.Marshal(verifySubscriptionListRequest{
			Type: "rooms", Offset: offset, Limit: verifyOraclePageSize,
			IncludeLastMessage: &includeLastMessage,
		})
		if err != nil {
			return false, fmt.Errorf("marshal subscription list: %w", err)
		}
		raw, err := r.env.request(ctx, subject.UserSubscriptionList(account, r.siteID), body, defaultRequestTimeout)
		if err != nil {
			return false, fmt.Errorf("subscription list for %s: %w", account, err)
		}
		if envErr, ok := errcode.Parse(raw); ok {
			return false, fmt.Errorf("subscription list for %s: %w", account, envErr)
		}
		var resp verifySubscriptionListResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			return false, fmt.Errorf("decode subscription list for %s: %w", account, err)
		}
		for _, s := range resp.Subscriptions {
			if s.RoomID == roomID {
				return true, nil
			}
		}
		if !resp.HasMore || len(resp.Subscriptions) == 0 {
			return false, nil
		}
		offset += len(resp.Subscriptions)
	}
	return false, nil
}

// sendAsTarget attempts one send into the room as the changed user and reports
// whether the gatekeeper accepted it. The second result is false when no reply
// was observed at all — that is not evidence either way, so nothing is
// recorded (spec §9.1).
func (r *verifyRun) sendAsTarget(ctx context.Context, pc *pendingChange) (accepted, seen bool) {
	if r.oracleNC == nil {
		return false, false
	}
	reqID := idgen.GenerateRequestID()
	sub, err := r.oracleNC.SubscribeSync(subject.UserResponse(pc.account, reqID))
	if err != nil {
		slog.Warn("subscribe gatekeeper reply failed", "room", pc.roomID, "err", err)
		return false, false
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := r.oracleNC.Flush(); err != nil {
		slog.Warn("flush gatekeeper reply subscription failed", "room", pc.roomID, "err", err)
		return false, false
	}

	data, err := json.Marshal(model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: verifyProbeContent, RequestID: reqID,
	})
	if err != nil {
		slog.Warn("marshal membership probe send failed", "room", pc.roomID, "err", err)
		return false, false
	}
	if err := r.env.publish(ctx, subject.MsgSend(pc.account, pc.roomID, r.siteID), data); err != nil {
		slog.Warn("membership probe send failed", "room", pc.roomID, "err", err)
		return false, false
	}

	waitCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
	defer cancel()
	msg, err := sub.NextMsgWithContext(waitCtx)
	if err != nil {
		return false, false
	}
	if _, isErr := errcode.Parse(msg.Data); isErr {
		return false, true
	}
	return true, true
}

// readback confirms every tracked probe persisted. Queries run as the probe's
// OWN sender account: GetMessagesByIDs filters on the caller's access window,
// so querying as another member reports a phantom persistence_miss for a
// message that persisted fine (spec §8).
func (r *verifyRun) readback(ctx context.Context) ([]Violation, error) {
	r.mu.Lock()
	targets := append([]ReadbackTarget(nil), r.targets...)
	r.mu.Unlock()
	if len(targets) == 0 {
		return nil, nil
	}

	byAccount := map[string][]ReadbackTarget{}
	for _, t := range targets {
		u, ok := r.byID[t.SenderID]
		if !ok {
			continue
		}
		byAccount[u.Account] = append(byAccount[u.Account], t)
	}
	accounts := make([]string, 0, len(byAccount))
	for a := range byAccount {
		accounts = append(accounts, a)
	}
	sort.Strings(accounts)

	rb := NewReadback(r.env.request, r.siteID, verifyReadbackAttempts, verifyReadbackBackoff)
	var out []Violation
	for _, account := range accounts {
		vs, err := rb.Verify(ctx, account, byAccount[account])
		if err != nil {
			return out, fmt.Errorf("readback as %s: %w", account, err)
		}
		out = append(out, vs...)
	}
	return out, nil
}
