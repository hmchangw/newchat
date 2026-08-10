package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

//nolint:unused // wired into main.go's dependency graph by a later task
type verifierConfig struct {
	Poll          time.Duration
	Timeout       time.Duration
	MaxChecks     int
	SamplePercent int
}

// compiledSource is the mapping pre-folded for the hot path: field fan-out
// grouped per target, the dest column list each lookup projects, and which
// resolvers each target depends on.
type compiledSource struct {
	src      *SourceMapping
	aliases  []string               // target aliases, sorted — fixes sub-check order
	pairs    map[string][]fieldPair // alias -> compared fields (nil for verbatim targets)
	destCols map[string][]string    // alias -> columns/fields to fetch (nil => whole doc)
	deps     map[string][]string    // alias -> resolver aliases its key/fields reference
}

// checkHandle is the per-check control block held by the pending index. The
// superseded flag distinguishes "a newer event took over this key" from
// "the verifier is shutting down" when the check's context is cancelled.
type checkHandle struct {
	id         string
	cancel     context.CancelFunc
	superseded atomic.Bool
}

//nolint:unused // wired into main.go's dependency graph by a later task
type verifier struct {
	compiled map[string]*compiledSource // by source collection
	source   SourceStore
	target   TargetStore
	cass     CassStore
	reg      transformRegistry
	results  *resultsStore
	cfg      verifierConfig

	sem chan struct{}
	wg  sync.WaitGroup

	mu      sync.Mutex
	closed  bool                    // set by Shutdown; stops new checks being enrolled
	pending map[string]*checkHandle // collection+"\x00"+docID -> active check

	baseCtx  context.Context //nolint:containedctx // the verifier owns the lifetime of every in-flight check
	baseStop context.CancelFunc

	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) bool // false => ctx done
	sampleFn func() int
}

//nolint:unused // wired into main.go's dependency graph by a later task
func newVerifier(m *Mapping, src SourceStore, tgt TargetStore, cass CassStore,
	reg transformRegistry, results *resultsStore, cfg verifierConfig,
) *verifier {
	compiled := make(map[string]*compiledSource, len(m.Sources))
	for i := range m.Sources {
		s := &m.Sources[i]
		compiled[s.Collection] = compile(s)
	}
	ctx, stop := context.WithCancel(context.Background())
	return &verifier{
		compiled: compiled,
		source:   src,
		target:   tgt,
		cass:     cass,
		reg:      reg,
		results:  results,
		cfg:      cfg,
		sem:      make(chan struct{}, cfg.MaxChecks),
		pending:  map[string]*checkHandle{},
		baseCtx:  ctx,
		baseStop: stop,
		now:      time.Now,
		sleep:    sleepCtx,
		sampleFn: func() int { return rand.IntN(100) }, //nolint:gosec // sampling decision, not security-sensitive
	}
}

// sleepCtx waits d, reporting false when the context is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// compile folds Fields and Derived into per-target field pairs and precomputes
// the projection each target lookup needs.
func compile(s *SourceMapping) *compiledSource {
	cs := &compiledSource{
		src:      s,
		pairs:    map[string][]fieldPair{},
		destCols: map[string][]string{},
		deps:     map[string][]string{},
	}
	for alias := range s.Targets {
		cs.aliases = append(cs.aliases, alias)
	}
	sort.Strings(cs.aliases)

	for _, path := range sortedMapKeys(s.Fields) {
		for _, ref := range s.Fields[path] {
			alias, field := ref.Split()
			if t, ok := s.Targets[alias]; !ok || t.Mode == "verbatim" {
				continue
			}
			cs.pairs[alias] = append(cs.pairs[alias], fieldPair{
				SourcePaths: []string{path},
				DestField:   field,
				Transform:   ref.Transform,
				Required:    ref.Required,
			})
		}
	}
	for i := range s.Derived {
		d := &s.Derived[i]
		for _, dest := range d.Dest {
			alias, field := DestRef{Dest: dest}.Split()
			if t, ok := s.Targets[alias]; !ok || t.Mode == "verbatim" {
				continue
			}
			cs.pairs[alias] = append(cs.pairs[alias], fieldPair{
				SourcePaths: d.From,
				DestField:   field,
				Transform:   d.Transform,
			})
		}
	}

	for _, alias := range cs.aliases {
		pairs := cs.pairs[alias]
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].DestField < pairs[j].DestField })
		t := s.Targets[alias]
		cs.deps[alias] = targetDeps(&t, pairs)
		if t.Mode == "verbatim" {
			continue // whole-document compare: no projection, no pairs
		}
		cs.destCols[alias] = destColumns(&t, pairs)
	}
	return cs
}

// targetDeps lists the resolver aliases a target's key columns and compared
// fields reference through "@alias.path".
func targetDeps(t *Target, pairs []fieldPair) []string {
	seen := map[string]bool{}
	add := func(path string) {
		if alias, ok := resolverAlias(path); ok {
			seen[alias] = true
		}
	}
	for _, k := range t.Key {
		for _, p := range k.From {
			add(p)
		}
	}
	for i := range pairs {
		for _, p := range pairs[i].SourcePaths {
			add(p)
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for a := range seen {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// destColumns is the sorted union of key columns and compared dest fields.
// Cassandra needs bare column names, Mongo takes the full projection path.
func destColumns(t *Target, pairs []fieldPair) []string {
	seen := map[string]bool{}
	for col := range t.Key {
		seen[col] = true
	}
	for i := range pairs {
		f := pairs[i].DestField
		if t.Kind == "cassandra" {
			f, _, _ = strings.Cut(f, ".")
		}
		seen[f] = true
	}
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func resolverAlias(path string) (string, bool) {
	rest, ok := strings.CutPrefix(path, "@")
	if !ok {
		return "", false
	}
	alias, _, found := strings.Cut(rest, ".")
	if !found || alias == "" {
		return "", false
	}
	return alias, true
}

func sortedMapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Submit classifies the event synchronously and spawns the check goroutine for
// everything it does not skip. It never blocks on the check-concurrency
// semaphore — the spawned goroutine acquires that itself.
//
//nolint:unused // wired into main.go's dependency graph by a later task
func (v *verifier) Submit(ev CDCEvent) {
	nowMs := v.now().UTC().UnixMilli()
	row := CheckResult{
		ID:          idgen.GenerateID(),
		Collection:  ev.Collection,
		Op:          ev.Op,
		DocID:       ev.DocID,
		StartedAtMs: nowMs,
	}

	cs, ok := v.compiled[ev.Collection]
	if !ok {
		v.skip(&row, "unmapped", nowMs)
		return
	}
	action := cs.src.Action(ev.Op)
	if action == OpSkip {
		v.skip(&row, "op-skip", nowMs)
		return
	}
	if v.sampleFn() >= v.cfg.SamplePercent {
		v.skip(&row, "sampled-out", nowMs)
		return
	}

	row.State = StatePending
	row.Targets = make([]TargetResult, len(cs.aliases))
	for i, alias := range cs.aliases {
		row.Targets[i] = TargetResult{Alias: alias}
	}

	ctx, cancel := context.WithCancel(v.baseCtx)
	h := &checkHandle{id: row.ID, cancel: cancel}
	key := pendingKey(ev.Collection, ev.DocID)
	prev, started := v.beginCheck(key, h)
	if !started {
		cancel() // shutting down: the check never starts, so it records nothing
		return
	}
	if prev != nil {
		prev.superseded.Store(true) // must precede cancel: the woken check reads it
		prev.cancel()
	}
	v.results.Upsert(row)

	go func() {
		defer v.wg.Done()
		v.runCheck(ctx, h, key, &row, cs, action)
	}()
}

func (v *verifier) skip(row *CheckResult, reason string, nowMs int64) {
	row.State = StateSkipped
	row.SkipReason = reason
	row.EndedAtMs = nowMs
	v.results.Upsert(*row)
}

func pendingKey(collection, docID string) string { return collection + "\x00" + docID }

// beginCheck installs h as the active check for key and enrols its goroutine in
// the wait group, returning the displaced predecessor for the caller to
// supersede. Registering here rather than inside the goroutine keeps supersede
// ordering identical to Submit ordering; doing the wg.Add under the same lock
// that Shutdown takes keeps it strictly ordered before wg.Wait. Reports false
// once the verifier is shutting down.
func (v *verifier) beginCheck(key string, h *checkHandle) (*checkHandle, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, false
	}
	prev := v.pending[key]
	v.pending[key] = h
	v.wg.Add(1)
	return prev, true
}

// unregisterPending drops h from the pending index unless a newer check has
// already replaced it.
func (v *verifier) unregisterPending(key string, h *checkHandle) {
	v.mu.Lock()
	if v.pending[key] == h {
		delete(v.pending, key)
	}
	v.mu.Unlock()
}

// targetState is a sub-check's live state; only the owning check goroutine
// touches it.
type targetState struct {
	alias   string
	matched bool
	cause   string
	diffs   []FieldDiff
}

// runCheck owns one check row from PENDING to a terminal state. The goroutine
// running it is the row's sole writer and hands copies to the store.
func (v *verifier) runCheck(ctx context.Context, h *checkHandle, key string, row *CheckResult,
	cs *compiledSource, action OpAction,
) {
	defer v.unregisterPending(key, h)
	defer h.cancel() // release the context even on the terminal-state paths

	select {
	case v.sem <- struct{}{}:
	case <-ctx.Done():
		v.finishCancelled(h, row)
		return
	}
	defer func() { <-v.sem }()

	states := make([]targetState, len(cs.aliases))
	for i, alias := range cs.aliases {
		states[i] = targetState{alias: alias}
	}
	deadline := v.now().Add(v.cfg.Timeout)

	for {
		row.Attempts++
		failNow := v.attempt(ctx, cs, action, row.DocID, row.Collection, states)

		switch {
		case allMatched(states):
			v.finish(row, StateMatched, states)
			return
		case failNow, !v.now().Before(deadline):
			v.finish(row, StateFailed, states)
			return
		}

		row.State = StatePending
		row.Targets = snapshotTargets(states, false)
		v.results.Upsert(*row)

		if !v.sleep(ctx, v.cfg.Poll) {
			v.finishCancelled(h, row)
			return
		}
	}
}

// attempt runs one full pass over the unfrozen sub-checks. It reports whether
// the whole check must fail immediately (an ambiguous dest key can never
// resolve itself by polling).
func (v *verifier) attempt(ctx context.Context, cs *compiledSource, action OpAction,
	docID, collection string, states []targetState,
) bool {
	srcDoc, cause := v.loadSource(ctx, action, collection, docID)
	if cause != "" {
		for i := range states {
			if !states[i].matched {
				states[i].cause = cause
				states[i].diffs = nil
			}
		}
		return false
	}

	resolved, resolveCauses := v.resolveAll(ctx, cs, srcDoc, states)
	view := sourceView(srcDoc, resolved)

	for i := range states {
		st := &states[i]
		if st.matched {
			continue
		}
		if c := firstResolverCause(cs.deps[st.alias], resolveCauses); c != "" {
			st.cause, st.diffs = c, nil
			continue
		}
		t := cs.src.Targets[st.alias]
		k, err := buildKey(&t, view, v.reg)
		if err != nil {
			st.cause, st.diffs = "key-unresolvable", nil
			continue
		}
		rec, err := v.lookupDest(ctx, &t, k, cs.destCols[st.alias])
		if v.applyLookup(st, &t, cs.pairs[st.alias], action, view, rec, err) {
			return true
		}
	}
	return false
}

// applyLookup folds one dest lookup outcome into the sub-check state and
// reports whether the whole check must fail now.
func (v *verifier) applyLookup(st *targetState, t *Target, pairs []fieldPair, action OpAction,
	view, rec map[string]any, err error,
) bool {
	st.diffs = nil
	switch {
	case errors.Is(err, errAmbiguous):
		st.cause = "ambiguous-key"
		return true
	case errors.Is(err, errNotFound):
		if action == OpVerifyAbsent {
			st.matched, st.cause = true, ""
			return false
		}
		st.cause = "dest-missing"
		return false
	case err != nil:
		st.cause = "lookup-error: " + err.Error()
		return false
	}
	if action == OpVerifyAbsent {
		st.cause = "still-present"
		return false
	}
	var diffs []FieldDiff
	if t.Mode == "verbatim" {
		diffs = diffVerbatim(view, rec, t.Ignore)
	} else {
		diffs = diffFields(view, rec, pairs, v.reg)
	}
	if len(diffs) == 0 {
		st.matched, st.cause = true, ""
		return false
	}
	st.cause, st.diffs = "mismatch", diffs
	return false
}

// loadSource returns the source document to compare against, or a cause to
// stamp on every unfrozen sub-check. verify-absent has no source document —
// only the change-stream document key is known.
func (v *verifier) loadSource(ctx context.Context, action OpAction, collection, docID string) (map[string]any, string) {
	if action == OpVerifyAbsent {
		return map[string]any{"_id": docID}, ""
	}
	doc, err := v.source.FindByID(ctx, collection, docID)
	switch {
	case errors.Is(err, errNotFound):
		// Deleted between the event and the check; keep polling — the delete
		// event's own check supersedes this one.
		return nil, "source-missing"
	case err != nil:
		return nil, "source-error: " + err.Error()
	}
	return doc, ""
}

// resolveAll performs the point lookups the still-unfrozen targets depend on,
// once per attempt. Returns the resolved docs and a per-resolver failure cause.
func (v *verifier) resolveAll(ctx context.Context, cs *compiledSource, srcDoc map[string]any,
	states []targetState,
) (map[string]map[string]any, map[string]string) {
	needed := map[string]bool{}
	for i := range states {
		if states[i].matched {
			continue
		}
		for _, alias := range cs.deps[states[i].alias] {
			needed[alias] = true
		}
	}
	if len(needed) == 0 {
		return nil, nil
	}
	docs := make(map[string]map[string]any, len(needed))
	causes := map[string]string{}
	for _, alias := range sortedMapKeys(needed) {
		r := cs.src.Resolvers[alias]
		key, err := buildKeyFrom(r.Key, srcDoc, v.reg)
		if err != nil {
			causes[alias] = "resolver-miss: " + alias
			continue
		}
		var doc map[string]any
		if r.DB == "target" {
			doc, err = v.target.FindOne(ctx, r.Collection, key, r.Fields)
		} else {
			doc, err = v.source.FindOne(ctx, r.Collection, key, r.Fields)
		}
		switch {
		case errors.Is(err, errNotFound):
			causes[alias] = "resolver-miss: " + alias
		case err != nil:
			causes[alias] = fmt.Sprintf("resolver-error: %s: %v", alias, err)
		default:
			docs[alias] = doc
		}
	}
	return docs, causes
}

func firstResolverCause(deps []string, causes map[string]string) string {
	for _, alias := range deps {
		if c, ok := causes[alias]; ok {
			return c
		}
	}
	return ""
}

// sourceView overlays resolved documents onto the source document under their
// "@alias" keys, so a single getPath walk serves both plain paths ("u._id")
// and resolver paths ("@user.username" -> view["@user"]["username"]).
func sourceView(srcDoc map[string]any, resolved map[string]map[string]any) map[string]any {
	if len(resolved) == 0 {
		return srcDoc
	}
	view := make(map[string]any, len(srcDoc)+len(resolved))
	for k, val := range srcDoc {
		view[k] = val
	}
	for alias, doc := range resolved {
		view["@"+alias] = doc
	}
	return view
}

func buildKey(t *Target, view map[string]any, reg transformRegistry) (map[string]any, error) {
	return buildKeyFrom(t.Key, view, reg)
}

// buildKeyFrom evaluates every key column against the source view; a missing
// path or a failing transform makes the whole key unresolvable.
func buildKeyFrom(spec map[string]KeyFrom, view map[string]any, reg transformRegistry) (map[string]any, error) {
	key := make(map[string]any, len(spec))
	for _, col := range sortedMapKeys(spec) {
		kf := spec[col]
		args := make([]any, 0, len(kf.From))
		for _, path := range kf.From {
			val, ok := getPath(view, path)
			if !ok {
				return nil, fmt.Errorf("key column %q: path %q has no value", col, path)
			}
			args = append(args, val)
		}
		val, err := reg.apply(kf.Transform, args)
		if err != nil {
			return nil, fmt.Errorf("key column %q: %w", col, err)
		}
		key[col] = val
	}
	return key, nil
}

func (v *verifier) lookupDest(ctx context.Context, t *Target, key map[string]any, cols []string) (map[string]any, error) {
	if t.Kind == "cassandra" {
		if v.cass == nil {
			return nil, errors.New("cassandra target configured but no cassandra store")
		}
		return v.cass.SelectOne(ctx, t.Table, key, cols)
	}
	return v.target.FindOne(ctx, t.Collection, key, cols)
}

func allMatched(states []targetState) bool {
	for i := range states {
		if !states[i].matched {
			return false
		}
	}
	return true
}

// snapshotTargets renders sub-check state for the row. Diffs are attached only
// on the terminal failure row — intermediate rows carry causes alone.
func snapshotTargets(states []targetState, withDiffs bool) []TargetResult {
	out := make([]TargetResult, len(states))
	for i := range states {
		out[i] = TargetResult{Alias: states[i].alias, Matched: states[i].matched, LastCause: states[i].cause}
		if withDiffs && !states[i].matched && len(states[i].diffs) > 0 {
			out[i].Diffs = states[i].diffs
		}
	}
	return out
}

func (v *verifier) finish(row *CheckResult, state CheckState, states []targetState) {
	row.State = state
	row.Targets = snapshotTargets(states, state == StateFailed)
	row.EndedAtMs = v.now().UTC().UnixMilli()
	v.results.Upsert(*row)
}

// finishCancelled handles a cancelled check: a superseded one records that
// outcome, a shutdown one leaves the row in its last observed state.
func (v *verifier) finishCancelled(h *checkHandle, row *CheckResult) {
	if !h.superseded.Load() {
		return
	}
	row.State = StateSuperseded
	row.EndedAtMs = v.now().UTC().UnixMilli()
	v.results.Upsert(*row)
}

// Shutdown cancels every in-flight check and waits for the goroutines, bounded
// by ctx.
//
//nolint:unused // wired into main.go's dependency graph by a later task
func (v *verifier) Shutdown(ctx context.Context) {
	v.mu.Lock()
	v.closed = true
	v.mu.Unlock()
	v.baseStop()
	done := make(chan struct{})
	go func() {
		v.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}
