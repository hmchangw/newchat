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

type verifierConfig struct {
	Poll          time.Duration
	Timeout       time.Duration
	MaxChecks     int
	SamplePercent int
}

// compiledSource is the mapping pre-folded for the hot path: per-target field
// pairs, lookup projections, and resolver dependencies.
type compiledSource struct {
	src      *SourceMapping
	aliases  []string               // target aliases, sorted — fixes sub-check order
	pairs    map[string][]fieldPair // alias -> compared fields (nil for verbatim targets)
	destCols map[string][]string    // alias -> columns/fields to fetch (nil => whole doc)
	deps     map[string][]string    // alias -> resolver aliases its key/fields reference
}

// checkHandle is the per-check control block in the pending index; superseded
// distinguishes "newer event took over" from "shutting down" on cancellation.
type checkHandle struct {
	cancel     context.CancelFunc
	superseded atomic.Bool
}

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

// compile folds Fields and Derived into per-target field pairs and projections.
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

// targetDeps lists the resolver aliases a target references through "@alias.path".
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

// Submit classifies the event synchronously and spawns a check goroutine for
// anything not skipped; the goroutine acquires the concurrency semaphore itself.
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
	h := &checkHandle{cancel: cancel}
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

// beginCheck installs h as key's active check, returning the displaced predecessor; false once shutting down.
// Held under the Shutdown mutex so wg.Add precedes wg.Wait and supersede order matches Submit order.
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

// unregisterPending drops h unless a newer check has already replaced it.
func (v *verifier) unregisterPending(key string, h *checkHandle) {
	v.mu.Lock()
	if v.pending[key] == h {
		delete(v.pending, key)
	}
	v.mu.Unlock()
}

// targetState is a sub-check's live state, touched only by the owning goroutine.
type targetState struct {
	alias   string
	matched bool
	cause   string
	diffs   []FieldDiff
}

// runCheck owns one row from PENDING to a terminal state; its goroutine is the
// row's sole writer and hands copies to the store.
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
		failNow := v.attempt(ctx, cs, action, row.DocID, states)

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

// attempt runs one pass over the unfrozen sub-checks, reporting whether the
// whole check must fail now (an ambiguous dest key never resolves by polling).
func (v *verifier) attempt(ctx context.Context, cs *compiledSource, action OpAction,
	docID string, states []targetState,
) bool {
	srcDoc, cause := v.loadSource(ctx, cs, action, docID)
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
	docs := attemptDocs{raw: srcDoc, view: sourceView(srcDoc, resolved)}

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
		k, err := buildKey(&t, docs.view, v.reg)
		if err != nil {
			st.cause, st.diffs = "key-unresolvable", nil
			continue
		}
		rec, err := v.lookupDest(ctx, &t, k, cs.destCols[st.alias])
		if v.applyLookup(st, &t, cs.pairs[st.alias], action, docs, rec, err) {
			return true
		}
	}
	return false
}

// attemptDocs: raw is the source doc as stored; view overlays resolved docs under "@alias" keys.
// Verbatim compares must use raw — overlay keys would diff against a dest that can never contain them.
type attemptDocs struct {
	raw  map[string]any
	view map[string]any
}

// applyLookup folds one dest lookup into the sub-check state, reporting whether
// the whole check must fail now.
func (v *verifier) applyLookup(st *targetState, t *Target, pairs []fieldPair, action OpAction,
	docs attemptDocs, rec map[string]any, err error,
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
		diffs = diffVerbatim(docs.raw, rec, t.Ignore) // raw: the overlay is not part of the document
	} else {
		diffs = diffFields(docs.view, rec, pairs, v.reg)
	}
	if len(diffs) == 0 {
		st.matched, st.cause = true, ""
		return false
	}
	st.cause, st.diffs = "mismatch", diffs
	return false
}

// loadSource returns the source doc, or a cause to stamp on every unfrozen
// sub-check. verify-absent knows only the change-stream document key.
func (v *verifier) loadSource(ctx context.Context, cs *compiledSource, action OpAction, docID string) (map[string]any, string) {
	if action == OpVerifyAbsent {
		return map[string]any{"_id": docID}, ""
	}
	doc, err := v.source.FindByID(ctx, cs.src.Collection, docID)
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

// resolveAll runs the point lookups the unfrozen targets need, once per attempt.
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

// sourceView overlays resolved docs under their "@alias" keys so one getPath
// walk serves plain paths ("u._id") and resolver paths ("@user.username") alike.
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

// buildKeyFrom evaluates every key column; a missing path or failing transform
// makes the whole key unresolvable.
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

// snapshotTargets renders sub-check state; diffs attach only to the terminal
// failure row — intermediate rows carry causes alone.
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

// finishCancelled: a superseded check records that outcome; a shutdown one
// keeps its last observed state.
func (v *verifier) finishCancelled(h *checkHandle, row *CheckResult) {
	if !h.superseded.Load() {
		return
	}
	row.State = StateSuperseded
	row.EndedAtMs = v.now().UTC().UnixMilli()
	v.results.Upsert(*row)
}

// Shutdown cancels every in-flight check and waits for them, bounded by ctx.
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

// InspectTarget is one destination's live view in an on-demand inspection.
// KeyFields/CompareFields let the UI highlight what the mapping touches.
type InspectTarget struct {
	Alias         string         `json:"alias"`
	Kind          string         `json:"kind"`
	Dest          string         `json:"dest"`
	Key           map[string]any `json:"key,omitempty"`
	Doc           map[string]any `json:"doc,omitempty"`
	Error         string         `json:"error,omitempty"`
	KeyFields     []string       `json:"keyFields,omitempty"`
	CompareFields []string       `json:"compareFields,omitempty"`
	Verbatim      bool           `json:"verbatim,omitempty"`
}

// InspectResult is the live source + destination state for one document, read
// at request time; SourceFields lists every plain source path the mapping reads.
type InspectResult struct {
	Collection   string          `json:"collection"`
	DocID        string          `json:"docId"`
	Source       map[string]any  `json:"source,omitempty"`
	SourceErr    string          `json:"sourceError,omitempty"`
	SourceFields []string        `json:"sourceFields,omitempty"`
	Targets      []InspectTarget `json:"targets"`
}

// errUnmapped marks an inspect request for a collection the mapping doesn't cover.
var errUnmapped = errors.New("unmapped collection")

// Inspect fetches the current source doc and every mapped destination record, live.
// A missing source falls back to {"_id": docID} so _id-keyed targets survive a delete.
func (v *verifier) Inspect(ctx context.Context, collection, docID string) (InspectResult, error) {
	cs, ok := v.compiled[collection]
	if !ok {
		return InspectResult{}, fmt.Errorf("%w: %q", errUnmapped, collection)
	}
	res := InspectResult{Collection: collection, DocID: docID, SourceFields: mappedSourceFields(cs)}

	srcDoc, err := v.source.FindByID(ctx, collection, docID)
	switch {
	case errors.Is(err, errNotFound):
		res.SourceErr = "not-found"
		srcDoc = map[string]any{"_id": docID}
	case err != nil:
		res.SourceErr = "lookup-error: " + err.Error()
		srcDoc = map[string]any{"_id": docID}
	default:
		res.Source = srcDoc
	}

	states := make([]targetState, len(cs.aliases))
	for i, alias := range cs.aliases {
		states[i] = targetState{alias: alias}
	}
	resolved, resolveCauses := v.resolveAll(ctx, cs, srcDoc, states)
	view := sourceView(srcDoc, resolved)

	for _, alias := range cs.aliases {
		t := cs.src.Targets[alias]
		it := InspectTarget{
			Alias:         alias,
			Kind:          t.Kind,
			KeyFields:     sortedMapKeys(t.Key),
			CompareFields: comparedDestFields(cs.pairs[alias]),
			Verbatim:      t.Mode == "verbatim",
		}
		if t.Kind == "cassandra" {
			it.Dest = t.Table
		} else {
			it.Dest = t.Collection
		}
		if c := firstResolverCause(cs.deps[alias], resolveCauses); c != "" {
			it.Error = c
			res.Targets = append(res.Targets, it)
			continue
		}
		key, err := buildKey(&t, view, v.reg)
		if err != nil {
			it.Error = "key-unresolvable: " + err.Error()
			res.Targets = append(res.Targets, it)
			continue
		}
		it.Key = key
		// nil columns/fields => whole doc / SELECT * — the inspect view wants everything
		var doc map[string]any
		if t.Kind == "cassandra" {
			doc, err = v.cass.SelectOne(ctx, t.Table, key, nil)
		} else {
			doc, err = v.target.FindOne(ctx, t.Collection, key, nil)
		}
		switch {
		case errors.Is(err, errNotFound):
			it.Error = "not-found"
		case errors.Is(err, errAmbiguous):
			it.Error = "ambiguous-key"
		case err != nil:
			it.Error = "lookup-error: " + err.Error()
		default:
			it.Doc = doc
		}
		res.Targets = append(res.Targets, it)
	}
	return res, nil
}

// mappedSourceFields lists every plain source path the mapping reads (compared,
// derived, dest keys, resolver keys) — what the UI highlights on the source doc.
func mappedSourceFields(cs *compiledSource) []string {
	set := map[string]bool{}
	add := func(paths []string) {
		for _, p := range paths {
			if !strings.HasPrefix(p, "@") {
				set[p] = true
			}
		}
	}
	for _, alias := range cs.aliases {
		for i := range cs.pairs[alias] {
			add(cs.pairs[alias][i].SourcePaths)
		}
		for _, k := range cs.src.Targets[alias].Key {
			add(k.From)
		}
	}
	for _, r := range cs.src.Resolvers {
		for _, k := range r.Key {
			add(k.From)
		}
	}
	return sortedMapKeys(set)
}

// comparedDestFields lists the dest field paths a target's pairs compare.
func comparedDestFields(pairs []fieldPair) []string {
	set := map[string]bool{}
	for i := range pairs {
		set[pairs[i].DestField] = true
	}
	return sortedMapKeys(set)
}
