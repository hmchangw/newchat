package main

import (
	"fmt"
	"strings"
)

var validOps = map[string]bool{"insert": true, "update": true, "replace": true, "delete": true}

func validateMapping(m *Mapping) error {
	seen := map[string]bool{}
	for i := range m.Sources {
		src := &m.Sources[i]
		if seen[src.Collection] {
			return fmt.Errorf("duplicate source collection %q", src.Collection)
		}
		seen[src.Collection] = true
		if err := validateSource(src); err != nil {
			return fmt.Errorf("source %q: %w", src.Collection, err)
		}
	}
	return nil
}

func validateSource(src *SourceMapping) error {
	if len(src.Targets) == 0 {
		return fmt.Errorf("no targets declared")
	}
	for op, action := range src.Ops {
		if !validOps[op] {
			return fmt.Errorf("unknown op %q", op)
		}
		if action != OpVerify && action != OpVerifyAbsent && action != OpSkip {
			return fmt.Errorf("op %q: unknown action %q", op, action)
		}
	}
	for alias, r := range src.Resolvers {
		if r.DB != "source" && r.DB != "target" {
			return fmt.Errorf("resolver %q: db must be source or target, got %q", alias, r.DB)
		}
		if r.Collection == "" || len(r.Key) == 0 || len(r.Fields) == 0 {
			return fmt.Errorf("resolver %q: collection, key, and fields are all required", alias)
		}
		for kf, k := range r.Key {
			if err := checkKeyFrom(k, nil); err != nil { // nil resolvers: no chaining allowed
				return fmt.Errorf("resolver %q key %q: resolvers must not chain: %w", alias, kf, err)
			}
		}
	}
	for alias, t := range src.Targets {
		if err := validateTarget(alias, &t, src.Resolvers); err != nil {
			return err
		}
	}
	for path, refs := range src.Fields {
		if err := checkSourcePath(path, src.Resolvers); err != nil {
			return fmt.Errorf("fields %q: %w", path, err)
		}
		for _, ref := range refs {
			if err := checkDestRef(ref, src.Targets); err != nil {
				return fmt.Errorf("fields %q: %w", path, err)
			}
		}
	}
	for i, d := range src.Derived {
		if len(d.From) == 0 || len(d.Dest) == 0 || d.Transform == "" {
			return fmt.Errorf("derived[%d]: from, dest, and transform are all required", i)
		}
		if !knownTransform(d.Transform) {
			return fmt.Errorf("derived[%d]: unknown transform %q", i, d.Transform)
		}
		for _, f := range d.From {
			if err := checkSourcePath(f, src.Resolvers); err != nil {
				return fmt.Errorf("derived[%d]: %w", i, err)
			}
		}
		for _, dest := range d.Dest {
			if err := checkDestRef(DestRef{Dest: dest}, src.Targets); err != nil {
				return fmt.Errorf("derived[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func validateTarget(alias string, t *Target, resolvers map[string]Resolver) error {
	switch t.Kind {
	case "mongo":
		if t.Collection == "" {
			return fmt.Errorf("target %q: mongo target requires collection", alias)
		}
	case "cassandra":
		if t.Table == "" {
			return fmt.Errorf("target %q: cassandra target requires table", alias)
		}
	default:
		return fmt.Errorf("target %q: kind must be mongo or cassandra, got %q", alias, t.Kind)
	}
	if len(t.Key) == 0 {
		return fmt.Errorf("target %q: empty key", alias)
	}
	if t.Mode != "" && t.Mode != "verbatim" {
		return fmt.Errorf("target %q: mode must be empty or verbatim, got %q", alias, t.Mode)
	}
	if t.Mode == "verbatim" && t.Kind == "cassandra" {
		return fmt.Errorf("target %q: verbatim requires a mongo target; whole-row cassandra reads are unsupported", alias)
	}
	for kf, k := range t.Key {
		if err := checkKeyFrom(k, resolvers); err != nil {
			return fmt.Errorf("target %q key %q: %w", alias, kf, err)
		}
	}
	return nil
}

func checkKeyFrom(k KeyFrom, resolvers map[string]Resolver) error {
	if len(k.From) == 0 {
		return fmt.Errorf("empty from")
	}
	if k.Transform != "" && !knownTransform(k.Transform) {
		return fmt.Errorf("unknown transform %q", k.Transform)
	}
	for _, f := range k.From {
		if err := checkSourcePath(f, resolvers); err != nil {
			return err
		}
	}
	return nil
}

// checkSourcePath validates a source path; "@alias.field" must name a declared
// resolver. nil resolvers reject any @-reference — how chaining is forbidden.
func checkSourcePath(path string, resolvers map[string]Resolver) error {
	if !strings.HasPrefix(path, "@") {
		return nil
	}
	alias, _, ok := strings.Cut(strings.TrimPrefix(path, "@"), ".")
	if !ok || alias == "" {
		return fmt.Errorf("malformed resolver reference %q", path)
	}
	if _, declared := resolvers[alias]; !declared {
		return fmt.Errorf("path %q references undeclared resolver %q", path, alias)
	}
	return nil
}

func checkDestRef(ref DestRef, targets map[string]Target) error {
	alias, field := ref.Split()
	if field == "" {
		return fmt.Errorf("dest ref %q must be alias.field", ref.Dest)
	}
	t, ok := targets[alias]
	if !ok {
		return fmt.Errorf("dest ref %q: unknown target %q", ref.Dest, alias)
	}
	if t.Mode == "verbatim" {
		return fmt.Errorf("dest ref %q: target %q is verbatim and takes no field refs", ref.Dest, alias)
	}
	if ref.Transform != "" && !knownTransform(ref.Transform) {
		return fmt.Errorf("dest ref %q: unknown transform %q", ref.Dest, ref.Transform)
	}
	return nil
}

// Source returns the mapping entry for a raw source collection.
func (m *Mapping) Source(collection string) (*SourceMapping, bool) {
	for i := range m.Sources {
		if m.Sources[i].Collection == collection {
			return &m.Sources[i], true
		}
	}
	return nil, false
}

// NeedsCassandra reports whether any target is a cassandra table — gates the connect in main.
func (m *Mapping) NeedsCassandra() bool {
	for _, s := range m.Sources {
		for _, t := range s.Targets {
			if t.Kind == "cassandra" {
				return true
			}
		}
	}
	return false
}
