package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type OpAction string

const (
	OpVerify       OpAction = "verify"
	OpVerifyAbsent OpAction = "verify-absent"
	OpSkip         OpAction = "skip"
)

type Mapping struct {
	Sources []SourceMapping `json:"sources"`
}

type SourceMapping struct {
	Collection string               `json:"collection"`
	Ops        map[string]OpAction  `json:"ops"`
	Resolvers  map[string]Resolver  `json:"resolvers,omitempty"`
	Targets    map[string]Target    `json:"targets"`
	Fields     map[string][]DestRef `json:"fields,omitempty"`
	Derived    []Derived            `json:"derived,omitempty"`
}

// Action returns the configured action for op; unlisted ops are skipped.
func (s *SourceMapping) Action(op string) OpAction {
	if a, ok := s.Ops[op]; ok {
		return a
	}
	return OpSkip
}

type Resolver struct {
	DB         string             `json:"db"`
	Collection string             `json:"collection"`
	Key        map[string]KeyFrom `json:"key"`
	Fields     []string           `json:"fields"`
}

type Target struct {
	Kind       string             `json:"kind"`
	Collection string             `json:"collection,omitempty"`
	Table      string             `json:"table,omitempty"`
	Key        map[string]KeyFrom `json:"key"`
	Mode       string             `json:"mode,omitempty"`
	Ignore     []string           `json:"ignore,omitempty"`
}

// KeyFrom is one dest-key/resolver-key entry: source path(s), optionally
// through a named transform. JSON forms: "path" | {"from": "p"|["p"...], "transform": "t"}.
type KeyFrom struct {
	From      []string
	Transform string
}

func (k *KeyFrom) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		k.From = []string{s}
		return nil
	}
	var obj struct {
		From      json.RawMessage `json:"from"`
		Transform string          `json:"transform"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("key entry must be a string or object: %w", err)
	}
	k.Transform = obj.Transform
	var one string
	if err := json.Unmarshal(obj.From, &one); err == nil {
		k.From = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(obj.From, &many); err != nil {
		return fmt.Errorf(`key "from" must be a string or string array: %w`, err)
	}
	k.From = many
	return nil
}

// DestRef is one fan-out destination for a source field.
// JSON forms: "alias.field" | {"dest": "...", "transform": "...", "required": true}.
type DestRef struct {
	Dest      string
	Transform string
	Required  bool
}

func (d *DestRef) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		d.Dest = s
		return nil
	}
	var obj struct {
		Dest      string `json:"dest"`
		Transform string `json:"transform"`
		Required  bool   `json:"required"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("dest ref must be a string or object: %w", err)
	}
	d.Dest, d.Transform, d.Required = obj.Dest, obj.Transform, obj.Required
	return nil
}

// Split separates the target alias (first dot-segment) from the dest field path.
func (d DestRef) Split() (alias, field string) {
	alias, field, _ = strings.Cut(d.Dest, ".")
	return alias, field
}

type Derived struct {
	From      []string `json:"from"`
	Transform string   `json:"transform"`
	Dest      []string `json:"dest"`
}

func loadMapping(path string) (*Mapping, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mapping file: %w", err)
	}
	var m Mapping
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse mapping file %s: %w", path, err)
	}
	if err := validateMapping(&m); err != nil {
		return nil, fmt.Errorf("validate mapping file %s: %w", path, err)
	}
	return &m, nil
}

// validateMapping is a temporary stub for Task 3. Task 4 will move this to
// mapping_validate.go and implement the actual validation logic.
func validateMapping(*Mapping) error {
	return nil
}
