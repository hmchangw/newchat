package model

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Parses the whole package (constants aren't enumerable at runtime) and asserts
// every MessageType* constant is in systemMessageTypes or exempted in nonSystem
// with a reason — an unregistered one silently changes behaviour in four services.
func TestSystemMessageTypesCoverEveryConstant(t *testing.T) {
	nonSystem := map[string]string{
		"MessageTypeImportant": "client-settable (重要訊息): previews and notifies like a normal message",
	}

	// Whole-directory walk so a MessageType* constant moved or added in any
	// other pkg/model file stays guarded; a single-file parse would silently
	// un-guard it while found>0 keeps the test green.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var found int
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, cname := range vs.Names {
					if !strings.HasPrefix(cname.Name, "MessageType") || i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					value, uerr := strconv.Unquote(lit.Value)
					if uerr != nil {
						t.Errorf("%s: cannot unquote value %s: %v", cname.Name, lit.Value, uerr)
						continue
					}
					found++

					if reason, exempt := nonSystem[cname.Name]; exempt {
						if IsSystemMessageType(value) {
							t.Errorf("%s (%q) is in systemMessageTypes but marked non-system: %s",
								cname.Name, value, reason)
						}
						continue
					}
					if !IsSystemMessageType(value) {
						t.Errorf("%s (%q) is missing from systemMessageTypes.\n"+
							"Add it there, or add %s to this test's nonSystem map with a reason.\n"+
							"Left unregistered it will be indexed as searchable content, trigger push "+
							"notifications, and show up in room history.", cname.Name, value, cname.Name)
					}
				}
			}
		}
	}

	if found == 0 {
		t.Fatal("no MessageType* constants found — the parser stopped matching, so this test guards nothing")
	}
}

func TestIsSystemMessageType(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"room_created", MessageTypeRoomCreated, true},
		{"members_added", MessageTypeMembersAdded, true},
		{"member_removed", MessageTypeMemberRemoved, true},
		{"member_left", MessageTypeMemberLeft, true},
		{"room_renamed", MessageTypeRoomRenamed, true},
		{"room_restricted", MessageTypeRoomRestricted, true},
		{"teams_meet_started", MessageTypeTeamsMeetStarted, true},
		{"empty is normal user message", "", false},
		{"literal message", "message", false},
		{"cassandra tombstone excluded", "message_removed", false},
		{"teams migration marker excluded", "teams_system", false},
		{"unknown", "call_ended", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSystemMessageType(tc.in); got != tc.want {
				t.Fatalf("IsSystemMessageType(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestPreviewMessageOmitempty(t *testing.T) {
	// previewMessage is *PreviewMessage with omitempty on every carrier: serialized when set,
	// omitted when nil (no eligible message / read error / thread path).
	pvw := &PreviewMessage{MessageID: "m1"}
	cases := []struct {
		name  string
		set   any
		unset any
	}{
		{"EditRoomEvent", &EditRoomEvent{PreviewMessage: pvw}, &EditRoomEvent{}},
		{"DeleteRoomEvent", &DeleteRoomEvent{PreviewMessage: pvw}, &DeleteRoomEvent{}},
		{"MessageEvent", &MessageEvent{PreviewMessage: pvw}, &MessageEvent{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.set)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !strings.Contains(string(b), `"previewMessage":{"messageId":"m1"`) {
				t.Fatalf("preview not serialized: %s", b)
			}
			b, _ = json.Marshal(tc.unset)
			if strings.Contains(string(b), "previewMessage") {
				t.Fatalf("nil preview should be omitted: %s", b)
			}
		})
	}
}
