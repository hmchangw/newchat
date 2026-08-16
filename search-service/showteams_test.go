package main

import "testing"

func TestEffectiveShowTeams(t *testing.T) {
	tests := []struct {
		name    string
		cfg     handlerConfig
		account string
		want    bool
	}{
		{"global on", handlerConfig{ShowTeamsRoom: true}, "alice", true},
		{"global off, not allowlisted", handlerConfig{ShowTeamsRoom: false}, "alice", false},
		{"global off, allowlisted", handlerConfig{ShowTeamsRoom: false, ShowTeamsAccounts: map[string]bool{"alice": true}}, "alice", true},
		{"global off, other allowlisted", handlerConfig{ShowTeamsRoom: false, ShowTeamsAccounts: map[string]bool{"bob": true}}, "alice", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.effectiveShowTeams(tt.account); got != tt.want {
				t.Errorf("effectiveShowTeams(%q) = %v, want %v", tt.account, got, tt.want)
			}
		})
	}
}

func TestTeamsAccountSet_TrimsWhitespace(t *testing.T) {
	set := teamsAccountSet([]string{"alice", " bob", "  ", ""})
	if !set["alice"] || !set["bob"] {
		t.Fatalf("expected alice+bob (trimmed), got %v", set)
	}
	if set[" bob"] || len(set) != 2 {
		t.Fatalf("blank/untrimmed entries must be dropped, got %v", set)
	}
}
