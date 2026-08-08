package model

import "testing"

func TestDefaultChatlistStateHasBuiltins(t *testing.T) {
	st := DefaultChatlistState()
	want := []string{SectionFavorites, SectionApps, SectionTeams, SectionChats}
	if len(st.SectionOrder) != len(want) {
		t.Fatalf("SectionOrder = %v, want %v", st.SectionOrder, want)
	}
	for i, id := range want {
		if st.SectionOrder[i] != id {
			t.Fatalf("SectionOrder[%d] = %q, want %q", i, st.SectionOrder[i], id)
		}
	}
	if len(st.Sections) != len(want) {
		t.Fatalf("Sections len = %d, want %d", len(st.Sections), len(want))
	}
	for _, s := range st.Sections {
		if !s.BuiltIn {
			t.Errorf("section %q should be built-in", s.ID)
		}
		if s.SortMode != SortModeMostRecent {
			t.Errorf("built-in %q sortMode = %q, want mostRecent", s.ID, s.SortMode)
		}
	}
}

func TestIsBuiltinSection(t *testing.T) {
	for _, id := range []string{SectionFavorites, SectionApps, SectionTeams, SectionChats} {
		if !IsBuiltinSection(id) {
			t.Errorf("%q should be built-in", id)
		}
	}
	for _, id := range []string{"", "custom-123", "Teams", "work"} {
		if IsBuiltinSection(id) {
			t.Errorf("%q should NOT be built-in", id)
		}
	}
}
