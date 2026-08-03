package service

import (
	"testing"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

func TestValidateSectionName(t *testing.T) {
	ok := []string{"Work", "團隊", "a-b_c.d/(e)", "A B", repeat("x", 50)}
	for _, n := range ok {
		if err := validateSectionName(n); err != nil {
			t.Errorf("validateSectionName(%q) = %v, want nil", n, err)
		}
	}
	bad := []string{"", repeat("x", 51), "a  b", "bad!", "no@sign", "semi;colon"}
	for _, n := range bad {
		if err := validateSectionName(n); err == nil {
			t.Errorf("validateSectionName(%q) = nil, want error", n)
		} else if !errcode.HasReason(err, errcode.UserChatlistInvalidName) {
			t.Errorf("validateSectionName(%q) reason mismatch: %v", n, err)
		}
	}
}

func TestValidateSortMode(t *testing.T) {
	if err := validateSortMode(model.SortModeCustom); err != nil {
		t.Errorf("custom rejected: %v", err)
	}
	if err := validateSortMode(model.SortModeMostRecent); err != nil {
		t.Errorf("mostRecent rejected: %v", err)
	}
	if err := validateSortMode("weird"); err == nil {
		t.Error("weird accepted, want error")
	}
}

func TestIsPermutation(t *testing.T) {
	if !isPermutation([]string{"a", "b", "c"}, []string{"c", "a", "b"}) {
		t.Error("valid permutation rejected")
	}
	if isPermutation([]string{"a", "b"}, []string{"a", "a"}) {
		t.Error("dup accepted as permutation")
	}
	if isPermutation([]string{"a", "b"}, []string{"a"}) {
		t.Error("short list accepted as permutation")
	}
}

func TestInsertAboveChatsAndRemove(t *testing.T) {
	st := model.DefaultChatlistState()
	insertAboveChats(st, "sec-1")
	// sec-1 must land immediately before "chats".
	idx := indexOf(st.SectionOrder, "sec-1")
	if idx < 0 || st.SectionOrder[idx+1] != model.SectionChats {
		t.Fatalf("sec-1 not above chats: %v", st.SectionOrder)
	}
	st.Sections = append(st.Sections, model.ChatlistSection{ID: "sec-1", Name: "S1"})
	removeSection(st, "sec-1")
	if indexOf(st.SectionOrder, "sec-1") >= 0 || findSection(st, "sec-1") != nil {
		t.Fatalf("sec-1 not removed: %v", st.SectionOrder)
	}
}

func TestHasCustomName(t *testing.T) {
	st := &model.ChatlistState{Sections: []model.ChatlistSection{
		{ID: "b1", Name: "Chats", BuiltIn: true},
		{ID: "c1", Name: "Work", BuiltIn: false},
	}}
	if !hasCustomName(st, "Work", "") {
		t.Error("existing custom name not detected")
	}
	if hasCustomName(st, "Work", "c1") {
		t.Error("self-exclusion failed on rename")
	}
	if hasCustomName(st, "work", "") {
		t.Error("name match should be case-sensitive")
	}
	if hasCustomName(st, "Chats", "") {
		t.Error("built-in name should not collide with custom uniqueness")
	}
}

func repeat(r string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += r
	}
	return out
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}
