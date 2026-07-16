package subject_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestSubjectBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"MsgSend", subject.MsgSend("alice", "r1", "site-a"),
			"chat.user.alice.room.r1.site-a.msg.send"},
		{"UserResponse", subject.UserResponse("alice", "req-1"),
			"chat.user.alice.response.req-1"},
		{"RoomMetadataUpdate", subject.RoomMetadataUpdate("r1"),
			"chat.room.r1.event.metadata.update"},
		{"RoomMsgStream", subject.RoomMsgStream("r1"),
			"chat.room.r1.stream.msg"},
		{"UserRoomUpdate", subject.UserRoomUpdate("alice"),
			"chat.user.alice.event.room.update"},
		{"UserMsgStream", subject.UserMsgStream("alice"),
			"chat.user.alice.stream.msg"},
		{"MemberRoleUpdate", subject.MemberRoleUpdate("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.role-update"},
		{"RoomCanonical", subject.RoomCanonical("site-a", "invited"),
			"chat.room.canonical.site-a.invited"},
		{"SubscriptionUpdate", subject.SubscriptionUpdate("alice"),
			"chat.user.alice.event.subscription.update"},
		{"RoomMetadataChanged", subject.RoomMetadataChanged("alice"),
			"chat.user.alice.event.room.metadata.update"},
		{"Notification", subject.Notification("alice"),
			"chat.user.alice.notification"},
		{"InboxExternal", subject.InboxExternal("site-b", "member_added"),
			"chat.inbox.site-b.external.member_added"},
		{"InboxInternal", subject.InboxInternal("site-a", "member_added"),
			"chat.inbox.site-a.internal.member_added"},
		{"InboxExternalMemberRemoved", subject.InboxExternal("site-b", "member_removed"),
			"chat.inbox.site-b.external.member_removed"},
		{"InboxInternalMemberRemoved", subject.InboxInternal("site-a", "member_removed"),
			"chat.inbox.site-a.internal.member_removed"},
		{"MsgCanonicalCreated", subject.MsgCanonicalCreated("site-a"),
			"chat.msg.canonical.site-a.created"},
		{"MsgCanonicalUpdated", subject.MsgCanonicalUpdated("site-a"),
			"chat.msg.canonical.site-a.updated"},
		{"MsgCanonicalDeleted", subject.MsgCanonicalDeleted("site-a"),
			"chat.msg.canonical.site-a.deleted"},
		{"RoomsInfoBatch", subject.RoomsInfoBatch("site-a"),
			"chat.server.request.room.site-a.info.batch"},
		{"RoomEvent", subject.RoomEvent("r1"), "chat.room.r1.event"},
		{"UserRoomEvent", subject.UserRoomEvent("alice"), "chat.user.alice.event.room"},
		{"RoomKeyUpdate", subject.RoomKeyUpdate("alice"),
			"chat.user.alice.event.room.key"},
		{"MemberRemove", subject.MemberRemove("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.remove"},
		{"MemberAdd", subject.MemberAdd("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.add"},
		{"MemberEvent", subject.MemberEvent("r1"),
			"chat.room.r1.event.member"},
		{"RoomMemberEventWildcard", subject.RoomMemberEventWildcard(),
			"chat.room.*.event.member"},
		{"MemberList", subject.MemberList("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.list"},
		{"MemberListWildcard", subject.MemberListWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.list"},
		{"MemberStatuses", subject.MemberStatuses("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.statuses"},
		{"MentionableSubscriptions", subject.MentionableSubscriptions("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.subscription.mentionable"},
		{"OrgMembers", subject.OrgMembers("alice", "sect-eng", "site-a"),
			"chat.user.alice.request.orgs.sect-eng.site-a.members"},
		{"OrgMembersWildcard", subject.OrgMembersWildcard("site-a"),
			"chat.user.*.request.orgs.*.site-a.members"},
		{"MsgThreadPattern", subject.MsgThreadPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.thread"},
		{"MsgThreadParentPattern", subject.MsgThreadParentPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.thread.parent"},
		{"MsgHistory", subject.MsgHistory("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.msg.history"},
		{"MsgThread", subject.MsgThread("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.msg.thread"},
		{"SearchMessages", subject.SearchMessages("alice", "site-a"),
			"chat.user.alice.request.search.site-a.messages"},
		{"SearchRooms", subject.SearchRooms("alice", "site-a"),
			"chat.user.alice.request.search.site-a.rooms"},
		{"SearchMessagesPattern", subject.SearchMessagesPattern("site-a"),
			"chat.user.{account}.request.search.site-a.messages"},
		{"SearchRoomsPattern", subject.SearchRoomsPattern("site-a"),
			"chat.user.{account}.request.search.site-a.rooms"},
		{"SearchApps", subject.SearchApps("alice", "site-a"),
			"chat.user.alice.request.search.site-a.apps"},
		{"SearchAppsPattern", subject.SearchAppsPattern("site-a"),
			"chat.user.{account}.request.search.site-a.apps"},
		{"SearchUsers", subject.SearchUsers("alice", "site-a"),
			"chat.user.alice.request.search.site-a.users"},
		{"SearchUsersPattern", subject.SearchUsersPattern("site-a"),
			"chat.user.{account}.request.search.site-a.users"},
		{"EmojiList", subject.EmojiList("alice", "site-a"),
			"chat.user.alice.request.emoji.site-a.list"},
		{"EmojiListPattern", subject.EmojiListPattern("site-a"),
			"chat.user.{account}.request.emoji.site-a.list"},
		{"EmojiDelete", subject.EmojiDelete("alice", "site-a"),
			"chat.user.alice.request.emoji.site-a.delete"},
		{"EmojiDeletePattern", subject.EmojiDeletePattern("site-a"),
			"chat.user.{account}.request.emoji.site-a.delete"},
		{"MsgEditPattern", subject.MsgEditPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.edit"},
		{"MsgDeletePattern", subject.MsgDeletePattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.delete"},
		{"MsgPinPattern", subject.MsgPinPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.pin"},
		{"MsgUnpinPattern", subject.MsgUnpinPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.unpin"},
		{"MsgPinnedListPattern", subject.MsgPinnedListPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.pinned.list"},
		{"MsgCanonicalPinned", subject.MsgCanonicalPinned("site-a"),
			"chat.msg.canonical.site-a.pinned"},
		{"MsgCanonicalUnpinned", subject.MsgCanonicalUnpinned("site-a"),
			"chat.msg.canonical.site-a.unpinned"},
		{"MsgReactPattern", subject.MsgReactPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.react"},
		{"MsgCanonicalReacted", subject.MsgCanonicalReacted("site-a"),
			"chat.msg.canonical.site-a.reacted"},
		{"MsgGet", subject.MsgGet("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.msg.get"},
		{"RoomKeyGet", subject.RoomKeyGet("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.key.get"},
		{"RoomRename", subject.RoomRename("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.room.rename"},
		{"RoomRenameWildcard", subject.RoomRenameWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.room.rename"},
		{"RoomRestricted", subject.RoomRestricted("site-a"),
			"chat.server.request.room.site-a.restricted"},
		{"RoomAppTabs", subject.RoomAppTabs("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.app.tabs"},
		{"RoomAppTabsWildcard", subject.RoomAppTabsWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.app.tabs"},
		{"RoomAppCmdMenu", subject.RoomAppCmdMenu("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.app.cmd-menu"},
		{"RoomAppCmdMenuWildcard", subject.RoomAppCmdMenuWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.app.cmd-menu"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}

	t.Run("RoomCreate", func(t *testing.T) {
		got := subject.RoomCreate("alice", "site-A")
		want := "chat.user.alice.request.room.site-A.create"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("RoomCreateWildcard", func(t *testing.T) {
		got := subject.RoomCreateWildcard("site-A")
		want := "chat.user.*.request.room.site-A.create"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("PushNotificationFilter", func(t *testing.T) {
		assert.Equal(t,
			"chat.server.notification.push.site-a.>",
			subject.PushNotificationFilter("site-a"))
	})

	t.Run("InboxMemberEventSubjects", func(t *testing.T) {
		got := subject.InboxMemberEventSubjects("site-a")
		want := []string{
			"chat.inbox.site-a.internal.member_added",
			"chat.inbox.site-a.internal.member_removed",
			"chat.inbox.site-a.external.member_added",
			"chat.inbox.site-a.external.member_removed",
		}
		if len(got) != len(want) {
			t.Fatalf("got %d subjects, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})
}

func TestParseUserRoomSubject(t *testing.T) {
	tests := []struct {
		name        string
		subj        string
		wantAccount string
		wantRoomID  string
		wantOK      bool
	}{
		{"invite", "chat.user.alice.request.room.r1.site-a.member.invite", "alice", "r1", true},
		{"history", "chat.user.alice.request.room.r1.site-a.msg.history", "alice", "r1", true},
		{"role_update", "chat.user.alice.request.room.r1.site-a.member.role-update", "alice", "r1", true},
		{"msg_send", "chat.user.alice.room.r1.site-a.msg.send", "alice", "r1", true},
		{"too_short", "chat.user.alice", "", "", false},
		{"no_room", "chat.user.alice.request.foo.bar", "", "", false},
		{"bad_prefix", "foo.user.alice.room.r1", "", "", false},
		{"wildcard_account", "chat.user.*.room.r1.site-a.msg.send", "", "", false},
		{"wildcard_roomid", "chat.user.alice.room.*.site-a.msg.send", "", "", false},
		{"gt_wildcard_account", "chat.user.>.room.r1.site-a.msg.send", "", "", false},
		{"empty_account", "chat.user..room.r1.site-a.msg.send", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, rid, ok := subject.ParseUserRoomSubject(tt.subj)
			if ok != tt.wantOK || account != tt.wantAccount || rid != tt.wantRoomID {
				t.Errorf("ParseUserRoomSubject(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.subj, account, rid, ok, tt.wantAccount, tt.wantRoomID, tt.wantOK)
			}
		})
	}
}

func TestParseUserRoomSiteSubject(t *testing.T) {
	tests := []struct {
		name        string
		subj        string
		wantAccount string
		wantRoomID  string
		wantSiteID  string
		wantOK      bool
	}{
		{"valid msg send", "chat.user.alice.room.r1.site-a.msg.send", "alice", "r1", "site-a", true},
		{"different values", "chat.user.bob.room.room-42.site-b.msg.send", "bob", "room-42", "site-b", true},
		{"too few parts", "chat.user.alice.room.r1.site-a", "", "", "", false},
		{"bad prefix", "foo.user.alice.room.r1.site-a.msg.send", "", "", "", false},
		{"not user", "chat.blah.alice.room.r1.site-a.msg.send", "", "", "", false},
		{"no room token", "chat.user.alice.notroom.r1.site-a.msg.send", "", "", "", false},
		{"empty", "", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, roomID, siteID, ok := subject.ParseUserRoomSiteSubject(tt.subj)
			if ok != tt.wantOK || account != tt.wantAccount || roomID != tt.wantRoomID || siteID != tt.wantSiteID {
				t.Errorf("ParseUserRoomSiteSubject(%q) = (%q, %q, %q, %v), want (%q, %q, %q, %v)",
					tt.subj, account, roomID, siteID, ok, tt.wantAccount, tt.wantRoomID, tt.wantSiteID, tt.wantOK)
			}
		})
	}
}

func TestWildcardPatterns(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"MsgSendWild", subject.MsgSendWildcard("site-a"),
			"chat.user.*.room.*.site-a.msg.send"},
		{"MemberRoleUpdateWild", subject.MemberRoleUpdateWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.role-update"},
		{"RoomCanonicalWild", subject.RoomCanonicalWildcard("site-a"),
			"chat.room.canonical.site-a.>"},
		{"Outbox", subject.Outbox("site-a", "site-b", "subscription_read"),
			"chat.outbox.site-a.site-b.subscription_read"},
		{"OutboxWildcard", subject.OutboxWildcard("site-a"),
			"chat.outbox.site-a.>"},
		{"MsgHistoryWild", subject.MsgHistoryWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.msg.history"},
		{"MsgCanonicalWild", subject.MsgCanonicalWildcard("site-a"),
			"chat.msg.canonical.site-a.>"},
		{"InboxExternalAll", subject.InboxExternalAll("site-a"),
			"chat.inbox.site-a.external.>"},
		{"MemberAddWild", subject.MemberAddWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.add"},
		{"RoomsInfoBatchSubscribe", subject.RoomsInfoBatchSubscribe("site-a"),
			"chat.server.request.room.site-a.info.batch"},
		{"MsgThreadWild", subject.MsgThreadWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.msg.thread"},
		{"MsgThreadParentWild", subject.MsgThreadParentWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.msg.thread.parent"},
		{"RoomKeyGetWildcard", subject.RoomKeyGetWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.key.get"},
		{"MemberStatusesWild", subject.MemberStatusesWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.statuses"},
		{"MentionableSubscriptionsWild", subject.MentionableSubscriptionsWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.subscription.mentionable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestParseOrgMembersSubject(t *testing.T) {
	tests := []struct {
		name     string
		subj     string
		wantOrg  string
		wantSite string
		wantOK   bool
	}{
		{"valid", "chat.user.alice.request.orgs.sect-eng.site-a.members", "sect-eng", "site-a", true},
		{"different site", "chat.user.bob.request.orgs.dept-hr.site-b.members", "dept-hr", "site-b", true},
		{"wrong prefix", "chat.user.alice.request.rooms.get.r1", "", "", false},
		{"wrong suffix", "chat.user.alice.request.orgs.sect-eng.site-a.other", "", "", false},
		{"too short (7-token old form)", "chat.user.alice.request.orgs.sect-eng.members", "", "", false},
		{"too long", "chat.user.alice.request.orgs.sect-eng.site-a.members.x", "", "", false},
		{"empty", "", "", "", false},
		{"wildcard account", "chat.user.*.request.orgs.sect-eng.site-a.members", "", "", false},
		{"wildcard orgID", "chat.user.alice.request.orgs.*.site-a.members", "", "", false},
		{"wildcard siteID", "chat.user.alice.request.orgs.sect-eng.*.members", "", "", false},
		{"multi-token wildcard account", "chat.user.>.request.orgs.sect-eng.site-a.members", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOrg, gotSite, ok := subject.ParseOrgMembersSubject(tt.subj)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if gotOrg != tt.wantOrg {
				t.Errorf("orgID = %q, want %q", gotOrg, tt.wantOrg)
			}
			if gotSite != tt.wantSite {
				t.Errorf("siteID = %q, want %q", gotSite, tt.wantSite)
			}
		})
	}
}

func TestMessageRead(t *testing.T) {
	got := subject.MessageRead("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.message.read"
	if got != want {
		t.Errorf("MessageRead: got %q, want %q", got, want)
	}
}

func TestMessageReadWildcard(t *testing.T) {
	got := subject.MessageReadWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.message.read"
	if got != want {
		t.Errorf("MessageReadWildcard: got %q, want %q", got, want)
	}
}

func TestMessageRead_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MessageRead("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("parse: got (%q,%q,%v), want (alice,r1,true)", account, roomID, ok)
	}
}

func TestMessageReadReceipt(t *testing.T) {
	got := subject.MessageReadReceipt("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.message.read-receipt"
	if got != want {
		t.Errorf("MessageReadReceipt: got %q, want %q", got, want)
	}
}

func TestMessageReadReceiptWildcard(t *testing.T) {
	got := subject.MessageReadReceiptWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.message.read-receipt"
	if got != want {
		t.Errorf("MessageReadReceiptWildcard: got %q, want %q", got, want)
	}
}

func TestMessageReadReceipt_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MessageReadReceipt("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("parse: got (%q,%q,%v), want (alice,r1,true)", account, roomID, ok)
	}
}

func TestMessageThreadRead(t *testing.T) {
	got := subject.MessageThreadRead("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.message.thread.read"
	if got != want {
		t.Errorf("MessageThreadRead: got %q, want %q", got, want)
	}
}

func TestMessageThreadReadWildcard(t *testing.T) {
	got := subject.MessageThreadReadWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.message.thread.read"
	if got != want {
		t.Errorf("MessageThreadReadWildcard: got %q, want %q", got, want)
	}
}

func TestMessageThreadRead_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("ParseUserRoomSubject(%q) = (%q, %q, %v); want (\"alice\", \"r1\", true)",
			subj, account, roomID, ok)
	}
}

func TestMuteToggle(t *testing.T) {
	got := subject.MuteToggle("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.mute.toggle"
	if got != want {
		t.Errorf("MuteToggle: got %q, want %q", got, want)
	}
}

func TestMuteToggleWildcard(t *testing.T) {
	got := subject.MuteToggleWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.mute.toggle"
	if got != want {
		t.Errorf("MuteToggleWildcard: got %q, want %q", got, want)
	}
}

func TestMuteToggle_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MuteToggle("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("ParseUserRoomSubject(%q) = (%q,%q,%v), want (alice,r1,true)", subj, account, roomID, ok)
	}
}

func TestFavoriteToggle(t *testing.T) {
	got := subject.FavoriteToggle("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.favorite.toggle"
	if got != want {
		t.Errorf("FavoriteToggle: got %q, want %q", got, want)
	}
}

func TestFavoriteToggleWildcard(t *testing.T) {
	got := subject.FavoriteToggleWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.favorite.toggle"
	if got != want {
		t.Errorf("FavoriteToggleWildcard: got %q, want %q", got, want)
	}
}

func TestFavoriteToggle_ParseUserRoomSubject(t *testing.T) {
	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("ParseUserRoomSubject(%q) = (%q,%q,%v), want (alice,r1,true)", subj, account, roomID, ok)
	}
}

func TestParseRoomCreateSubject(t *testing.T) {
	tests := []struct {
		name        string
		subj        string
		wantAccount string
		wantOK      bool
	}{
		{"valid", "chat.user.alice.request.room.site-A.create", "alice", true},
		{"different account", "chat.user.bob.request.room.site-B.create", "bob", true},
		{"too many tokens", "chat.user.alice.request.room.site-A.member.add", "", false},
		{"too few tokens", "chat.user.alice.request.room.site-A", "", false},
		{"wrong suffix", "chat.user.alice.request.room.site-A.member", "", false},
		{"wrong prefix", "foo.user.alice.request.room.site-A.create", "", false},
		{"empty", "", "", false},
		{"account is wildcard star", "chat.user.*.request.room.site-A.create", "", false},
		{"account is wildcard tail", "chat.user.>.request.room.site-A.create", "", false},
		{"account contains star", "chat.user.al*ce.request.room.site-A.create", "", false},
		{"account contains tail", "chat.user.al>ce.request.room.site-A.create", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := subject.ParseRoomCreateSubject(tt.subj)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v for %q", ok, tt.wantOK, tt.subj)
			}
			if got != tt.wantAccount {
				t.Errorf("account = %q, want %q", got, tt.wantAccount)
			}
		})
	}
}

func TestRoomCanonicalOperation(t *testing.T) {
	tests := map[string]struct {
		subject string
		want    string
		ok      bool
	}{
		"member.add": {"chat.room.canonical.site-A.member.add", "member.add", true},
		"create":     {"chat.room.canonical.site-A.create", "create", true},
		"unrelated":  {"chat.user.alice.request.room.site-A.create", "", false},
		"too short":  {"chat.room.canonical.site-A", "", false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			op, ok := subject.RoomCanonicalOperation(tc.subject)
			if ok != tc.ok {
				t.Errorf("ok = %v, want %v", ok, tc.ok)
			}
			if op != tc.want {
				t.Errorf("op = %q, want %q", op, tc.want)
			}
		})
	}
}

func TestRoomCreateDMSync(t *testing.T) {
	got := subject.RoomCreateDMSync("site-a")
	want := "chat.server.request.room.site-a.create.dm"
	if got != want {
		t.Errorf("RoomCreateDMSync: got %q, want %q", got, want)
	}
}

func TestUserServiceBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"status.getByName", subject.UserStatusGetByName("alice", "s1"), "chat.user.alice.request.user.s1.status.getByName"},
		{"profile.getByName", subject.UserProfileGetByName("alice", "s1"), "chat.user.alice.request.user.s1.profile.getByName"},
		{"status.set", subject.UserStatusSet("alice", "s1"), "chat.user.alice.request.user.s1.status.set"},
		{"subscription.getChannels", subject.UserSubscriptionGetChannels("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getChannels"},
		{"subscription.getDM", subject.UserSubscriptionGetDM("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getDM"},
		{"subscription.count", subject.UserSubscriptionCount("alice", "s1"), "chat.user.alice.request.user.s1.subscription.count"},
		{"apps.list", subject.UserAppsList("alice", "s1"), "chat.user.alice.request.user.s1.apps.list"},
		{"apps.categories", subject.UserAppsCategories("alice", "s1"), "chat.user.alice.request.user.s1.apps.categories"},
		{"subscription.list", subject.UserSubscriptionList("alice", "s1"), "chat.user.alice.request.user.s1.subscription.list"},
		{"subscription.setAppSubscription", subject.UserSubscriptionSetAppSubscription("alice", "s1"), "chat.user.alice.request.user.s1.subscription.setAppSubscription"},
		{"subscription.getByRoomID", subject.UserSubscriptionGetByRoomID("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getByRoomID"},
		{"me", subject.UserMe("alice", "s1"), "chat.user.alice.request.user.s1.me"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestParseUserSubject(t *testing.T) {
	t.Run("status.getByName roundtrips", func(t *testing.T) {
		subj := subject.UserStatusGetByName("alice", "s1")
		account, siteID, area, action, ok := subject.ParseUserSubject(subj)
		assert.True(t, ok)
		assert.Equal(t, "alice", account)
		assert.Equal(t, "s1", siteID)
		assert.Equal(t, "status", area)
		assert.Equal(t, "getByName", action)
	})

	t.Run("apps.list roundtrips", func(t *testing.T) {
		_, _, area, action, ok := subject.ParseUserSubject(subject.UserAppsList("alice", "s1"))
		assert.True(t, ok)
		assert.Equal(t, "apps", area)
		assert.Equal(t, "list", action)
	})

	t.Run("rejects malformed", func(t *testing.T) {
		bad := []string{
			"",
			"chat.user.alice",
			"chat.room.r1.event.metadata.update",
			"chat.user.alice.request.user.s1.status.getByName.extra",
			"chat.user.alice.notrequest.user.s1.status.getByName",
			"chat.user.alice.request.notuser.s1.status.getByName",
			"chat.user.alice.request.user.s1.bogus.action",
			"chat.user.alice.request.user.s1.room.r1.subscription.get",
		}
		for _, s := range bad {
			_, _, _, _, ok := subject.ParseUserSubject(s)
			assert.False(t, ok, "expected ok=false for %q", s)
		}
	})
}

func TestParseStatusSubject(t *testing.T) {
	account, action, ok := subject.ParseStatusSubject(subject.UserStatusSet("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "set", action)

	_, _, ok = subject.ParseStatusSubject(subject.UserAppsList("alice", "s1"))
	assert.False(t, ok, "wrong area must be rejected")
}

func TestParseSubscriptionSubject(t *testing.T) {
	account, action, ok := subject.ParseSubscriptionSubject(subject.UserSubscriptionList("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "list", action)

	_, _, ok = subject.ParseSubscriptionSubject(subject.UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseAppsSubject(t *testing.T) {
	account, action, ok := subject.ParseAppsSubject(subject.UserAppsList("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "list", action)

	account, action, ok = subject.ParseAppsSubject(subject.UserAppsCategories("bob", "s2"))
	assert.True(t, ok)
	assert.Equal(t, "bob", account)
	assert.Equal(t, "categories", action)

	_, _, ok = subject.ParseAppsSubject(subject.UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseRoomSubject(t *testing.T) {
	t.Run("parses valid 10-token room subject", func(t *testing.T) {
		subj := "chat.user.alice.request.user.s1.room.r1.subscription.get"
		account, roomID, action, ok := subject.ParseRoomSubject(subj)
		assert.True(t, ok)
		assert.Equal(t, "alice", account)
		assert.Equal(t, "r1", roomID)
		assert.Equal(t, "get", action)
	})

	t.Run("rejects malformed", func(t *testing.T) {
		bad := []string{
			"",
			"chat.user.alice.request.user.s1.status.getByName",
			"chat.user.alice.request.user.s1.room.r1",
			"chat.user.alice.request.user.s1.room.r1.subscription",
			"chat.user.alice.request.user.s1.room.r1.subscription.get.extra",
			"chat.user.alice.notrequest.user.s1.room.r1.subscription.get",
			"chat.user.alice.request.notuser.s1.room.r1.subscription.get",
			"chat.user.alice.request.user.s1.notroom.r1.subscription.get",
		}
		for _, s := range bad {
			_, _, _, ok := subject.ParseRoomSubject(s)
			assert.False(t, ok, "expected ok=false for %q", s)
		}
	})
}

func TestUserServiceWildcards(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"status", subject.UserStatusWildCard("s1"), "chat.user.*.request.user.s1.status.>"},
		{"subscription", subject.UserSubscriptionWildCard("s1"), "chat.user.*.request.user.s1.subscription.>"},
		{"profile", subject.UserProfileWildCard("s1"), "chat.user.*.request.user.s1.profile.>"},
		{"room", subject.UserRoomWildCard("s1"), "chat.user.*.request.user.s1.room.>"},
		{"apps", subject.UserAppsWildCard("s1"), "chat.user.*.request.user.s1.apps.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestIsValidAccountToken(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    bool
	}{
		{"plain account", "alice", true},
		{"dash underscore digits", "user-01_X", true},
		{"at sign is routable", "alice@corp", true},
		{"unicode is routable", "júlio", true},
		{"empty", "", false},
		{"dot splits subject tokens", "john.doe", false},
		{"single-token wildcard", "mal*ory", false},
		{"multi-token wildcard", "mal>ory", false},
		{"space", "mal ory", false},
		{"tab", "mal\tory", false},
		{"control rune", "mal\x00ory", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, subject.IsValidAccountToken(tt.account))
		})
	}
}

func TestUserServiceBuildersRejectWildcardAccounts(t *testing.T) {
	builders := []struct {
		name string
		fn   func()
	}{
		{"UserStatusGetByName", func() { subject.UserStatusGetByName("*", "s1") }},
		{"UserProfileGetByName", func() { subject.UserProfileGetByName("*", "s1") }},
		{"UserStatusSet", func() { subject.UserStatusSet("*", "s1") }},
		{"UserSubscriptionGetChannels", func() { subject.UserSubscriptionGetChannels("*", "s1") }},
		{"UserSubscriptionGetDM", func() { subject.UserSubscriptionGetDM(">", "s1") }},
		{"UserSubscriptionCount", func() { subject.UserSubscriptionCount(">", "s1") }},
		{"UserAppsList", func() { subject.UserAppsList(">", "s1") }},
		{"UserAppsCategories", func() { subject.UserAppsCategories(">", "s1") }},
		{"UserSubscriptionList", func() { subject.UserSubscriptionList("*", "s1") }},
		{"UserSubscriptionSetAppSubscription", func() { subject.UserSubscriptionSetAppSubscription("*", "s1") }},
		{"UserSubscriptionGetByRoomID", func() { subject.UserSubscriptionGetByRoomID(">", "s1") }},
		{"UserMe", func() { subject.UserMe("*", "s1") }},
	}
	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			assert.Panics(t, b.fn)
		})
	}
}

func TestRoomAppAndOrgMembersBuildersRejectWildcards(t *testing.T) {
	builders := []struct {
		name string
		fn   func()
	}{
		{"RoomAppTabs star account", func() { subject.RoomAppTabs("*", "r1", "s1") }},
		{"RoomAppTabs gt account", func() { subject.RoomAppTabs(">", "r1", "s1") }},
		{"RoomAppCmdMenu star account", func() { subject.RoomAppCmdMenu("*", "r1", "s1") }},
		{"RoomAppCmdMenu gt account", func() { subject.RoomAppCmdMenu(">", "r1", "s1") }},
		{"OrgMembers star account", func() { subject.OrgMembers("*", "o1", "s1") }},
		{"OrgMembers star orgID", func() { subject.OrgMembers("a1", "*", "s1") }},
		{"OrgMembers gt siteID", func() { subject.OrgMembers("a1", "o1", ">") }},
	}
	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			assert.Panics(t, b.fn)
		})
	}
}

func TestEmojiBuildersRejectWildcardAccounts(t *testing.T) {
	builders := []struct {
		name string
		fn   func()
	}{
		{"EmojiList star account", func() { subject.EmojiList("*", "s1") }},
		{"EmojiList gt account", func() { subject.EmojiList(">", "s1") }},
		{"EmojiDelete star account", func() { subject.EmojiDelete("*", "s1") }},
		{"EmojiDelete gt account", func() { subject.EmojiDelete(">", "s1") }},
	}
	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			assert.Panics(t, b.fn)
		})
	}
}

func TestParseUserSubject_RejectsWildcardAccount(t *testing.T) {
	bad := []string{
		"chat.user.*.request.user.s1.status.getByName",
		"chat.user.>.request.user.s1.status.getByName",
		"chat.user..request.user.s1.status.getByName",
	}
	for _, s := range bad {
		_, _, _, _, ok := subject.ParseUserSubject(s)
		assert.False(t, ok, "expected ok=false for %q", s)
	}
}

func TestParseRoomSubject_RejectsWildcardAccount(t *testing.T) {
	bad := []string{
		"chat.user.*.request.user.s1.room.r1.subscription.get",
		"chat.user.>.request.user.s1.room.r1.subscription.get",
		"chat.user..request.user.s1.room.r1.subscription.get",
	}
	for _, s := range bad {
		_, _, _, ok := subject.ParseRoomSubject(s)
		assert.False(t, ok, "expected ok=false for %q", s)
	}
}

func TestRoomAppTabs_ParseUserRoomSubject(t *testing.T) {
	subj := subject.RoomAppTabs("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "r1", roomID)
}

func TestRoomAppCmdMenu_ParseUserRoomSubject(t *testing.T) {
	subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "r1", roomID)
}

func TestMemberStatuses_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MemberStatuses("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "r1", roomID)
}

func TestMentionableSubscriptions_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MentionableSubscriptions("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "r1", roomID)
}

func TestUserServicePatternBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"status.getByName", subject.UserStatusGetByNamePattern("s1"), "chat.user.{account}.request.user.s1.status.getByName"},
		{"profile.getByName", subject.UserProfileGetByNamePattern("s1"), "chat.user.{account}.request.user.s1.profile.getByName"},
		{"status.set", subject.UserStatusSetPattern("s1"), "chat.user.{account}.request.user.s1.status.set"},
		{"subscription.getChannels", subject.UserSubscriptionGetChannelsPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getChannels"},
		{"subscription.getDM", subject.UserSubscriptionGetDMPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getDM"},
		{"subscription.count", subject.UserSubscriptionCountPattern("s1"), "chat.user.{account}.request.user.s1.subscription.count"},
		{"apps.list", subject.UserAppsListPattern("s1"), "chat.user.{account}.request.user.s1.apps.list"},
		{"apps.categories", subject.UserAppsCategoriesPattern("s1"), "chat.user.{account}.request.user.s1.apps.categories"},
		{"subscription.list", subject.UserSubscriptionListPattern("s1"), "chat.user.{account}.request.user.s1.subscription.list"},
		{"subscription.setAppSubscription", subject.UserSubscriptionSetAppSubscriptionPattern("s1"), "chat.user.{account}.request.user.s1.subscription.setAppSubscription"},
		{"subscription.getByRoomID", subject.UserSubscriptionGetByRoomIDPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getByRoomID"},
		{"me", subject.UserMePattern("s1"), "chat.user.{account}.request.user.s1.me"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestPushNotification(t *testing.T) {
	assert.Equal(t,
		"chat.server.notification.push.site-a.send",
		subject.PushNotification("site-a"))
}

func TestPresenceSnapshot(t *testing.T) {
	assert.Equal(t,
		"chat.presence.site-a.request.snapshot",
		subject.PresenceSnapshot("site-a"))
}

func TestSubscriptionUpdateWildcard(t *testing.T) {
	assert.Equal(t,
		"chat.user.*.event.subscription.update",
		subject.SubscriptionUpdateWildcard())
}

func TestParseSubscriptionUpdateAccount(t *testing.T) {
	acct, ok := subject.ParseSubscriptionUpdateAccount("chat.user.alice.event.subscription.update")
	assert.True(t, ok)
	assert.Equal(t, "alice", acct)

	_, ok = subject.ParseSubscriptionUpdateAccount("chat.user.alice.event.room.update")
	assert.False(t, ok)

	_, ok = subject.ParseSubscriptionUpdateAccount("chat.user.*.event.subscription.update")
	assert.False(t, ok) // wildcard token rejected
}

func TestParseOutbox(t *testing.T) {
	// Round-trips with the builder.
	origin, dest, evt, ok := subject.ParseOutbox(subject.Outbox("site-a", "site-b", "subscription_read"))
	require.True(t, ok)
	assert.Equal(t, "site-a", origin)
	assert.Equal(t, "site-b", dest)
	assert.Equal(t, "subscription_read", evt)

	// Malformed subjects are rejected.
	for _, bad := range []string{
		"chat.outbox.site-a.site-b", // missing eventType
		"chat.outbox.site-a",        // too short
		"chat.inbox.site-a.site-b.x",
		"chat.outbox..site-b.x", // empty origin
		"",
	} {
		_, _, _, ok := subject.ParseOutbox(bad)
		assert.False(t, ok, "should reject %q", bad)
	}
}

func TestRoomPatternsMatchWildcards(t *testing.T) {
	const site = "site-a"
	repl := strings.NewReplacer("{account}", "*", "{roomID}", "*", "{orgID}", "*")
	cases := []struct{ name, pattern, wildcard string }{
		{"create", subject.RoomCreatePattern(site), subject.RoomCreateWildcard(site)},
		{"role-update", subject.MemberRoleUpdatePattern(site), subject.MemberRoleUpdateWildcard(site)},
		{"remove", subject.MemberRemovePattern(site), subject.MemberRemoveWildcard(site)},
		{"add", subject.MemberAddPattern(site), subject.MemberAddWildcard(site)},
		{"list", subject.MemberListPattern(site), subject.MemberListWildcard(site)},
		{"member-statuses", subject.MemberStatusesPattern(site), subject.MemberStatusesWildcard(site)},
		{"mentionable", subject.MentionableSubscriptionsPattern(site), subject.MentionableSubscriptionsWildcard(site)},
		{"org-members", subject.OrgMembersPattern(site), subject.OrgMembersWildcard(site)},
		{"message-read", subject.MessageReadPattern(site), subject.MessageReadWildcard(site)},
		{"read-receipt", subject.MessageReadReceiptPattern(site), subject.MessageReadReceiptWildcard(site)},
		{"thread-read", subject.MessageThreadReadPattern(site), subject.MessageThreadReadWildcard(site)},
		{"key-get", subject.RoomKeyGetPattern(site), subject.RoomKeyGetWildcard(site)},
		{"mute", subject.MuteTogglePattern(site), subject.MuteToggleWildcard(site)},
		{"favorite", subject.FavoriteTogglePattern(site), subject.FavoriteToggleWildcard(site)},
		{"rename", subject.RoomRenamePattern(site), subject.RoomRenameWildcard(site)},
		{"app-tabs", subject.RoomAppTabsPattern(site), subject.RoomAppTabsWildcard(site)},
		{"app-cmd-menu", subject.RoomAppCmdMenuPattern(site), subject.RoomAppCmdMenuWildcard(site)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wildcard, repl.Replace(tc.pattern),
				"pattern with params replaced by * must equal the existing wildcard subscription subject")
		})
	}
}

func TestPresenceSubjects(t *testing.T) {
	assert.Equal(t, "chat.user.{account}.event.presence.site-a.hello", subject.PresenceHelloPattern("site-a"))
	assert.Equal(t, "chat.user.{account}.event.presence.site-a.ping", subject.PresencePingPattern("site-a"))
	assert.Equal(t, "chat.user.{account}.event.presence.site-a.activity", subject.PresenceActivityPattern("site-a"))
	assert.Equal(t, "chat.user.{account}.event.presence.site-a.bye", subject.PresenceByePattern("site-a"))
	assert.Equal(t, "chat.user.{account}.request.presence.site-a.manual.set", subject.PresenceManualSetPattern("site-a"))
	assert.Equal(t, "chat.user.presence.site-a.query.batch", subject.PresenceQueryBatch("site-a"))
	assert.Equal(t, "chat.server.request.presence.site-a.query.batch", subject.PresenceQueryBatchPeer("site-a"))
	assert.Equal(t, "chat.user.presence.state.alice", subject.PresenceState("alice"))
}

func TestTeamsSubjectBuilders(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		got, err := subject.TeamsRoomCall("alice", "r1", "site-a")
		require.NoError(t, err)
		assert.Equal(t, "chat.user.alice.request.room.r1.site-a.teams.call", got)

		got, err = subject.TeamsMeeting("alice", "r1", "site-a")
		require.NoError(t, err)
		assert.Equal(t, "chat.user.alice.request.room.r1.site-a.teams.meeting", got)

		got, err = subject.TeamsUserCall("alice", "site-a")
		require.NoError(t, err)
		assert.Equal(t, "chat.user.alice.request.teams.site-a.call.user", got)
	})

	t.Run("wildcard account rejected with error (no panic)", func(t *testing.T) {
		got, err := subject.TeamsRoomCall("*", "r1", "site-a")
		require.Error(t, err)
		assert.Empty(t, got)

		got, err = subject.TeamsMeeting(">", "r1", "site-a")
		require.Error(t, err)
		assert.Empty(t, got)

		got, err = subject.TeamsUserCall("*", "site-a")
		require.Error(t, err)
		assert.Empty(t, got)
	})

	t.Run("empty account rejected with error", func(t *testing.T) {
		_, err := subject.TeamsRoomCall("", "r1", "site-a")
		require.Error(t, err)
		_, err = subject.TeamsMeeting("", "r1", "site-a")
		require.Error(t, err)
		_, err = subject.TeamsUserCall("", "site-a")
		require.Error(t, err)
	})

	t.Run("pattern builders", func(t *testing.T) {
		assert.Equal(t, "chat.user.{account}.request.room.{roomID}.site-a.teams.call", subject.TeamsRoomCallPattern("site-a"))
		assert.Equal(t, "chat.user.{account}.request.room.{roomID}.site-a.teams.meeting", subject.TeamsMeetingPattern("site-a"))
		assert.Equal(t, "chat.user.{account}.request.teams.site-a.call.user", subject.TeamsUserCallPattern("site-a"))
	})
}

func TestUserThreadUnreadSummary(t *testing.T) {
	assert.Equal(t,
		"chat.user.alice.request.user.site-a.thread.unread.summary",
		subject.UserThreadUnreadSummary("alice", "site-a"))
	assert.Equal(t,
		"chat.user.{account}.request.user.site-a.thread.unread.summary",
		subject.UserThreadUnreadSummaryPattern("site-a"))
}

func TestUserThreadUnreadSummary_PanicsOnWildcardAccount(t *testing.T) {
	assert.Panics(t, func() { subject.UserThreadUnreadSummary("a.*", "site-a") })
}

func TestUserThreadReadAll(t *testing.T) {
	assert.Equal(t,
		"chat.user.alice.request.user.site-a.thread.read.all",
		subject.UserThreadReadAll("alice", "site-a"))
	assert.Equal(t,
		"chat.user.{account}.request.user.site-a.thread.read.all",
		subject.UserThreadReadAllPattern("site-a"))
}

func TestUserThreadReadAll_PanicsOnWildcardAccount(t *testing.T) {
	assert.Panics(t, func() { subject.UserThreadReadAll("a.*", "site-a") })
}

func TestRoomThreadReadAll(t *testing.T) {
	assert.Equal(t,
		"chat.server.request.room.site-a.thread.read.all",
		subject.RoomThreadReadAll("site-a"))
	assert.Equal(t,
		subject.RoomThreadReadAll("site-a"),
		subject.RoomThreadReadAllSubscribe("site-a"))
}

func TestThreadRoomInfoBatch(t *testing.T) {
	assert.Equal(t,
		"chat.server.request.room.site-a.thread.info.batch",
		subject.ThreadRoomInfoBatch("site-a"))
}

func TestOrgSyncEmployeesUpsert(t *testing.T) {
	got := subject.OrgSyncEmployeesUpsert("site-a")
	assert.Equal(t, "chat.hr.site-a.employees.upsert", got)
}
