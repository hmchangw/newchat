package subject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
		{"Outbox", subject.Outbox("site-a", "site-b", "member_added"),
			"outbox.site-a.to.site-b.member_added"},
		{"InboxMemberAdded", subject.InboxMemberAdded("site-a"),
			"chat.inbox.site-a.member_added"},
		{"InboxMemberRemoved", subject.InboxMemberRemoved("site-a"),
			"chat.inbox.site-a.member_removed"},
		{"InboxMemberAddedAggregate", subject.InboxMemberAddedAggregate("site-a"),
			"chat.inbox.site-a.aggregate.member_added"},
		{"InboxMemberRemovedAggregate", subject.InboxMemberRemovedAggregate("site-a"),
			"chat.inbox.site-a.aggregate.member_removed"},
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
		{"OrgMembers", subject.OrgMembers("alice", "sect-eng"),
			"chat.user.alice.request.orgs.sect-eng.members"},
		{"OrgMembersWildcard", subject.OrgMembersWildcard(),
			"chat.user.*.request.orgs.*.members"},
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
		{"MsgGet", subject.MsgGet("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.msg.get"},
		{"RoomKeyGet", subject.RoomKeyGet("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.key.get"},
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

	t.Run("InboxMemberEventSubjects", func(t *testing.T) {
		got := subject.InboxMemberEventSubjects("site-a")
		want := []string{
			"chat.inbox.site-a.member_added",
			"chat.inbox.site-a.member_removed",
			"chat.inbox.site-a.aggregate.member_added",
			"chat.inbox.site-a.aggregate.member_removed",
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
		{"MsgHistoryWild", subject.MsgHistoryWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.msg.history"},
		{"MsgCanonicalWild", subject.MsgCanonicalWildcard("site-a"),
			"chat.msg.canonical.site-a.>"},
		{"OutboxWild", subject.OutboxWildcard("site-a"),
			"outbox.site-a.>"},
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
		name    string
		subj    string
		wantOrg string
		wantOK  bool
	}{
		{"valid", "chat.user.alice.request.orgs.sect-eng.members", "sect-eng", true},
		{"wrong prefix", "chat.user.alice.request.rooms.get.r1", "", false},
		{"wrong suffix", "chat.user.alice.request.orgs.sect-eng.other", "", false},
		{"too short", "chat.user.alice.request.orgs", "", false},
		{"too long", "chat.user.alice.request.orgs.sect-eng.members.x", "", false},
		{"empty", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := subject.ParseOrgMembersSubject(tt.subj)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.wantOrg {
				t.Errorf("orgID = %q, want %q", got, tt.wantOrg)
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
		// Wildcard guard: NATS '*' / '>' must never leak into the parsed account.
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
		{"status.set", subject.UserStatusSet("alice", "s1"), "chat.user.alice.request.user.s1.status.set"},
		{"profile.getByName", subject.UserProfileGetByName("alice", "s1"), "chat.user.alice.request.user.s1.profile.getByName"},
		{"subscription.getCurrent", subject.UserSubscriptionGetCurrent("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getCurrent"},
		{"subscription.getRooms", subject.UserSubscriptionGetRooms("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getRooms"},
		{"subscription.getChannels", subject.UserSubscriptionGetChannels("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getChannels"},
		{"subscription.getDM", subject.UserSubscriptionGetDM("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getDM"},
		{"subscription.getApps", subject.UserSubscriptionGetApps("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getApps"},
		{"subscription.count", subject.UserSubscriptionCount("alice", "s1"), "chat.user.alice.request.user.s1.subscription.count"},
		{"subscription.subscribeApp", subject.UserSubscriptionSubscribeApp("alice", "s1"), "chat.user.alice.request.user.s1.subscription.subscribeApp"},
		{"subscription.unsubscribeApp", subject.UserSubscriptionUnsubscribeApp("alice", "s1"), "chat.user.alice.request.user.s1.subscription.unsubscribeApp"},
		{"room.subscription.get", subject.UserRoomSubscriptionGet("alice", "s1", "r1"), "chat.user.alice.request.user.s1.room.r1.subscription.get"},
		{"apps.list", subject.UserAppsList("alice", "s1"), "chat.user.alice.request.user.s1.apps.list"},
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

	_, _, ok = subject.ParseStatusSubject(subject.UserProfileGetByName("alice", "s1"))
	assert.False(t, ok, "wrong area must be rejected")
}

func TestParseSubscriptionSubject(t *testing.T) {
	account, action, ok := subject.ParseSubscriptionSubject(subject.UserSubscriptionGetCurrent("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "getCurrent", action)

	_, _, ok = subject.ParseSubscriptionSubject(subject.UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseProfileSubject(t *testing.T) {
	account, action, ok := subject.ParseProfileSubject(subject.UserProfileGetByName("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "getByName", action)

	_, _, ok = subject.ParseProfileSubject(subject.UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseAppsSubject(t *testing.T) {
	account, action, ok := subject.ParseAppsSubject(subject.UserAppsList("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "list", action)

	_, _, ok = subject.ParseAppsSubject(subject.UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseRoomSubject(t *testing.T) {
	t.Run("subscription.get roundtrips", func(t *testing.T) {
		subj := subject.UserRoomSubscriptionGet("alice", "s1", "r1")
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

func TestUserServiceBuildersRejectWildcardAccounts(t *testing.T) {
	builders := []struct {
		name string
		fn   func()
	}{
		{"UserStatusGetByName", func() { subject.UserStatusGetByName("*", "s1") }},
		{"UserStatusSet", func() { subject.UserStatusSet("*", "s1") }},
		{"UserProfileGetByName", func() { subject.UserProfileGetByName("*", "s1") }},
		{"UserSubscriptionGetCurrent", func() { subject.UserSubscriptionGetCurrent("*", "s1") }},
		{"UserSubscriptionGetRooms", func() { subject.UserSubscriptionGetRooms("*", "s1") }},
		{"UserSubscriptionGetChannels", func() { subject.UserSubscriptionGetChannels("*", "s1") }},
		{"UserSubscriptionGetDM", func() { subject.UserSubscriptionGetDM(">", "s1") }},
		{"UserSubscriptionGetApps", func() { subject.UserSubscriptionGetApps(">", "s1") }},
		{"UserSubscriptionCount", func() { subject.UserSubscriptionCount(">", "s1") }},
		{"UserSubscriptionSubscribeApp", func() { subject.UserSubscriptionSubscribeApp(">", "s1") }},
		{"UserSubscriptionUnsubscribeApp", func() { subject.UserSubscriptionUnsubscribeApp(">", "s1") }},
		{"UserRoomSubscriptionGet", func() { subject.UserRoomSubscriptionGet("*", "s1", "r1") }},
		{"UserAppsList", func() { subject.UserAppsList(">", "s1") }},
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

func TestUserServicePatternBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"status.getByName", subject.UserStatusGetByNamePattern("s1"), "chat.user.{account}.request.user.s1.status.getByName"},
		{"status.set", subject.UserStatusSetPattern("s1"), "chat.user.{account}.request.user.s1.status.set"},
		{"profile.getByName", subject.UserProfileGetByNamePattern("s1"), "chat.user.{account}.request.user.s1.profile.getByName"},
		{"subscription.getCurrent", subject.UserSubscriptionGetCurrentPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getCurrent"},
		{"subscription.getRooms", subject.UserSubscriptionGetRoomsPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getRooms"},
		{"subscription.getChannels", subject.UserSubscriptionGetChannelsPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getChannels"},
		{"subscription.getDM", subject.UserSubscriptionGetDMPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getDM"},
		{"subscription.getApps", subject.UserSubscriptionGetAppsPattern("s1"), "chat.user.{account}.request.user.s1.subscription.getApps"},
		{"subscription.count", subject.UserSubscriptionCountPattern("s1"), "chat.user.{account}.request.user.s1.subscription.count"},
		{"subscription.subscribeApp", subject.UserSubscriptionSubscribeAppPattern("s1"), "chat.user.{account}.request.user.s1.subscription.subscribeApp"},
		{"subscription.unsubscribeApp", subject.UserSubscriptionUnsubscribeAppPattern("s1"), "chat.user.{account}.request.user.s1.subscription.unsubscribeApp"},
		{"room.subscription.get", subject.UserRoomSubscriptionGetPattern("s1"), "chat.user.{account}.request.user.s1.room.{roomID}.subscription.get"},
		{"apps.list", subject.UserAppsListPattern("s1"), "chat.user.{account}.request.user.s1.apps.list"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
