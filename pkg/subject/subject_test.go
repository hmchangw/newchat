package subject_test

import (
	"testing"

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
		{"RoomsCreate", subject.RoomsCreate("alice"),
			"chat.user.alice.request.rooms.create"},
		{"RoomsList", subject.RoomsList("alice"),
			"chat.user.alice.request.rooms.list"},
		{"RoomsGet", subject.RoomsGet("alice", "r1"),
			"chat.user.alice.request.rooms.get.r1"},
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
		{"MemberList", subject.MemberList("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.list"},
		{"MemberListWildcard", subject.MemberListWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.list"},
		{"OrgMembers", subject.OrgMembers("alice", "sect-eng"),
			"chat.user.alice.request.orgs.sect-eng.members"},
		{"OrgMembersWildcard", subject.OrgMembersWildcard(),
			"chat.user.*.request.orgs.*.members"},
		{"SearchMessages", subject.SearchMessages("alice"),
			"chat.user.alice.request.search.messages"},
		{"SearchRooms", subject.SearchRooms("alice"),
			"chat.user.alice.request.search.rooms"},
		{"SearchMessagesPattern", subject.SearchMessagesPattern(),
			"chat.user.{account}.request.search.messages"},
		{"SearchRoomsPattern", subject.SearchRoomsPattern(),
			"chat.user.{account}.request.search.rooms"},
		{"MsgEditPattern", subject.MsgEditPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.edit"},
		{"MsgDeletePattern", subject.MsgDeletePattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.delete"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}

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
		{"RoomsCreateWild", subject.RoomsCreateWildcard(),
			"chat.user.*.request.rooms.create"},
		{"RoomsListWild", subject.RoomsListWildcard(),
			"chat.user.*.request.rooms.list"},
		{"RoomsGetWild", subject.RoomsGetWildcard(),
			"chat.user.*.request.rooms.get.*"},
		{"MemberAddWild", subject.MemberAddWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.add"},
		{"RoomsInfoBatchSubscribe", subject.RoomsInfoBatchSubscribe("site-a"),
			"chat.server.request.room.site-a.info.batch"},
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
