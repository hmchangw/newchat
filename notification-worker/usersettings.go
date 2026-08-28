package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

// notifSettings is the resolved, pointer-free view of the three settings this
// worker gates on. The stored type is all *bool; resolving once at this boundary
// keeps the gate total instead of making every read site re-decide what nil means.
//
// The zero value is exactly pre-enforcement behaviour — not muted, no pierce,
// in-call suppressed. That is what makes fail-open free: a missing user, an unset
// settings sub-document, a Mongo error and the kill switch all converge here.
type notifSettings struct {
	muteAll          bool
	allowPriority    bool
	showInCall       bool
	priorityContacts map[string]struct{}
}

// isPriority reports whether account is one of this recipient's priority contacts.
// Decoded to a set at the snapshot boundary so the gate is O(1) per candidate.
func (n notifSettings) isPriority(account string) bool {
	if account == "" {
		return false
	}
	_, ok := n.priorityContacts[account]
	return ok
}

// noopUserSettings returns an empty map so every recipient takes the zero
// notifSettings. Backs USER_SETTINGS_ENABLED=false, mirroring noopPresenceSnapshotter.
type noopUserSettings struct{}

func (noopUserSettings) Snapshot(context.Context, []string) (map[string]notifSettings, error) {
	return map[string]notifSettings{}, nil
}

// resolveNotifSettings collapses the stored pointer fields into a total value and
// decodes priorityContacts into a set, so the gate never dereferences a nil.
func resolveNotifSettings(s *model.UserSettings) notifSettings {
	if s == nil {
		return notifSettings{}
	}
	ns := notifSettings{
		muteAll:       boolValue(s.MuteAllNotifications),
		allowPriority: boolValue(s.AlwaysAllowPriorityNotifications),
		showInCall:    boolValue(s.ShowNotificationsInCall),
	}
	if len(s.PriorityContacts) > 0 {
		ns.priorityContacts = make(map[string]struct{}, len(s.PriorityContacts))
		for _, a := range s.PriorityContacts {
			if a != "" {
				ns.priorityContacts[a] = struct{}{}
			}
		}
	}
	return ns
}

// boolValue treats an unset pointer as false — an absent setting means the user
// never enabled it.
func boolValue(p *bool) bool { return p != nil && *p }
