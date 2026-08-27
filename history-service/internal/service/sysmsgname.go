package service

import (
	"strings"

	"github.com/hmchangw/chat/history-service/internal/models"
)

const (
	// legacyMembersRemovedType is the migrated system-message type (plural
	// "members_"), distinct from the modern model.MessageTypeMemberRemoved.
	// Rows of this type carry a raw account in their text, not in sysMsgData.
	legacyMembersRemovedType = "members_removed"
	// removedFromChannelSuffix must match exactly, trailing period included: a
	// user can type the same sentence without it, and the period plus the type
	// gate is what keeps an ordinary message from being rewritten.
	removedFromChannelSuffix = " has been removed from the channel."
)

// extractRemovedAccount returns the account baked into a legacy members_removed
// row's text. Reports false for every other row, leaving it untouched.
func extractRemovedAccount(m *models.Message) (string, bool) {
	if m == nil || m.Type != legacyMembersRemovedType {
		return "", false
	}
	account, found := strings.CutSuffix(m.Msg, removedFromChannelSuffix)
	if !found || account == "" {
		return "", false
	}
	return account, true
}
