package main

import (
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// displayName falls back to Account when both name fields are empty.
func displayName(u *model.User) string {
	eng := strings.TrimSpace(u.EngName)
	chinese := strings.TrimSpace(u.ChineseName)
	switch {
	case eng == "" && chinese == "":
		return u.Account
	case eng == "":
		return chinese
	case chinese == "":
		return eng
	case eng == chinese:
		return eng
	default:
		return eng + " " + chinese
	}
}

func quoted(name string) string {
	return "\"" + name + "\""
}

func formatAddedSingle(requester, added *model.User) string {
	return quoted(displayName(requester)) + " added " + quoted(displayName(added)) + " to the channel"
}

func formatAddedMulti(requester *model.User) string {
	return quoted(displayName(requester)) + " added members to the channel"
}

func formatRemovedUser(user *model.User) string {
	return quoted(displayName(user)) + " has been removed from the channel"
}

func formatRemovedOrg(sectName string) string {
	return quoted(sectName) + " has been removed from the channel"
}

func formatLeft(user *model.User) string {
	return quoted(displayName(user)) + " left the channel"
}
