package main

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

func makeSoakUsers(count int, siteID string) []model.User {
	users := make([]model.User, count)
	for i := range users {
		users[i] = model.User{
			ID:      fmt.Sprintf("u-%05d", i),
			Account: fmt.Sprintf("user-%05d", i),
			SiteID:  siteID,
			Roles:   []model.UserRole{model.UserRoleUser},
		}
	}
	return users
}

func cloneSoakUsers(users []model.User) []model.User {
	cloned := make([]model.User, len(users))
	for i := range users {
		cloned[i] = users[i]
		cloned[i].Roles = append([]model.UserRole(nil), users[i].Roles...)
	}
	return cloned
}

func newSequenceSoakIDs() *soakIDs {
	var room, subscription int
	return &soakIDs{
		NewChannelRoomID: func() string {
			room++
			return fmt.Sprintf("channel-%03d", room)
		},
		NewSubscriptionID: func() string {
			subscription++
			return fmt.Sprintf("subscription-%05d", subscription)
		},
	}
}
