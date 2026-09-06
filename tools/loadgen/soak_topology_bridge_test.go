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
