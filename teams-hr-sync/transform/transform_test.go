package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

var testGroup = msgraph.GroupProfile{ID: "g1", DisplayName: "Engineering", Description: "eng dept"}

func TestDefaultMapper_OrgFromGroup(t *testing.T) {
	got := DefaultMapper{OrgType: "group"}.OrgFromGroup(testGroup)
	assert.Equal(t, model.Org{ID: "g1", Name: "Engineering", Description: "eng dept", Type: "group"}, got)
}

func TestDefaultMapper_EmployeeFromMember(t *testing.T) {
	org := model.Org{ID: "g1", Name: "Engineering", Description: "eng dept", Type: "group"}
	tests := []struct {
		name string
		user msgraph.GraphUser
		want model.Employee
	}{
		{
			name: "full identity mapping",
			user: msgraph.GraphUser{
				ID: "u1", UserPrincipalName: "Alice.Wu@corp.com",
				DisplayName: "愛麗絲", GivenName: "Alice", Surname: "Wu", EmployeeID: "EMP1",
			},
			want: model.Employee{
				EmployeeID: "EMP1", Account: "alice.wu", EngName: "Alice Wu",
				ChineseName: "愛麗絲", SiteID: "site-a", Source: "teams", Org: org,
			},
		},
		{
			name: "surname only trims the joiner space",
			user: msgraph.GraphUser{UserPrincipalName: "bob@corp.com", Surname: "Lin"},
			want: model.Employee{Account: "bob", EngName: "Lin", SiteID: "site-a", Source: "teams", Org: org},
		},
		// empty Account signals unmappable — the caller skips
		{name: "no at sign", user: msgraph.GraphUser{UserPrincipalName: "not-a-upn"}, want: model.Employee{}},
		{name: "empty local part", user: msgraph.GraphUser{UserPrincipalName: "@corp.com"}, want: model.Employee{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, DefaultMapper{OrgType: "group"}.EmployeeFromMember(tt.user, org, "site-a"))
		})
	}
}

func TestDefaultConverter_IdentityFieldsOnly(t *testing.T) {
	e := model.Employee{
		EmployeeID: "EMP1", Account: "alice", EngName: "Alice Wu", ChineseName: "愛麗絲",
		SiteID: "site-a", Source: "teams",
		Org: model.Org{ID: "g1", Name: "Engineering", Type: "group"},
	}
	got := DefaultConverter{}.UserFromEmployee(e)
	assert.Equal(t, model.User{
		Account:     "alice",
		SiteID:      "site-a",
		EngName:     "Alice Wu",
		ChineseName: "愛麗絲",
		EmployeeID:  "EMP1",
	}, got, "identity fields copied; all other User fields zero")
}

func TestDefaultConverter_ZeroEmployee(t *testing.T) {
	assert.Equal(t, model.User{}, DefaultConverter{}.UserFromEmployee(model.Employee{}))
}
