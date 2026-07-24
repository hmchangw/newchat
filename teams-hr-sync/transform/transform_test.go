package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

var testGroup = msgraph.GroupProfile{ID: "g1", DisplayName: "Engineering", Description: "eng dept"}

func TestDefaultMapper_EmployeeFromMember(t *testing.T) {
	// testGroup maps to this org node, derived inside EmployeeFromMember.
	org := model.IOrg{SectID: "g1", SectName: "Engineering", SectDescription: "eng dept"}
	tests := []struct {
		name string
		user msgraph.GraphUser
		want *model.IEmployee
	}{
		{
			name: "full identity mapping",
			user: msgraph.GraphUser{
				ID: "u1", UserPrincipalName: "Alice.Wu@corp.com",
				DisplayName: "愛麗絲", GivenName: "Alice", Surname: "Wu", EmployeeID: "EMP1",
				Mail: "alice.wu@corp.com", MailNickname: "alice.wu", UserType: "Member", AccountEnabled: true,
			},
			// EmployeeID derived from the Graph id (u1), not the AAD EMP1 attribute;
			// mail/userType/accountEnabled carried through.
			want: &model.IEmployee{
				ID: EmployeeIDFromGraphID("u1"), EmployeeID: EmployeeIDFromGraphID("u1"),
				Account: "alice.wu", EngName: "Alice Wu",
				ChineseName: "愛麗絲", Mail: "alice.wu@corp.com", MailNickname: "alice.wu",
				UserType: "Member", AccountEnabled: true,
				SiteID: "site-a", IOrg: org,
			},
		},
		{
			name: "surname only trims the joiner space",
			user: msgraph.GraphUser{ID: "u2", UserPrincipalName: "bob@corp.com", Surname: "Lin"},
			want: &model.IEmployee{ID: EmployeeIDFromGraphID("u2"), EmployeeID: EmployeeIDFromGraphID("u2"), Account: "bob", EngName: "Lin", SiteID: "site-a", IOrg: org},
		},
		// no usable UPN or Graph id signals unmappable → nil, the caller skips
		{name: "no at sign", user: msgraph.GraphUser{ID: "u3", UserPrincipalName: "not-a-upn"}, want: nil},
		{name: "empty local part", user: msgraph.GraphUser{ID: "u4", UserPrincipalName: "@corp.com"}, want: nil},
		{name: "no graph id", user: msgraph.GraphUser{UserPrincipalName: "carol@corp.com"}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, DefaultMapper{}.EmployeeFromMember(&tt.user, testGroup, "site-a"))
		})
	}
}

func TestEmployeeIDFromGraphID(t *testing.T) {
	got := EmployeeIDFromGraphID("u1")
	// Deterministic 24-hex bson.ObjectID; same id → same key on every sync.
	assert.Len(t, got, 24)
	assert.Equal(t, got, EmployeeIDFromGraphID("u1"), "stable for the same Graph id")
	assert.NotEqual(t, got, EmployeeIDFromGraphID("u2"), "distinct ids → distinct employeeIds")
	_, err := bson.ObjectIDFromHex(got)
	assert.NoError(t, err, "must be a valid bson.ObjectID")
}

func TestDefaultConverter_IdentityFieldsOnly(t *testing.T) {
	e := model.IEmployee{
		EmployeeID: "EMP1", Account: "alice", EngName: "Alice Wu", ChineseName: "愛麗絲",
		SiteID: "site-a",
		IOrg:   model.IOrg{SectID: "g1", SectName: "Engineering"},
	}
	got := DefaultConverter{}.UserFromEmployee(&e)
	assert.Equal(t, model.User{
		Account:     "alice",
		SiteID:      "site-a",
		EngName:     "Alice Wu",
		ChineseName: "愛麗絲",
		EmployeeID:  "EMP1",
	}, got, "identity fields copied; all other User fields zero")
}

func TestDefaultConverter_ZeroEmployee(t *testing.T) {
	assert.Equal(t, model.User{}, DefaultConverter{}.UserFromEmployee(&model.IEmployee{}))
}
