package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

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
