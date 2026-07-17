// Package transform derives model.User rows from HR Employee rows for the
// users.upsert feed. One-way: Employee is the source of truth.
package transform

import "github.com/hmchangw/chat/pkg/model"

// EmployeeUserConverter maps an Employee to the User the users.upsert feed
// publishes.
type EmployeeUserConverter interface {
	UserFromEmployee(model.Employee) model.User
}

// DefaultConverter copies the identity fields only; every other User field
// stays zero — the downstream persister owns defaults/merging.
type DefaultConverter struct{}

func (DefaultConverter) UserFromEmployee(e model.Employee) model.User {
	return model.User{
		Account:     e.Account,
		SiteID:      e.SiteID,
		EngName:     e.EngName,
		ChineseName: e.ChineseName,
		EmployeeID:  e.EmployeeID,
	}
}
