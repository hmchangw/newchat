package model

// Org is the group-shaped org node an Employee belongs to — a Graph group's
// profile (id, displayName, description) plus a producer-configured type
// (e.g. "group"). Nested under Employee as a single node.
type Org struct {
	ID          string `json:"id"          bson:"id"`
	Description string `json:"description" bson:"description"`
	Name        string `json:"name"        bson:"name"`
	Type        string `json:"type"        bson:"type"`
}

// Employee is the shared HR row and the source of truth a downstream service
// maps into model.User. EngName/ChineseName mirror model.User so the derive is
// lossless. Source tags the origin feed (e.g. "teams") so coexisting producers
// don't quit each other's rows.
type Employee struct {
	EmployeeID  string `json:"employeeId"  bson:"employeeId"`
	Account     string `json:"account"     bson:"account"`
	EngName     string `json:"engName"     bson:"engName"`
	ChineseName string `json:"chineseName" bson:"chineseName"`
	SiteID      string `json:"siteId"      bson:"siteId"`
	Source      string `json:"source"      bson:"source"`
	Org         Org    `json:"org"         bson:"org"`
}

// EmployeeWithChange is one employees.upsert element. Change marks how the row
// moved since the last sync ("created"/"updated") as a plain wire string — the
// diff logic lives in the producer, not here.
type EmployeeWithChange struct {
	Employee
	Change string `json:"change,omitempty"`
}

// EmployeesUpsertBatch is the employees.upsert wire shape.
type EmployeesUpsertBatch struct {
	Timestamp int64                `json:"timestamp"`
	Employees []EmployeeWithChange `json:"employees"`
}

// UserWithChange is one users.upsert element.
type UserWithChange struct {
	User
	Change string `json:"change,omitempty"`
}

// UsersUpsertBatch is the users.upsert wire shape.
type UsersUpsertBatch struct {
	Timestamp int64            `json:"timestamp"`
	Users     []UserWithChange `json:"users"`
}

// HRSyncEmployeeQuitBatch lists departed accounts for one site
// (subject chat.hr.{siteID}.employees.quit).
type HRSyncEmployeeQuitBatch struct {
	Timestamp int64    `json:"timestamp"`
	SiteID    string   `json:"siteId"`
	Accounts  []string `json:"accounts"`
}
