package model

// Org is the org-hierarchy node an Employee belongs to — the nine
// section/department/division fields, tags identical to search-sync-worker's
// SpotlightOrgIndex so the two share one wire shape. Embedded inline in
// Employee (fields serialize flat at top level).
type Org struct {
	SectID          string `json:"sectId,omitempty"          bson:"sectId,omitempty"`
	SectTCName      string `json:"sectTCName,omitempty"      bson:"sectTCName,omitempty"`
	SectName        string `json:"sectName,omitempty"        bson:"sectName,omitempty"`
	SectDescription string `json:"sectDescription,omitempty" bson:"sectDescription,omitempty"`
	DeptID          string `json:"deptId,omitempty"          bson:"deptId,omitempty"`
	DeptTCName      string `json:"deptTCName,omitempty"      bson:"deptTCName,omitempty"`
	DeptName        string `json:"deptName,omitempty"        bson:"deptName,omitempty"`
	DeptDescription string `json:"deptDescription,omitempty" bson:"deptDescription,omitempty"`
	DivisionID      string `json:"divisionId,omitempty"      bson:"divisionId,omitempty"`
}

// ChangeType marks how a row moved since the last sync.
type ChangeType string

const (
	ChangeTypeNewHire ChangeType = "new_hire"
	ChangeTypeUpdate  ChangeType = "update"
)

// Employee is the shared HR row and the source of truth a downstream service
// maps into model.User. Org embeds inline so the nine org fields sit flat
// alongside the identity fields — one row feeds both the ES org index and
// the hr_employee store. Source tags the origin feed (e.g. "teams") so
// coexisting producers don't quit each other's rows.
type Employee struct {
	Org         `bson:",inline"`
	EmployeeID  string `json:"employeeId"  bson:"employeeId"`
	Account     string `json:"account"     bson:"account"`
	EngName     string `json:"engName"     bson:"engName"`
	ChineseName string `json:"chineseName" bson:"chineseName"`
	SiteID      string `json:"siteId"      bson:"siteId"`
	Source      string `json:"source"      bson:"source"`
}

// EmployeeWithChange is one employees.upsert element (published as a bare
// array, no wrapper).
type EmployeeWithChange struct {
	Employee
	ChangeType ChangeType `json:"changeType,omitempty"`
}

// UserWithChange is one users.upsert element (bare array).
type UserWithChange struct {
	User
	ChangeType ChangeType `json:"changeType,omitempty"`
}

// HRSyncEmployeeQuitBatch lists departed accounts for one site
// (subject chat.hr.{siteID}.employees.quit). Quit stays a wrapper; only the
// two upserts go bare.
type HRSyncEmployeeQuitBatch struct {
	Timestamp int64    `json:"timestamp"`
	SiteID    string   `json:"siteId"`
	Accounts  []string `json:"accounts"`
}
