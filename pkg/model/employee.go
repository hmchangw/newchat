package model

// Org is the shared structured org hierarchy — the same nine fields (and
// identical json tags) as search-sync-worker's SpotlightOrgIndex, so one wire
// row feeds both the ES org index and the hr_employee store.
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

// Employee is the shared HR row and the source of truth a downstream service
// maps into model.User. Org embeds inline so the org fields stay flat on the
// wire (the search-sync consumer decodes that flat subset). EngName/ChineseName
// mirror model.User so the derive is lossless. Source tags the origin feed
// (e.g. "teams") so coexisting producers don't quit each other's rows.
type Employee struct {
	Org         `bson:",inline"`
	EmployeeID  string `json:"employeeId,omitempty"  bson:"employeeId,omitempty"`
	Account     string `json:"account,omitempty"     bson:"account,omitempty"`
	EngName     string `json:"engName,omitempty"     bson:"engName,omitempty"`
	ChineseName string `json:"chineseName,omitempty" bson:"chineseName,omitempty"`
	SiteID      string `json:"siteId,omitempty"      bson:"siteId,omitempty"`
	Source      string `json:"source,omitempty"      bson:"source,omitempty"`
}

// EmployeeWithChange is one employees.upsert element; Employee embeds anonymously
// so the org fields stay flat. Change marks how the row moved since the last sync
// ("created"/"updated") as a plain wire string — the typed enum + diff logic live
// in the producer's transform layer, not here.
type EmployeeWithChange struct {
	Employee
	Change string `json:"change,omitempty"`
}

// EmployeesUpsertBatch matches the search-sync-worker consumer's wire shape.
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
