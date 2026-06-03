package model

// UserRole is a platform-level role flag on the User record.
// Empty Roles reads as ["user"]; only positive marker is "admin".
type UserRole string

const (
	UserRoleAdmin UserRole = "admin"
	UserRoleUser  UserRole = "user"
)

type User struct {
	ID          string     `json:"id"           bson:"_id"`
	Account     string     `json:"account"      bson:"account"`
	SiteID      string     `json:"siteId"       bson:"siteId"`
	SectID      string     `json:"sectId"       bson:"sectId"`
	SectName    string     `json:"sectName"     bson:"sectName"`
	SectTCName  string     `json:"sectTCName"   bson:"sectTCName"`
	DeptID      string     `json:"deptId"       bson:"deptId"`
	DeptName    string     `json:"deptName"     bson:"deptName"`
	DeptTCName  string     `json:"deptTCName"   bson:"deptTCName"`
	EngName     string     `json:"engName"      bson:"engName"`
	ChineseName string     `json:"chineseName"  bson:"chineseName"`
	EmployeeID  string     `json:"employeeId"   bson:"employeeId"`
	Roles       []UserRole `json:"roles,omitempty"        bson:"roles,omitempty"`
}

// IsPlatformAdmin reports whether u holds the platform admin role.
// Returns false for nil receivers and for users without the role.
func IsPlatformAdmin(u *User) bool {
	if u == nil {
		return false
	}
	for _, r := range u.Roles {
		if r == UserRoleAdmin {
			return true
		}
	}
	return false
}
