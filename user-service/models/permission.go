package models

// PermissionGetRequest is the body of permission.get.
type PermissionGetRequest struct {
	Permission string `json:"permission"`
}

// PermissionGetResponse echoes the requested key and whether the caller
// currently holds it. No dates: keeps user-service free of timezone handling
// (design §6) — chat-frontend is out of scope for this change.
type PermissionGetResponse struct {
	Permission string `json:"permission"`
	Granted    bool   `json:"granted"`
}
