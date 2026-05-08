package model

type ErrorResponse struct {
	Error  string `json:"error"`
	Code   string `json:"code,omitempty"`
	RoomID string `json:"roomId,omitempty"`
}
