package model

import "time"

// AvatarSubjectType discriminates what an Avatar document portrays.
type AvatarSubjectType string

const (
	AvatarSubjectRoom AvatarSubjectType = "room"
	AvatarSubjectBot  AvatarSubjectType = "bot"
)

// Avatar is a custom (uploaded or migrated) avatar for a room or bot, stored in
// the avatars collection. Presence of a document means the subject has a custom
// image in MinIO; absence means the service serves a generated default.
// The collection is cluster-local, so no siteId is stored.
type Avatar struct {
	ID          string            `json:"id"          bson:"_id"`
	SubjectType AvatarSubjectType `json:"subjectType" bson:"subjectType"`
	// SubjectID is the id the service looks the subject up by:
	//   room → roomID;  bot → bot account (".bot" / "p_…").
	SubjectID   string    `json:"subjectId"   bson:"subjectId"`
	MinioKey    string    `json:"minioKey"    bson:"minioKey"`
	ContentType string    `json:"contentType" bson:"contentType"`
	Size        int64     `json:"size"        bson:"size"`
	ETag        string    `json:"etag"        bson:"etag"`
	CreatedAt   time.Time `json:"createdAt"   bson:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"   bson:"updatedAt"`
}
