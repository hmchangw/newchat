package auditarchive

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/hmchangw/chat/pkg/searchindex"
)

// EventDoc is one message event in a daily audit-events index. Everything
// searchable is plaintext metadata; the message body, when present, is only
// in EncBody sealed under the site DEK.
type EventDoc struct {
	Seq             uint64    `json:"seq"                       es:"long"`
	EventType       string    `json:"eventType"                 es:"keyword"`
	EventAt         time.Time `json:"eventAt"                   es:"date"`
	MessageID       string    `json:"messageId"                 es:"keyword"`
	RoomID          string    `json:"roomId"                    es:"keyword"`
	SiteID          string    `json:"siteId"                    es:"keyword"`
	SenderAccount   string    `json:"senderAccount"             es:"keyword"`
	SenderID        string    `json:"senderId"                  es:"keyword"`
	CreatedAt       time.Time `json:"createdAt"                 es:"date"`
	ThreadParentID  string    `json:"threadParentId,omitempty"  es:"keyword"`
	AttachmentCount int       `json:"attachmentCount"           es:"integer"`
	AttachmentTypes []string  `json:"attachmentTypes,omitempty" es:"keyword"`
	ActorAccount    string    `json:"actorAccount,omitempty"    es:"keyword"`
	SegmentKey      string    `json:"segmentKey"                es:"keyword"`
	FrameOffset     int64     `json:"frameOffset"               es:"long"`
	ContentHash     string    `json:"contentHash"               es:"keyword"`
	EncBody         []byte    `json:"encBody,omitempty"         es:"binary"`
}

// MemberDoc is one membership event, one document per affected account, in a
// daily audit-members index.
type MemberDoc struct {
	Seq         uint64    `json:"seq"         es:"long"`
	EventType   string    `json:"eventType"   es:"keyword"`
	EventAt     time.Time `json:"eventAt"     es:"date"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	RoomSiteID  string    `json:"roomSiteId"  es:"keyword"`
	Account     string    `json:"account,omitempty" es:"keyword"`
	RoomType    string    `json:"roomType"    es:"keyword"`
	RoomName    string    `json:"roomName"    es:"keyword"`
	SegmentKey  string    `json:"segmentKey"  es:"keyword"`
	FrameOffset int64     `json:"frameOffset" es:"long"`
	ContentHash string    `json:"contentHash" es:"keyword"`
}

// BlobDoc describes one archived attachment, or why it was not archived.
type BlobDoc struct {
	FileID      string    `json:"fileId"      es:"keyword"`
	MessageID   string    `json:"messageId"   es:"keyword"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	SiteID      string    `json:"siteId"      es:"keyword"`
	FileName    string    `json:"fileName"    es:"keyword"`
	ContentType string    `json:"contentType" es:"keyword"`
	SizeBytes   int64     `json:"sizeBytes"   es:"long"`
	BlobKey     string    `json:"blobKey,omitempty"     es:"keyword"`
	PlainSHA256 string    `json:"plainSha256,omitempty" es:"keyword"`
	ChunkBytes  int       `json:"chunkBytes,omitempty"  es:"integer"`
	Skipped     string    `json:"skipped,omitempty"     es:"keyword"` // "", "size", "missing", "legacy"
	ArchivedAt  time.Time `json:"archivedAt"  es:"date"`
}

// KeyDoc is the site's single wrapped-DEK document.
type KeyDoc struct {
	SiteID     string    `json:"siteId"     es:"keyword"`
	WrappedDek []byte    `json:"wrappedDek" es:"binary"`
	CreatedAt  time.Time `json:"createdAt"  es:"date"`
}

// SetLocation records where the sealed record of this event lives.
func (d *EventDoc) SetLocation(segmentKey string, frameOffset int64) {
	d.SegmentKey, d.FrameOffset = segmentKey, frameOffset
}

// SetLocation records where the sealed record of this event lives.
func (d *MemberDoc) SetLocation(segmentKey string, frameOffset int64) {
	d.SegmentKey, d.FrameOffset = segmentKey, frameOffset
}

func dayIndex(prefix, site string, at time.Time) string {
	return fmt.Sprintf("%s-%s-%s", prefix, site, at.UTC().Format("2006.01.02"))
}

// EventsIndex is the daily events index a message event lands in.
func EventsIndex(site string, at time.Time) string { return dayIndex("audit-events", site, at) }

// MembersIndex is the daily members index a membership event lands in.
func MembersIndex(site string, at time.Time) string { return dayIndex("audit-members", site, at) }

// BlobsIndex holds one document per archived attachment.
func BlobsIndex(site string) string { return "audit-blobs-" + site }

// KeysIndex holds the site's single wrapped-DEK document.
func KeysIndex(site string) string { return "audit-keys-" + site }

// KeyDocID is the only document id in KeysIndex.
const KeyDocID = "current"

// EventDocID is "{site}-{seq}": one immutable document per stream sequence.
func EventDocID(site string, seq uint64) string { return site + "-" + strconv.FormatUint(seq, 10) }

// MemberDocID adds the account index because one INBOX event can name
// several accounts and each gets its own document.
func MemberDocID(site string, seq uint64, i int) string {
	return site + "-" + strconv.FormatUint(seq, 10) + "-" + strconv.Itoa(i)
}

// BlobDocID is "{site}-{fileID}": one document per archived file.
func BlobDocID(site, fileID string) string { return site + "-" + fileID }

// LifecyclePolicyName is the ILM policy every daily index carries.
const LifecyclePolicyName = "audit-archive"

// LifecyclePolicyBody makes each daily index read-only a day after creation
// and deletes it at retention. Body stripping (spec §4) is not an ILM
// action; it is a scheduled reindex owned by the audit-service PR.
func LifecyclePolicyBody(retention string) json.RawMessage {
	body := map[string]any{
		"policy": map[string]any{
			"phases": map[string]any{
				"hot": map[string]any{
					"min_age": "0ms",
					"actions": map[string]any{"set_priority": map[string]any{"priority": 100}},
				},
				"warm": map[string]any{
					"min_age": "1d",
					"actions": map[string]any{
						"readonly":     map[string]any{},
						"set_priority": map[string]any{"priority": 50},
					},
				},
				"delete": map[string]any{
					"min_age": retention,
					"actions": map[string]any{"delete": map[string]any{}},
				},
			},
		},
	}
	b, _ := json.Marshal(body) // error discarded: marshalling a literal map of strings and ints cannot fail
	return b
}

// Template is one composable index template to upsert.
type Template struct {
	Name string
	Body json.RawMessage
}

func templateBody(pattern string, props map[string]any, lifecycle, devMode bool) json.RawMessage {
	settings := map[string]any{
		"number_of_shards":   1,
		"number_of_replicas": 1,
		"refresh_interval":   "5s",
	}
	if devMode {
		settings["number_of_replicas"] = 0
	}
	if lifecycle {
		settings["index.lifecycle.name"] = LifecyclePolicyName
	}
	body := map[string]any{
		"index_patterns": []string{pattern},
		"template": map[string]any{
			"settings": settings,
			"mappings": map[string]any{"dynamic": false, "properties": props},
		},
	}
	b, _ := json.Marshal(body) // error discarded: marshalling a literal map of strings and ints cannot fail
	return b
}

// Templates returns the four index templates for one site. Only the daily
// event and member indices carry the lifecycle policy.
func Templates(site string, devMode bool) []Template {
	return []Template{
		{Name: "audit-events-" + site, Body: templateBody("audit-events-"+site+"-*", searchindex.EsPropertiesFromStruct[EventDoc](), true, devMode)},
		{Name: "audit-members-" + site, Body: templateBody("audit-members-"+site+"-*", searchindex.EsPropertiesFromStruct[MemberDoc](), true, devMode)},
		{Name: "audit-blobs-" + site, Body: templateBody(BlobsIndex(site), searchindex.EsPropertiesFromStruct[BlobDoc](), false, devMode)},
		{Name: "audit-keys-" + site, Body: templateBody(KeysIndex(site), searchindex.EsPropertiesFromStruct[KeyDoc](), false, devMode)},
	}
}
