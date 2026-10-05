package auditarchive

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexNames(t *testing.T) {
	at := time.Date(2026, 10, 5, 23, 59, 0, 0, time.FixedZone("x", -3600))
	assert.Equal(t, "audit-events-site-a-2026.10.06", EventsIndex("site-a", at), "UTC day")
	assert.Equal(t, "audit-members-site-a-2026.10.06", MembersIndex("site-a", at))
	assert.Equal(t, "audit-blobs-site-a", BlobsIndex("site-a"))
	assert.Equal(t, "audit-keys-site-a", KeysIndex("site-a"))
	assert.Equal(t, "site-a-77", EventDocID("site-a", 77))
	assert.Equal(t, "site-a-77-2", MemberDocID("site-a", 77, 2))
	assert.Equal(t, "site-a-f1", BlobDocID("site-a", "f1"))
}

func TestTemplates(t *testing.T) {
	tpls := Templates("site-a", true)
	require.Len(t, tpls, 4)
	names := map[string]json.RawMessage{}
	for _, tp := range tpls {
		names[tp.Name] = tp.Body
	}
	for _, name := range []string{"audit-events-site-a", "audit-members-site-a", "audit-blobs-site-a", "audit-keys-site-a"} {
		require.Contains(t, names, name)
	}
	var ev struct {
		IndexPatterns []string `json:"index_patterns"`
		Template      struct {
			Settings map[string]any `json:"settings"`
			Mappings struct {
				Dynamic    bool                      `json:"dynamic"`
				Properties map[string]map[string]any `json:"properties"`
			} `json:"mappings"`
		} `json:"template"`
	}
	require.NoError(t, json.Unmarshal(names["audit-events-site-a"], &ev))
	assert.Equal(t, []string{"audit-events-site-a-*"}, ev.IndexPatterns)
	assert.Equal(t, LifecyclePolicyName, ev.Template.Settings["index.lifecycle.name"])
	assert.False(t, ev.Template.Mappings.Dynamic)
	assert.Equal(t, "binary", ev.Template.Mappings.Properties["encBody"]["type"])
	assert.Equal(t, "long", ev.Template.Mappings.Properties["seq"]["type"])
	assert.Equal(t, float64(0), ev.Template.Settings["number_of_replicas"], "dev mode has no replicas")
	assert.NotContains(t, ev.Template.Mappings.Properties["messageId"], "ignore_above", "ids are bounded")

	// Free-text keywords: an over-long value is stored in _source but not
	// indexed, instead of failing the whole document with a 400.
	freeText := map[string][]string{
		"audit-members-site-a": {"roomName"},
		"audit-blobs-site-a":   {"fileName", "contentType"},
		"audit-events-site-a":  {"attachmentTypes"},
	}
	for tpl, fields := range freeText {
		var body struct {
			Template struct {
				Mappings struct {
					Properties map[string]map[string]any `json:"properties"`
				} `json:"mappings"`
			} `json:"template"`
		}
		require.NoError(t, json.Unmarshal(names[tpl], &body), tpl)
		for _, f := range fields {
			assert.Equal(t, "keyword", body.Template.Mappings.Properties[f]["type"], "%s.%s", tpl, f)
			assert.Equal(t, float64(IgnoreAbove), body.Template.Mappings.Properties[f]["ignore_above"], "%s.%s", tpl, f)
		}
	}

	var keys struct {
		Template struct {
			Settings map[string]any `json:"settings"`
		} `json:"template"`
	}
	require.NoError(t, json.Unmarshal(names["audit-keys-site-a"], &keys))
	assert.NotContains(t, keys.Template.Settings, "index.lifecycle.name", "keys never expire")
}

func TestLifecyclePolicyBody(t *testing.T) {
	var p struct {
		Policy struct {
			Phases map[string]struct {
				MinAge  string         `json:"min_age"`
				Actions map[string]any `json:"actions"`
			} `json:"phases"`
		} `json:"policy"`
	}
	require.NoError(t, json.Unmarshal(LifecyclePolicyBody("2555d"), &p))
	assert.Contains(t, p.Policy.Phases["warm"].Actions, "readonly")
	assert.Equal(t, "1d", p.Policy.Phases["warm"].MinAge)
	assert.Equal(t, "2555d", p.Policy.Phases["delete"].MinAge)
	assert.Contains(t, p.Policy.Phases["delete"].Actions, "delete")
}

func TestSetLocation(t *testing.T) {
	e := &EventDoc{}
	e.SetLocation("k", 12)
	assert.Equal(t, "k", e.SegmentKey)
	assert.Equal(t, int64(12), e.FrameOffset)
	m := &MemberDoc{}
	m.SetLocation("k2", 7)
	assert.Equal(t, "k2", m.SegmentKey)
	assert.Equal(t, int64(7), m.FrameOffset)
}

func TestBlobDoc_PlainDigestField(t *testing.T) {
	b, err := json.Marshal(BlobDoc{FileID: "f1", PlainDigest: "hmac-sha256:ab"})
	require.NoError(t, err)
	assert.Contains(t, string(b), `"plainDigest":"hmac-sha256:ab"`)
	assert.NotContains(t, string(b), "plainSha256")
}
