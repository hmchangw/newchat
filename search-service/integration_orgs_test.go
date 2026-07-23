//go:build integration

package main

// Integration tests for search.orgs (real ES + shared NATS + Valkey).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errtest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Per-test spotlight-org index against shared ES.
type orgsFixture struct {
	clientNATS *nats.Conn
	esURL      string
	orgIndex   string
}

func setupOrgsFixture(t *testing.T) *orgsFixture {
	t.Helper()
	esURL := testutil.Elasticsearch(t)
	orgIndex := testutil.ElasticsearchIndex(t, "spotlightorg")
	putTestSpotlightOrgIndex(t, esURL, orgIndex)

	engine, err := searchengine.New(context.Background(), searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err, "build searchengine for orgs fixture")

	cache := newValkeyCache(valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t)))
	t.Cleanup(func() { testutil.FlushValkey(t) })
	h := newHandler(newESStore(engine, testUserRoomIndex), nil, nil, cache, &handlerConfig{
		SiteID:                  testSiteID,
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		SpotlightOrgReadPattern: orgIndex,
	})
	clientNC := setupRouter(t, testQueueGroupOrgs, h.Register)
	return &orgsFixture{clientNATS: clientNC, esURL: esURL, orgIndex: orgIndex}
}

func putTestSpotlightOrgIndex(t *testing.T, esURL, index string) {
	t.Helper()
	// Mirror the production mapping shape: every org field is search_as_you_type
	// (see search-sync-worker/spotlight_org.go SpotlightOrgIndex).
	prop := map[string]any{"type": "search_as_you_type"}
	body := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
			"refresh_interval":   "1s",
		},
		"mappings": map[string]any{
			"dynamic": false,
			"properties": map[string]any{
				"sectId":          prop,
				"sectTCName":      prop,
				"sectName":        prop,
				"sectDescription": prop,
				"deptId":          prop,
				"deptTCName":      prop,
				"deptName":        prop,
				"deptDescription": prop,
				"divisionId":      prop,
			},
		},
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, esURL+"/"+index, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	require.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
		"create spotlight-org index: status=%d body=%s", resp.StatusCode, b)
}

func TestIntegration_SearchOrgs_HappyPath(t *testing.T) {
	f := setupOrgsFixture(t)

	seedDoc(t, f.esURL, f.orgIndex, "S1", map[string]any{
		"sectId": "S1", "sectName": "engineering-platform", "sectTCName": "平台",
		"deptId": "D1", "deptName": "technology", "deptTCName": "科技", "divisionId": "DIV1",
	})
	seedDoc(t, f.esURL, f.orgIndex, "S2", map[string]any{
		"sectId": "S2", "sectName": "engineering-infra",
		"deptId": "D1", "deptName": "technology", "divisionId": "DIV1",
	})
	seedDoc(t, f.esURL, f.orgIndex, "S3", map[string]any{
		"sectId": "S3", "sectName": "marketing",
		"deptId": "D2", "deptName": "growth", "divisionId": "DIV2",
	})

	reqBytes, err := json.Marshal(model.SearchOrgsRequest{Query: "engineering"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchOrgs("alice", testSiteID), reqBytes, 10*time.Second)
	require.NoError(t, err)

	var resp model.SearchOrgsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Orgs, 2, "both sections matching 'engineering' must be returned")
	byID := map[string]model.SearchOrg{}
	for _, o := range resp.Orgs {
		byID[o.SectID] = o
	}
	require.Contains(t, byID, "S1")
	assert.Equal(t, "engineering-platform", byID["S1"].SectName)
	assert.Equal(t, "平台", byID["S1"].SectTCName)
	assert.Equal(t, "technology", byID["S1"].DeptName)
	require.Contains(t, byID, "S2")
	_, leaked := byID["S3"]
	assert.False(t, leaked, "non-matching section must not appear")
}

func TestIntegration_SearchOrgs_CompanyWideNoAccountScoping(t *testing.T) {
	// Unlike search.rooms, the org index is company-wide: the same result set
	// is returned regardless of which account issues the request.
	f := setupOrgsFixture(t)
	seedDoc(t, f.esURL, f.orgIndex, "S1", map[string]any{
		"sectId": "S1", "sectName": "engineering", "deptId": "D1", "deptName": "technology", "divisionId": "DIV1",
	})

	reqBytes, err := json.Marshal(model.SearchOrgsRequest{Query: "engineering"})
	require.NoError(t, err)

	for _, account := range []string{"alice", "bob", "mallory"} {
		msg, err := f.clientNATS.Request(subject.SearchOrgs(account, testSiteID), reqBytes, 10*time.Second)
		require.NoError(t, err)
		var resp model.SearchOrgsResponse
		require.NoError(t, json.Unmarshal(msg.Data, &resp))
		require.Len(t, resp.Orgs, 1, "every account sees the same company-wide org result")
		assert.Equal(t, "S1", resp.Orgs[0].SectID)
	}
}

func TestIntegration_SearchOrgs_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupOrgsFixture(t)

	reqBytes, err := json.Marshal(model.SearchOrgsRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchOrgs("alice", testSiteID), reqBytes, 5*time.Second)
	require.NoError(t, err)

	errtest.AssertCode(t, msg.Data, errcode.CodeBadRequest)
}
