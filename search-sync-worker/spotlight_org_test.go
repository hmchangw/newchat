package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestSpotlightOrgCollection_Metadata(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)

	// Consumer name carries the local site (own cursor per fab site);
	// stream + filter subjects target the central site (where hr-syncer
	// publishes).
	assert.Equal(t, "spotlight-org-sync-site-a", coll.ConsumerName())
	assert.Equal(t, "spotlightorg-site-a_template", coll.TemplateName())

	cfg := coll.StreamConfig("site-a")
	assert.Equal(t, "HR_site-central", cfg.Name)
	assert.Equal(t, []string{"chat.hr.site-central.>"}, cfg.Subjects)
	assert.Empty(t, cfg.Sources)

	filters := coll.FilterSubjects("site-a")
	assert.Equal(t, []string{"chat.hr.site-central.employees.upsert"}, filters)

	assert.Nil(t, coll.StoredScripts())
}

func TestSpotlightOrgTemplateBody(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	body := coll.TemplateBody()
	require.NotNil(t, body)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))

	patterns := parsed["index_patterns"].([]any)
	assert.Equal(t, "spotlightorg-site-a-*", patterns[0])

	tmpl := parsed["template"].(map[string]any)
	idx := tmpl["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(3), idx["number_of_shards"])
	assert.Equal(t, float64(1), idx["number_of_replicas"])

	mappings := tmpl["mappings"].(map[string]any)
	assert.Equal(t, false, mappings["dynamic"])
	props := mappings["properties"].(map[string]any)
	for _, f := range []string{
		"sectId", "sectTCName", "sectName", "sectDescription",
		"deptId", "deptTCName", "deptName", "deptDescription", "divisionId",
	} {
		field, ok := props[f].(map[string]any)
		require.True(t, ok, "missing property: %s", f)
		assert.Equal(t, "search_as_you_type", field["type"], "field %s wrong type", f)
		assert.Equal(t, "custom_analyzer", field["analyzer"], "field %s wrong analyzer", f)
	}
}

func TestSpotlightOrgTemplateProperties_MatchesStruct(t *testing.T) {
	props := esPropertiesFromStruct[SpotlightOrgIndex]()

	typ := reflect.TypeOf(SpotlightOrgIndex{})
	esFieldCount := 0
	for i := range typ.NumField() {
		field := typ.Field(i)
		esTag := field.Tag.Get("es")
		if esTag == "" || esTag == "-" {
			continue
		}
		esFieldCount++
		jsonTag := field.Tag.Get("json")
		name, _, _ := strings.Cut(jsonTag, ",")
		_, ok := props[name]
		assert.True(t, ok, "template missing property for field %s (json %s)", field.Name, name)
	}
	assert.Equal(t, esFieldCount, len(props))
	assert.Equal(t, 9, esFieldCount, "SpotlightOrgIndex should expose exactly 9 ES-mapped fields")
}

// hrBatchJSON builds the employees.upsert bare array wire payload from org
// rows (each org becomes one Employee element).
func hrBatchJSON(t *testing.T, orgs []SpotlightOrgIndex) []byte {
	t.Helper()
	employees := make([]model.IEmployeeWithChange, 0, len(orgs))
	for i := range orgs {
		o := &orgs[i]
		employees = append(employees, model.IEmployeeWithChange{
			IEmployee: model.IEmployee{IOrg: model.IOrg{
				SectID: o.SectID, SectTCName: o.SectTCName, SectName: o.SectName,
				SectDescription: o.SectDescription, DeptID: o.DeptID, DeptTCName: o.DeptTCName,
				DeptName: o.DeptName, DeptDescription: o.DeptDescription, DivisionID: o.DivisionID,
			}},
		})
	}
	data, err := json.Marshal(employees)
	require.NoError(t, err)
	return data
}

func TestSpotlightOrg_BuildAction_HappyPath(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	data := hrBatchJSON(t, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Eng", DeptID: "D1", DeptName: "Tech"},
		{SectID: "S2", SectName: "Sales", DeptID: "D2", DeptName: "Biz"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	docIDs := map[string]bool{}
	for _, a := range actions {
		assert.Equal(t, searchengine.ActionUpdate, a.Action)
		assert.Equal(t, "spotlightorg-site-a-v1", a.Index)
		assert.Equal(t, int64(0), a.Version, "ActionUpdate must not use external versioning")
		docIDs[a.DocID] = true

		var body map[string]any
		require.NoError(t, json.Unmarshal(a.Doc, &body))
		assert.Equal(t, true, body["doc_as_upsert"])
		assert.Contains(t, body, "doc")
	}
	assert.True(t, docIDs["S1"])
	assert.True(t, docIDs["S2"])
}

func TestSpotlightOrg_BuildAction_DedupBySectID(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	data := hrBatchJSON(t, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering"},
		{SectID: "S1", SectName: "Engineering Renamed"},
		{SectID: "S2", SectName: "Sales"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	var s1Body map[string]any
	for _, a := range actions {
		if a.DocID == "S1" {
			require.NoError(t, json.Unmarshal(a.Doc, &s1Body))
		}
	}
	require.NotNil(t, s1Body)
	doc := s1Body["doc"].(map[string]any)
	assert.Equal(t, "Engineering Renamed", doc["sectName"], "last-wins on dedup")
}

func TestSpotlightOrg_BuildAction_EmptySectIDsSkipped(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	data := hrBatchJSON(t, []SpotlightOrgIndex{
		{SectID: "", SectName: "no-section"},
		{SectID: "S1", SectName: "Eng"},
		{SectID: "", DeptID: "D9"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, "S1", actions[0].DocID)
}

func TestSpotlightOrg_BuildAction_AllEmptySectIDs(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	data := hrBatchJSON(t, []SpotlightOrgIndex{
		{SectName: "no-section-1"},
		{SectName: "no-section-2"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	assert.Nil(t, actions)
}

func TestSpotlightOrg_BuildAction_EmptyEmployees(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	data := hrBatchJSON(t, []SpotlightOrgIndex{})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	assert.Nil(t, actions)
}

func TestSpotlightOrg_BuildAction_PartialFields(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)
	data := hrBatchJSON(t, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)

	var body map[string]any
	require.NoError(t, json.Unmarshal(actions[0].Doc, &body))
	doc := body["doc"].(map[string]any)

	assert.Equal(t, "S1", doc["sectId"])
	assert.Equal(t, "Engineering", doc["sectName"])
	for _, absent := range []string{
		"sectTCName", "sectDescription",
		"deptId", "deptTCName", "deptName", "deptDescription", "divisionId",
	} {
		_, present := doc[absent]
		assert.False(t, present, "doc must not carry %s when input did not set it", absent)
	}
}

func TestSpotlightOrg_BuildAction_Errors(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a-v1", "site-a", "site-central", false)

	t.Run("malformed json", func(t *testing.T) {
		_, err := coll.BuildAction([]byte("{invalid"))
		assert.Error(t, err)
	})
}
