package searchengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestIsBulkItemSuccess(t *testing.T) {
	tests := []struct {
		name   string
		action searchengine.ActionType
		result searchengine.BulkResult
		want   bool
	}{
		{"2xx index is success", searchengine.ActionIndex, searchengine.BulkResult{Status: 201}, true},
		{"2xx update is success", searchengine.ActionUpdate, searchengine.BulkResult{Status: 200}, true},
		{"409 index is benign version conflict", searchengine.ActionIndex, searchengine.BulkResult{Status: 409}, true},
		{"409 delete is benign version conflict", searchengine.ActionDelete, searchengine.BulkResult{Status: 409}, true},
		{"409 update is a real conflict", searchengine.ActionUpdate, searchengine.BulkResult{Status: 409}, false},
		{"404 delete with no error type is already-absent doc", searchengine.ActionDelete, searchengine.BulkResult{Status: 404, ErrorType: ""}, true},
		{"404 delete with index_not_found is a real failure", searchengine.ActionDelete, searchengine.BulkResult{Status: 404, ErrorType: "index_not_found_exception"}, false},
		{"404 update with document_missing is benign no-target-yet", searchengine.ActionUpdate, searchengine.BulkResult{Status: 404, ErrorType: "document_missing_exception"}, true},
		{"404 update with index_not_found is a real failure", searchengine.ActionUpdate, searchengine.BulkResult{Status: 404, ErrorType: "index_not_found_exception"}, false},
		{"404 index is always a failure", searchengine.ActionIndex, searchengine.BulkResult{Status: 404}, false},
		{"500 is always a failure", searchengine.ActionIndex, searchengine.BulkResult{Status: 500}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, searchengine.IsBulkItemSuccess(tc.action, tc.result))
		})
	}
}
