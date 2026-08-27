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

func TestIsBulkItemBackpressure(t *testing.T) {
	tests := []struct {
		name   string
		result searchengine.BulkResult
		want   bool
	}{
		{"429 is backpressure", searchengine.BulkResult{Status: 429}, true},
		{"rejected execution is backpressure", searchengine.BulkResult{Status: 429, ErrorType: "es_rejected_execution_exception"}, true},
		{"circuit breaker is backpressure", searchengine.BulkResult{Status: 429, ErrorType: "circuit_breaking_exception"}, true},
		{"circuit breaker under a 503 is still backpressure", searchengine.BulkResult{Status: 503, ErrorType: "circuit_breaking_exception"}, true},
		{"malformed doc is not backpressure", searchengine.BulkResult{Status: 400, ErrorType: "mapper_parsing_exception"}, false},
		{"missing index is not backpressure", searchengine.BulkResult{Status: 404, ErrorType: "index_not_found_exception"}, false},
		{"success is not backpressure", searchengine.BulkResult{Status: 201}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, searchengine.IsBulkItemBackpressure(tc.result))
		})
	}
}

func TestIsBulkItemPermanent(t *testing.T) {
	tests := []struct {
		name   string
		result searchengine.BulkResult
		want   bool
	}{
		{"malformed document is permanent", searchengine.BulkResult{Status: 400, ErrorType: "mapper_parsing_exception"}, true},
		{"illegal argument is permanent", searchengine.BulkResult{Status: 400, ErrorType: "illegal_argument_exception"}, true},
		{"backpressure is retryable", searchengine.BulkResult{Status: 429, ErrorType: "es_rejected_execution_exception"}, false},
		{"update conflict is retryable", searchengine.BulkResult{Status: 409}, false},
		{"missing index can clear later", searchengine.BulkResult{Status: 404, ErrorType: "index_not_found_exception"}, false},
		{"server error is retryable", searchengine.BulkResult{Status: 500}, false},
		{"success is not permanent", searchengine.BulkResult{Status: 201}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, searchengine.IsBulkItemPermanent(tc.result))
		})
	}
}
