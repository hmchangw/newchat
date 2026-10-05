//go:build integration

package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMinIO, testutil.EnsureNATS, testutil.EnsureElasticsearch)
}
