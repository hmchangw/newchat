package main

import (
	"os"
	"reflect"
	"regexp"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A knob the chart never sets is a knob that silently keeps its default. That
// is how six lane rates were once configured, reported as the run's target, and
// never actually sent. This pins the configmap and soakConfig to each other in
// both directions so neither can drift alone.

const soakConfigMapPath = "deploy/k8s/templates/configmap.yaml"

var soakConfigMapKeyPattern = regexp.MustCompile(`(?m)^\s{2}(SOAK_[A-Z0-9_]+):`)

func soakConfigEnvNames(t *testing.T) []string {
	t.Helper()
	fields := reflect.VisibleFields(reflect.TypeOf(soakConfig{}))
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		if name, ok := field.Tag.Lookup("env"); ok {
			names = append(names, "SOAK_"+name)
		}
	}
	sort.Strings(names)
	return names
}

func soakConfigMapKeys(t *testing.T) []string {
	t.Helper()
	source, err := os.ReadFile(soakConfigMapPath)
	require.NoError(t, err)
	matches := soakConfigMapKeyPattern.FindAllStringSubmatch(string(source), -1)
	require.NotEmpty(t, matches, "found no SOAK_ keys; the scan is broken, not the chart")
	keys := make([]string, 0, len(matches))
	for _, match := range matches {
		keys = append(keys, match[1])
	}
	sort.Strings(keys)
	return keys
}

func TestSoakConfigMap_SetsEveryConfiguredKnobAndNoOthers(t *testing.T) {
	inChart := make(map[string]bool)
	for _, key := range soakConfigMapKeys(t) {
		inChart[key] = true
	}
	inConfig := make(map[string]bool)
	for _, name := range soakConfigEnvNames(t) {
		inConfig[name] = true
	}

	for name := range inConfig {
		assert.True(t, inChart[name],
			"%s is read by soakConfig but never set by the chart, so a deploy "+
				"silently keeps its default", name)
	}
	for key := range inChart {
		assert.True(t, inConfig[key],
			"%s is set by the chart but read by nothing; either it is a typo or "+
				"the config field was removed", key)
	}
}
