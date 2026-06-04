package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseContainerMap(t *testing.T) {
	m, err := parseContainerMap("0a1b2c3d4e5f:cassandra,deadbeef0000:mongo")
	require.NoError(t, err)
	assert.Equal(t, "0a1b2c3d4e5f", m["cassandra"])
	assert.Equal(t, "deadbeef0000", m["mongo"])
}

func TestParseContainerMap_Empty(t *testing.T) {
	m, err := parseContainerMap("")
	require.NoError(t, err)
	assert.NotNil(t, m) // empty string must yield a non-nil (usable) map
	assert.Empty(t, m)
}

func TestParseContainerMap_Malformed(t *testing.T) {
	_, err := parseContainerMap("noseparator")
	require.Error(t, err)
}

func TestIdentityResolver_Selector(t *testing.T) {
	r := identityResolver{fallback: map[string]string{"cassandra": "0a1b2c3d4e5f"}}
	assert.Equal(t, `container_label_com_docker_compose_service="message-worker"`, r.selector("message-worker"))
	assert.Equal(t, `id=~".*0a1b2c3d4e5f.*"`, r.selector("cassandra"))
}
