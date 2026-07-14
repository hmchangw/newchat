package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPresenceConcreteSubjects(t *testing.T) {
	const account = "u-000007"
	const site = "site-local"

	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.hello",
		presenceHelloSubject(account, site))
	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.ping",
		presencePingSubject(account, site))
	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.activity",
		presenceActivitySubject(account, site))
	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.bye",
		presenceByeSubject(account, site))
}

func TestPresenceConcreteSubjects_NoPlaceholderLeftover(t *testing.T) {
	got := presenceHelloSubject("alice", "s1")
	assert.NotContains(t, got, "{account}")
}
