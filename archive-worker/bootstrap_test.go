package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/auditarchive"
)

type fakeStreams struct {
	created   []jetstream.StreamConfig
	existing  map[string]bool
	createErr error
}

func (f *fakeStreams) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy the streamManager interface
	f.created = append(f.created, cfg)
	return nil, f.createErr
}
func (f *fakeStreams) Stream(_ context.Context, name string) (o11ynats.Stream, error) {
	if f.existing[name] {
		return nil, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

func TestBootstrapStreams(t *testing.T) {
	t.Run("enabled creates canonical only and verifies inbox", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true}}
		require.NoError(t, bootstrapStreams(context.Background(), f, "site-a", true))
		require.Len(t, f.created, 1)
		assert.Equal(t, "MESSAGES-CANONICAL-site-a", f.created[0].Name)
		assert.Equal(t, []string{"chat.msg.canonical.site-a.>"}, f.created[0].Subjects)
	})
	t.Run("enabled still fails when inbox is missing", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{}}
		err := bootstrapStreams(context.Background(), f, "site-a", true)
		assert.True(t, errors.Is(err, jetstream.ErrStreamNotFound))
	})
	t.Run("enabled surfaces a create failure", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true}, createErr: errors.New("nats down")}
		assert.Error(t, bootstrapStreams(context.Background(), f, "site-a", true))
	})
	t.Run("disabled verifies both, creates nothing", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true, "MESSAGES-CANONICAL-site-a": true}}
		require.NoError(t, bootstrapStreams(context.Background(), f, "site-a", false))
		assert.Empty(t, f.created)
	})
	t.Run("disabled fails when canonical is missing", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true}}
		assert.Error(t, bootstrapStreams(context.Background(), f, "site-a", false))
	})
}

func TestBootstrapIndex(t *testing.T) {
	ctx := context.Background()

	for _, devMode := range []bool{true, false} {
		t.Run(fmt.Sprintf("policy and templates are sent with their real bodies devMode=%t", devMode), func(t *testing.T) {
			idx := NewMockindexStore(gomock.NewController(t))
			idx.EXPECT().EnsureLifecyclePolicy(ctx, "audit-archive", auditarchive.LifecyclePolicyBody("2555d")).Return(true, nil)
			templates := auditarchive.Templates("site-a", devMode)
			require.Len(t, templates, 4)
			for _, tpl := range templates {
				idx.EXPECT().UpsertTemplate(ctx, tpl.Name, tpl.Body).Return(nil)
			}
			require.NoError(t, bootstrapIndex(ctx, idx, "site-a", "2555d", devMode))
		})
	}

	t.Run("retention is applied to the policy body", func(t *testing.T) {
		idx := NewMockindexStore(gomock.NewController(t))
		idx.EXPECT().EnsureLifecyclePolicy(ctx, "audit-archive", auditarchive.LifecyclePolicyBody("30d")).Return(false, nil)
		idx.EXPECT().UpsertTemplate(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(4)
		require.NoError(t, bootstrapIndex(ctx, idx, "site-a", "30d", false))
	})

	t.Run("policy failure stops before templates", func(t *testing.T) {
		idx := NewMockindexStore(gomock.NewController(t))
		idx.EXPECT().EnsureLifecyclePolicy(ctx, "audit-archive", gomock.Any()).Return(false, errors.New("ilm unavailable"))
		assert.Error(t, bootstrapIndex(ctx, idx, "site-a", "2555d", true))
	})

	t.Run("template failure is returned", func(t *testing.T) {
		idx := NewMockindexStore(gomock.NewController(t))
		idx.EXPECT().EnsureLifecyclePolicy(ctx, "audit-archive", gomock.Any()).Return(false, nil)
		idx.EXPECT().UpsertTemplate(ctx, gomock.Any(), gomock.Any()).Return(errors.New("es down"))
		assert.Error(t, bootstrapIndex(ctx, idx, "site-a", "2555d", true))
	})
}
