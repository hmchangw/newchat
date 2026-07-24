package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func testConfig() config {
	return config{
		SiteID: "site-a", MsgIndexPrefix: "messages-a-v1", SpotlightIndex: "spotlight-a-v1", UserRoomIndex: "user-room-a",
		WorkerConcurrency: 4,
	}
}

func TestBootstrapPrerequisites_RegistersAllThreeTemplatesAndBothScripts(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockTemplateStore(ctrl)
	cfg := testConfig()

	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.MessageTemplateName(cfg.MsgIndexPrefix), gomock.Any()).Return(nil)
	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.SpotlightTemplateName(cfg.SpotlightIndex), gomock.Any()).Return(nil)
	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.UserRoomTemplateName(cfg.UserRoomIndex), gomock.Any()).Return(nil)
	engine.EXPECT().PutScript(gomock.Any(), searchindex.AddRoomScriptID, gomock.Any()).Return(nil)
	engine.EXPECT().PutScript(gomock.Any(), searchindex.RemoveRoomScriptID, gomock.Any()).Return(nil)

	err := bootstrapPrerequisites(context.Background(), engine, &cfg)

	require.NoError(t, err)
}

func TestBootstrapPrerequisites_TemplateErrorAbortsAndIsWrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockTemplateStore(ctrl)
	cfg := testConfig()

	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.MessageTemplateName(cfg.MsgIndexPrefix), gomock.Any()).
		Return(errors.New("es down"))

	err := bootstrapPrerequisites(context.Background(), engine, &cfg)

	require.Error(t, err)
}

func TestBootstrapPrerequisites_ScriptErrorIsWrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockTemplateStore(ctrl)
	cfg := testConfig()

	engine.EXPECT().UpsertTemplate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
	engine.EXPECT().PutScript(gomock.Any(), searchindex.AddRoomScriptID, gomock.Any()).Return(errors.New("script rejected"))

	err := bootstrapPrerequisites(context.Background(), engine, &cfg)

	require.Error(t, err)
}
