package service_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// degradedReader reports a site whose history is still catching up.
type degradedReader int64

func (d degradedReader) DegradedSince(context.Context, string) (*int64, error) {
	v := int64(d)
	return &v, nil
}

const testDegradedSince = int64(1700000000000)

func withDegraded() service.Option {
	return service.WithDegradation(degradedReader(testDegradedSince), "site-a")
}

// TestEveryHistoryResponseCarriesIncompleteSince is the guard for the class of bug,
// not one instance of it.
//
// incompleteSince was added to the three channel-history reads and missed on the two
// thread reads, which serve the same Cassandra tables written by the same worker on
// the same failing path. A client could therefore be told "still catching up" in the
// channel view and shown a silently short reply list, presented as complete, in a
// thread of that same room — contradictory answers from one service about one site.
//
// Any future response that carries message history belongs in this table, so the
// omission is a failing test rather than a discovery during an incident.
func TestEveryHistoryResponseCarriesIncompleteSince(t *testing.T) {
	historyResponses := []any{
		models.LoadHistoryResponse{},
		models.LoadNextMessagesResponse{},
		models.LoadSurroundingMessagesResponse{},
		models.GetThreadMessagesResponse{},
		models.GetThreadParentMessagesResponse{},
	}

	for _, resp := range historyResponses {
		typ := reflect.TypeOf(resp)
		t.Run(typ.Name(), func(t *testing.T) {
			field, ok := typ.FieldByName("IncompleteSince")
			require.True(t, ok,
				"%s returns message history, so it must be able to say the history is incomplete", typ.Name())
			assert.Equal(t, "*int64", field.Type.String(),
				"pointer, so the field is absent from the JSON on the happy path")
			assert.Equal(t, `incompleteSince,omitempty`, field.Tag.Get("json"))
		})
	}
}

// TestThreadResponses_IncompleteSinceOmittedWhenHealthy holds the additive-wire
// contract: a client that predates the field sees byte-identical responses.
func TestThreadResponses_IncompleteSinceOmittedWhenHealthy(t *testing.T) {
	for _, resp := range []any{
		models.GetThreadMessagesResponse{Messages: []models.Message{}},
		models.GetThreadParentMessagesResponse{ParentMessages: []models.Message{}},
	} {
		body, err := json.Marshal(resp)
		require.NoError(t, err)
		assert.NotContains(t, string(body), "incompleteSince")
	}
}

func TestHistoryService_GetThreadMessages_StampsIncompleteSince(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t, withDegraded())
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute), ThreadRoomID: "tr-1", TCount: intPtr(1)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	require.NotNil(t, resp.IncompleteSince,
		"replies still in the replay backlog read as absent — the client must not be shown that as a complete thread")
	assert.Equal(t, testDegradedSince, *resp.IncompleteSince)
}

func TestHistoryService_GetThreadParentMessages_StampsIncompleteSince(t *testing.T) {
	t.Run("populated page", func(t *testing.T) {
		svc, msgs, subs, _, threadRooms := newService(t, withDegraded())
		c := testContext()

		subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
		threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
		msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(makeCassMessages(), nil)

		resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterAll, Limit: 20})
		require.NoError(t, err)
		require.NotNil(t, resp.IncompleteSince,
			"a parent whose row has not landed is silently skipped from the page while Total still counts it, "+
				"so this list can be short with no other signal that anything is missing")
		assert.Equal(t, testDegradedSince, *resp.IncompleteSince)
	})

	t.Run("empty page", func(t *testing.T) {
		// The early return is the case most likely to be missed and the one a client
		// is most likely to misread: no threads at all, during an outage.
		svc, _, subs, _, threadRooms := newService(t, withDegraded())
		c := testContext()

		subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
		// makeThreadPage's argument is Total, not the row count — this branch needs a
		// page with no rows at all, so no hydration call is made.
		threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).
			Return(mongoutil.OffsetPage[pkgmodel.ThreadRoom]{Data: nil, Total: 0}, nil)

		resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterAll, Limit: 20})
		require.NoError(t, err)
		require.NotNil(t, resp.IncompleteSince)
		assert.Equal(t, testDegradedSince, *resp.IncompleteSince)
	})
}

func TestThreadReads_NoIncompleteSinceWhenHealthy(t *testing.T) {
	svc, msgs, subs, _, threadRooms := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(makeCassMessages(), nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterAll, Limit: 20})
	require.NoError(t, err)
	assert.Nil(t, resp.IncompleteSince)
}
