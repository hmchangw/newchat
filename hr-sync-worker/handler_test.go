package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
)

const (
	subjEmployees = "chat.hr.site-a.employees.upsert"
	subjUsers     = "chat.hr.site-a.users.upsert"
	subjQuit      = "chat.hr.site-a.employees.quit"
)

func newMock(t *testing.T) (*MockStore, *Handler) {
	t.Helper()
	store := NewMockStore(gomock.NewController(t))
	return store, NewHandler(store)
}

func TestHandleMessage_EmployeesUpsert(t *testing.T) {
	store, h := newMock(t)
	store.EXPECT().UpsertEmployees(gomock.Any(), gomock.Len(1)).Return(nil)
	err := h.HandleMessage(context.Background(), subjEmployees,
		[]byte(`{"timestamp":1,"employees":[{"account":"alice","source":"teams","change":"created"}]}`))
	require.NoError(t, err)
}

func TestHandleMessage_UsersUpsert(t *testing.T) {
	store, h := newMock(t)
	store.EXPECT().UpsertUserIdentities(gomock.Any(), gomock.Len(1)).Return(nil)
	err := h.HandleMessage(context.Background(), subjUsers,
		[]byte(`{"timestamp":1,"users":[{"account":"alice","siteId":"site-a","change":"created"}]}`))
	require.NoError(t, err)
}

func TestHandleMessage_Quit(t *testing.T) {
	store, h := newMock(t)
	store.EXPECT().QuitTeamsEmployees(gomock.Any(), []string{"alice", "bob"}).Return(nil)
	err := h.HandleMessage(context.Background(), subjQuit,
		[]byte(`{"timestamp":1,"siteId":"site-a","accounts":["alice","bob"]}`))
	require.NoError(t, err)
}

func TestHandleMessage_MalformedIsPermanent(t *testing.T) {
	for _, subj := range []string{subjEmployees, subjUsers, subjQuit} {
		t.Run(subj, func(t *testing.T) {
			_, h := newMock(t) // store must not be called
			err := h.HandleMessage(context.Background(), subj, []byte(`{not json`))
			var perm *errcode.PermanentError
			require.ErrorAs(t, err, &perm, "malformed payload must Ack-drop, not retry")
		})
	}
}

func TestHandleMessage_UnknownSubjectIsPermanent(t *testing.T) {
	_, h := newMock(t)
	err := h.HandleMessage(context.Background(), "chat.hr.site-a.something.else", []byte(`{}`))
	var perm *errcode.PermanentError
	require.ErrorAs(t, err, &perm)
}

func TestHandleMessage_EmptyBatchesNoStoreCall(t *testing.T) {
	_, h := newMock(t) // no EXPECT — any store call fails the test
	assert.NoError(t, h.HandleMessage(context.Background(), subjEmployees, []byte(`{"timestamp":1,"employees":[]}`)))
	assert.NoError(t, h.HandleMessage(context.Background(), subjUsers, []byte(`{"timestamp":1,"users":[]}`)))
	assert.NoError(t, h.HandleMessage(context.Background(), subjQuit, []byte(`{"timestamp":1,"siteId":"s","accounts":[]}`)))
}

func TestHandleMessage_StoreErrorIsTransient(t *testing.T) {
	store, h := newMock(t)
	boom := errors.New("mongo down")
	store.EXPECT().UpsertEmployees(gomock.Any(), gomock.Any()).Return(boom)
	err := h.HandleMessage(context.Background(), subjEmployees,
		[]byte(`{"timestamp":1,"employees":[{"account":"alice"}]}`))
	require.ErrorIs(t, err, boom)
	var perm *errcode.PermanentError
	assert.False(t, errors.As(err, &perm), "store failures must Nak-retry, not Ack-drop")
}
