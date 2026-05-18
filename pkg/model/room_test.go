package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildDMParticipants_SortsByUID(t *testing.T) {
	a := &User{ID: "zzz", Account: "alpha"}
	b := &User{ID: "aaa", Account: "zebra"}
	uids, accounts := BuildDMParticipants(a, b)
	assert.Equal(t, []string{"aaa", "zzz"}, uids)
	assert.Equal(t, []string{"zebra", "alpha"}, accounts, "accounts mirror uid permutation")
}

// Spec §4 F5: non-aligned sort. Users {ID:"zzz", Account:"aaa"} and
// {ID:"aaa", Account:"zzz"}. UIDs sort ascending; Accounts permute to
// preserve the same-user pairing at each index — NOT independently sorted.
func TestBuildDMParticipants_PreservesPairingUnderNonAlignedSort(t *testing.T) {
	user1 := &User{ID: "zzz", Account: "aaa"}
	user2 := &User{ID: "aaa", Account: "zzz"}
	uids, accounts := BuildDMParticipants(user1, user2)
	assert.Equal(t, []string{"aaa", "zzz"}, uids)
	assert.Equal(t, []string{"zzz", "aaa"}, accounts)
}

func TestBuildDMParticipants_AlreadySortedInput(t *testing.T) {
	a := &User{ID: "aaa", Account: "alpha"}
	b := &User{ID: "bbb", Account: "beta"}
	uids, accounts := BuildDMParticipants(a, b)
	assert.Equal(t, []string{"aaa", "bbb"}, uids)
	assert.Equal(t, []string{"alpha", "beta"}, accounts)
}

func TestBuildDMParticipants_SwapInputOrderProducesSameResult(t *testing.T) {
	a := &User{ID: "u1", Account: "alice"}
	b := &User{ID: "u2", Account: "bob"}
	uidsAB, accountsAB := BuildDMParticipants(a, b)
	uidsBA, accountsBA := BuildDMParticipants(b, a)
	assert.Equal(t, uidsAB, uidsBA, "callers passing args in either order must get the same result")
	assert.Equal(t, accountsAB, accountsBA)
}
