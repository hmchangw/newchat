import { describe, it, expect } from 'vitest'
import {
  userRoomEvent,
  userRoomKey,
  roomEvent,
  memberAdd,
  memberRemove,
  memberRoleUpdate,
  memberList,
  roomKeyGet,
  searchRooms,
  searchMessages,
  msgSurrounding,
  msgThread,
  msgEdit,
  msgDelete,
  readReceipt,
  messageRead,
  roomCreate,
  userResponse,
  orgMembers,
  userSubscriptionList,
  userSubscriptionCount,
} from './subjects'

describe('subjects', () => {
  it('userRoomEvent builds the per-user room event subject', () => {
    expect(userRoomEvent('alice')).toBe('chat.user.alice.event.room')
  })

  it('roomEvent still builds the per-room subject', () => {
    expect(roomEvent('r1')).toBe('chat.room.r1.event')
  })

  it('memberAdd builds the add-member request subject', () => {
    expect(memberAdd('alice', 'r1', 'site-A')).toBe(
      'chat.user.alice.request.room.r1.site-A.member.add'
    )
  })

  it('memberRemove builds the remove-member request subject', () => {
    expect(memberRemove('alice', 'r1', 'site-A')).toBe(
      'chat.user.alice.request.room.r1.site-A.member.remove'
    )
  })

  it('memberRoleUpdate builds the role-update request subject', () => {
    expect(memberRoleUpdate('alice', 'r1', 'site-A')).toBe(
      'chat.user.alice.request.room.r1.site-A.member.role-update'
    )
  })

  it('searchRooms builds the search rooms request subject', () => {
    expect(searchRooms('alice')).toBe('chat.user.alice.request.search.rooms')
  })

  it('searchMessages builds the search messages request subject', () => {
    expect(searchMessages('alice')).toBe('chat.user.alice.request.search.messages')
  })

  it('msgSurrounding builds the surrounding-messages request subject', () => {
    expect(msgSurrounding('alice', 'r1', 'site-A')).toBe(
      'chat.user.alice.request.room.r1.site-A.msg.surrounding'
    )
  })

  it('msgThread builds the thread RPC subject', () => {
    expect(msgThread('alice', 'r1', 'site-1')).toBe(
      'chat.user.alice.request.room.r1.site-1.msg.thread'
    )
  })

  it('msgEdit builds the edit RPC subject', () => {
    expect(msgEdit('alice', 'r1', 'site-1')).toBe(
      'chat.user.alice.request.room.r1.site-1.msg.edit'
    )
  })

  it('msgDelete builds the delete RPC subject', () => {
    expect(msgDelete('alice', 'r1', 'site-1')).toBe(
      'chat.user.alice.request.room.r1.site-1.msg.delete'
    )
  })

  it('readReceipt builds the request subject for the read-receipt RPC', () => {
    expect(readReceipt('alice', 'room1', 'site1')).toBe(
      'chat.user.alice.request.room.room1.site1.message.read-receipt'
    )
  })

  it('messageRead builds the mark-room-read RPC subject', () => {
    expect(messageRead('alice', 'room1', 'site1')).toBe(
      'chat.user.alice.request.room.room1.site1.message.read'
    )
  })

  it('roomCreate builds the create-room request subject scoped to the requester site', () => {
    expect(roomCreate('alice', 'site-A')).toBe('chat.user.alice.request.room.site-A.create')
  })

  it('memberList builds the list-members request subject', () => {
    expect(memberList('alice', 'r1', 'site-A')).toBe(
      'chat.user.alice.request.room.r1.site-A.member.list'
    )
  })

  it('roomKeyGet builds the per-room key-get request subject', () => {
    expect(roomKeyGet('alice', 'r1', 'site-A')).toBe(
      'chat.user.alice.request.room.r1.site-A.key.get'
    )
  })

  it('userResponse builds the per-request async-result subject', () => {
    expect(userResponse('alice', 'req-123')).toBe('chat.user.alice.response.req-123')
  })

  it('orgMembers builds the list-org-members request subject', () => {
    expect(orgMembers('alice', 'sect-eng')).toBe('chat.user.alice.request.orgs.sect-eng.members')
  })

  it('userSubscriptionList builds the user-service subscription.list subject', () => {
    expect(userSubscriptionList('alice', 'site-A')).toBe(
      'chat.user.alice.request.user.site-A.subscription.list'
    )
  })

  it('userSubscriptionCount builds the user-service subscription-count subject', () => {
    expect(userSubscriptionCount('alice', 'site-A')).toBe(
      'chat.user.alice.request.user.site-A.subscription.count'
    )
  })
})

describe('userRoomKey', () => {
  it('builds the per-user room-key event subject', () => {
    expect(userRoomKey('alice')).toBe('chat.user.alice.event.room.key')
  })
})

