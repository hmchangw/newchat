// Chatlist section-definition RPCs (user-service): get + the five section
// mutations. Grouped in one folder because they share the ChatlistState
// response, the mock, and the name-validation surface. Each mutation returns
// the full post-update ChatlistState and fans out chatlist.update.
//
// Every op guards on CHATLIST_MOCK: mock → the in-memory store; live → the
// real NATS subject. The live path is the default.

import { CHATLIST_MOCK } from '@/lib/runtimeConfig'
import {
  chatlistGet,
  chatlistSectionCreate,
  chatlistSectionRename,
  chatlistSectionDelete,
  chatlistSectionReorder,
  chatlistSectionSetSortMode,
} from '../_transport/subjects'
import {
  getChatlistMock,
  createSectionMock,
  renameSectionMock,
  deleteSectionMock,
  reorderSectionsMock,
  setSectionSortModeMock,
} from '../_transport/chatlistMock'
import type { Nats, ChatlistState, ChatlistSortMode } from '../types'

export function getChatlist({ user, request }: Nats): Promise<ChatlistState> {
  if (CHATLIST_MOCK) return Promise.resolve(getChatlistMock())
  return request<ChatlistState>(chatlistGet(user.account, user.siteId))
}

export interface CreateSectionArgs {
  name: string
  sortMode?: ChatlistSortMode
}

export function createChatlistSection(
  { user, request }: Nats,
  { name, sortMode }: CreateSectionArgs,
): Promise<ChatlistState> {
  if (CHATLIST_MOCK) return Promise.resolve(createSectionMock(name, sortMode))
  return request<ChatlistState>(chatlistSectionCreate(user.account, user.siteId), { name, sortMode })
}

export interface RenameSectionArgs {
  sectionId: string
  name: string
}

export function renameChatlistSection(
  { user, request }: Nats,
  { sectionId, name }: RenameSectionArgs,
): Promise<ChatlistState> {
  if (CHATLIST_MOCK) return Promise.resolve(renameSectionMock(sectionId, name))
  return request<ChatlistState>(chatlistSectionRename(user.account, user.siteId), { sectionId, name })
}

export function deleteChatlistSection(
  { user, request }: Nats,
  sectionId: string,
): Promise<ChatlistState> {
  if (CHATLIST_MOCK) return Promise.resolve(deleteSectionMock(sectionId))
  return request<ChatlistState>(chatlistSectionDelete(user.account, user.siteId), { sectionId })
}

export function reorderChatlistSections(
  { user, request }: Nats,
  sectionOrder: string[],
): Promise<ChatlistState> {
  if (CHATLIST_MOCK) return Promise.resolve(reorderSectionsMock(sectionOrder))
  return request<ChatlistState>(chatlistSectionReorder(user.account, user.siteId), { sectionOrder })
}

export interface SetSortModeArgs {
  sectionId: string
  sortMode: ChatlistSortMode
}

export function setChatlistSectionSortMode(
  { user, request }: Nats,
  { sectionId, sortMode }: SetSortModeArgs,
): Promise<ChatlistState> {
  if (CHATLIST_MOCK) return Promise.resolve(setSectionSortModeMock(sectionId, sortMode))
  return request<ChatlistState>(chatlistSectionSetSortMode(user.account, user.siteId), {
    sectionId,
    sortMode,
  })
}
