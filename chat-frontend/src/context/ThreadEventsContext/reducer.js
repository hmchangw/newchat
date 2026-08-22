import { mergeById } from '@/lib/messageBuffer'

export const initialState = {
  activeParent: null, // { roomId, siteId, messageId, createdAtMs }
  messages: [],
  hasLoadedHistory: false,
  historyLoading: false,
  historyError: null,
  nextCursor: null,
  hasNext: false,
}

function setMessage(messages, messageId, patch) {
  const idx = messages.findIndex((m) => m.id === messageId)
  if (idx < 0) return messages
  const out = [...messages]
  out[idx] = { ...out[idx], ...patch }
  return out
}

function unsetStatus(messages, messageId) {
  const idx = messages.findIndex((m) => m.id === messageId)
  if (idx < 0) return messages
  const next = { ...messages[idx] }
  delete next._status
  const out = [...messages]
  out[idx] = next
  return out
}

export function threadEventsReducer(state, action) {
  switch (action.type) {
    case 'OPEN_THREAD': {
      const p = action.parent
      if (state.activeParent && state.activeParent.messageId === p.messageId) {
        return state
      }
      return {
        ...initialState,
        activeParent: p,
        historyLoading: true,
      }
    }
    case 'CLOSE_THREAD':
      return initialState
    case 'HISTORY_LOADING': {
      if (!state.activeParent || state.activeParent.messageId !== action.parentId) return state
      return { ...state, historyLoading: true }
    }
    case 'HISTORY_LOADED': {
      if (!state.activeParent || state.activeParent.messageId !== action.parentId) return state
      const merged = mergeById(state.messages, action.resp.messages || [])
      return {
        ...state,
        messages: merged,
        hasLoadedHistory: true,
        historyLoading: false,
        historyError: null,
        hasNext: !!action.resp.hasNext,
        nextCursor: action.resp.nextCursor ?? null,
      }
    }
    case 'HISTORY_FAILED': {
      if (!state.activeParent || state.activeParent.messageId !== action.parentId) return state
      return { ...state, historyError: action.error, historyLoading: false }
    }
    case 'REPLY_SENT_LOCAL': {
      const msg = action.message
      if (state.messages.some((m) => m.id === msg.id)) return state
      return { ...state, messages: [...state.messages, msg] }
    }
    case 'THREAD_REPLY_RECEIVED': {
      // Append inbound reply if the open thread matches; dedupe by message id.
      if (!state.activeParent) return state
      if (state.activeParent.messageId !== action.parentId) return state
      const msg = action.message
      if (!msg?.id) return state
      if (state.messages.some((m) => m.id === msg.id)) return state
      return { ...state, messages: [...state.messages, msg] }
    }
    case 'REPLY_SEND_FAILED':
      return { ...state, messages: setMessage(state.messages, action.messageId, { _status: 'failed' }) }
    case 'REPLY_RETRIED':
      return { ...state, messages: unsetStatus(state.messages, action.messageId) }
    case 'REPLY_DISMISSED':
      return { ...state, messages: state.messages.filter((m) => m.id !== action.messageId) }
    case 'REPLY_EDITED_LOCAL': {
      const idx = state.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const updated = { ...state.messages[idx], content: action.content, editedAt: action.editedAt }
      const messages = [...state.messages.slice(0, idx), updated, ...state.messages.slice(idx + 1)]
      return { ...state, messages }
    }
    case 'REPLY_DELETED_LOCAL': {
      const idx = state.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const updated = { ...state.messages[idx], deleted: true }
      const messages = [...state.messages.slice(0, idx), updated, ...state.messages.slice(idx + 1)]
      return { ...state, messages }
    }
    case 'REPLY_EDITED': {
      // Live broadcast edit applied to the open thread, if it matches.
      const idx = state.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const current = state.messages[idx]
      // editedAt is the domain edit time, so it orders edits even though arrival
      // order does not: a redelivered older edit must not overwrite a newer one.
      // Equal timestamps also cover the duplicate a follower gets on both lanes.
      if (current.editedAt && action.editedAt && action.editedAt <= current.editedAt) return state
      if (current.content === action.content && current.editedAt === action.editedAt) return state
      const updated = { ...current, content: action.content, editedAt: action.editedAt }
      const messages = [...state.messages.slice(0, idx), updated, ...state.messages.slice(idx + 1)]
      return { ...state, messages }
    }
    case 'REPLY_DELETED': {
      // Live broadcast delete applied to the open thread, if it matches.
      const idx = state.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const current = state.messages[idx]
      // Idempotent for the same reason as REPLY_EDITED above.
      if (current.deleted) return state
      const updated = { ...current, deleted: true }
      const messages = [...state.messages.slice(0, idx), updated, ...state.messages.slice(idx + 1)]
      return { ...state, messages }
    }
    case 'RESET':
      return initialState
    default:
      return state
  }
}
