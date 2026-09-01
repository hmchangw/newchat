import { useState } from 'react'
import { listUsers } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import { useLatestRequest } from '@/hooks/useLatestRequest'
import './style.css'

export const SEARCH_LIMIT = 10

// Search-to-select account field: typing queries the admin users API (debounced) and only an
// account present in the results can be selected — free-typed text is never committed via
// `onChange`, so it can never reach a submit payload. `multiple` renders removable chips backed
// by an array `value`; single mode holds one account string and hides the input once set.
// `searchAccounts(q)` overrides where candidates come from — pass one to scope the picker to a
// narrower population (e.g. a single room's members) instead of every user on the platform.
export default function AccountPicker({
  id,
  label,
  authToken,
  multiple = false,
  value,
  onChange,
  disabled,
  searchAccounts,
}) {
  const [options, setOptions] = useState([])
  const [open, setOpen] = useState(false)
  // Index into `options`, not a selected account: this is the ARIA-1.2 combobox active
  // descendant, so focus stays on the input and only `aria-activedescendant` moves.
  // Reset to 0 wherever `options` is replaced, so it can never dangle past the end.
  const [highlight, setHighlight] = useState(0)
  const handleAdminError = useHandleAdminError()
  const { begin, isCurrent } = useLatestRequest()

  const selected = multiple ? value : value ? [value] : []

  const search = async (q) => {
    // Stamp every invocation — including the empty-query short-circuit below — so a
    // still-in-flight older request (e.g. "al" resolving after "ali", or after the field was
    // cleared) can never win a race and overwrite fresher/cleared state (see
    // useLatestRequest's own doc comment).
    const token = begin()
    if (!q) {
      setOptions([])
      setOpen(false)
      return
    }
    try {
      const found = searchAccounts
        ? await searchAccounts(q)
        : (await listUsers(authToken, { q, page: 1, limit: SEARCH_LIMIT })).users
      if (!isCurrent(token)) return // superseded by a newer query
      setOptions(found.filter((u) => !selected.includes(u.account)))
      setHighlight(0)
      setOpen(true)
    } catch (err) {
      if (!isCurrent(token)) return
      setOptions([])
      setOpen(false)
      handleAdminError(err) // invalid_token logs out; other errors just leave the dropdown empty
    }
  }

  const { query, setQuery } = useDebouncedSearch({ onSearch: search })

  const select = (account) => {
    if (multiple) {
      if (!value.includes(account)) onChange([...value, account])
      // Keep the dropdown open (minus the just-picked match) so several accounts from the
      // same search can be added in a row without re-typing the query each time.
      setOptions((prev) => prev.filter((u) => u.account !== account))
      setHighlight(0)
    } else {
      onChange(account)
      setQuery('')
      setOptions([])
      setOpen(false)
    }
  }

  const optionId = (i) => `${id}-option-${i}`
  const listOpen = open && options.length > 0

  const onKeyDown = (e) => {
    if (e.key === 'Escape') {
      setOpen(false)
      return
    }
    if (!['ArrowDown', 'ArrowUp', 'Enter'].includes(e.key)) return
    if (!listOpen) {
      // ArrowDown re-opens a list closed by Escape; without it the only way back is
      // re-typing the query. Enter/ArrowUp on a closed list fall through to the form.
      if (e.key === 'ArrowDown' && options.length > 0) {
        e.preventDefault()
        setOpen(true)
      }
      return
    }
    // preventDefault on all three: the arrows would otherwise jump the caret to the
    // start/end of the input, and Enter would submit the form this picker sits in.
    e.preventDefault()
    if (e.key === 'Enter') select(options[highlight].account)
    else setHighlight((i) => (i + (e.key === 'ArrowDown' ? 1 : options.length - 1)) % options.length)
  }

  const remove = (account) => {
    onChange(multiple ? value.filter((a) => a !== account) : '')
  }

  const showInput = multiple || selected.length === 0

  return (
    <div className="account-picker">
      <label htmlFor={id}>{label}</label>
      {selected.length > 0 && (
        <ul className="account-picker-chips">
          {selected.map((account) => (
            <li key={account} className="account-picker-chip">
              <span>{account}</span>
              <button
                type="button"
                aria-label={`Remove ${account}`}
                onClick={() => remove(account)}
                disabled={disabled}
              >
                ×
              </button>
            </li>
          ))}
        </ul>
      )}
      {showInput && (
        <div className="account-picker-input-wrap">
          <input
            id={id}
            type="text"
            autoComplete="off"
            role="combobox"
            aria-expanded={listOpen}
            aria-controls={`${id}-listbox`}
            aria-activedescendant={listOpen ? optionId(highlight) : undefined}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => options.length > 0 && setOpen(true)}
            onBlur={() => setOpen(false)}
            onKeyDown={onKeyDown}
            disabled={disabled}
          />
          {listOpen && (
            <ul
              id={`${id}-listbox`}
              className="account-picker-options"
              role="listbox"
              // Keeps focus on the input across the click so `onBlur` above doesn't close the
              // dropdown before the option's own onClick has a chance to run.
              onMouseDown={(e) => e.preventDefault()}
            >
              {options.map((u, i) => (
                <li
                  key={u.account}
                  id={optionId(i)}
                  role="option"
                  aria-selected={i === highlight}
                  onClick={() => select(u.account)}
                >
                  {u.account}
                  {u.engName ? ` (${u.engName})` : ''}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}
