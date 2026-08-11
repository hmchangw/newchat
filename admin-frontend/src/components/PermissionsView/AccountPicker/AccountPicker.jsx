import { useState } from 'react'
import { listUsers } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import { useLatestRequest } from '@/hooks/useLatestRequest'
import './style.css'

const SEARCH_LIMIT = 10

// Search-to-select account field: typing queries the admin users API (debounced) and only an
// account present in the results can be selected — free-typed text is never committed via
// `onChange`, so it can never reach a submit payload. `multiple` renders removable chips backed
// by an array `value`; single mode holds one account string and hides the input once set.
export default function AccountPicker({ id, label, authToken, multiple = false, value, onChange, disabled }) {
  const [options, setOptions] = useState([])
  const [open, setOpen] = useState(false)
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
      const result = await listUsers(authToken, { q, page: 1, limit: SEARCH_LIMIT })
      if (!isCurrent(token)) return // superseded by a newer query
      setOptions(result.users.filter((u) => !selected.includes(u.account)))
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
    } else {
      onChange(account)
      setQuery('')
      setOptions([])
      setOpen(false)
    }
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
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => options.length > 0 && setOpen(true)}
            onBlur={() => setOpen(false)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') setOpen(false)
            }}
            disabled={disabled}
          />
          {open && options.length > 0 && (
            <ul
              className="account-picker-options"
              role="listbox"
              // Keeps focus on the input across the click so `onBlur` above doesn't close the
              // dropdown before the option's own onClick has a chance to run.
              onMouseDown={(e) => e.preventDefault()}
            >
              {options.map((u) => (
                <li key={u.account} role="option" aria-selected="false" onClick={() => select(u.account)}>
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
