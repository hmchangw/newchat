// Splits a pasted account list on whitespace/commas/semicolons, deduplicating while
// preserving first-occurrence order. Accounts are kept verbatim (no case folding),
// matching backend semantics.
export function parsePastedAccounts(text) {
  const seen = new Set()
  const accounts = []
  let duplicates = 0
  for (const token of text.split(/[\s,;]+/)) {
    if (!token) continue
    if (seen.has(token)) {
      duplicates += 1
      continue
    }
    seen.add(token)
    accounts.push(token)
  }
  return { accounts, duplicates }
}
