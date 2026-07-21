# hr-sync-worker

Reference consumer for the HR feed — one durable, strictly-sequential
consumer per `SITE_IDS` entry on `HR_{siteID}` (`chat.hr.{siteID}.>`),
persisting:

| Subject suffix | Write |
|---|---|
| `employees.upsert` | Replace `hr_employee` by `{account, source}` |
| `users.upsert` | Upsert `users` by account — **identity fields only** (`account/siteId/engName/chineseName/employeeId`); roles/services/password are never written (`users` is the live auth store) |
| `employees.quit` | Delete `hr_employee` scoped `{account ∈ batch, source: "teams"}` — legacy-source rows survive; `users` untouched |

All writes idempotent (at-least-once feed). Malformed payloads Ack-drop as
poison; store failures Nak-retry with backoff. An external persister can
replace this worker — the contract is just the three subjects above.

## Config

`SITE_IDS` (comma list, required), `NATS_URL` (+`NATS_CREDS_FILE`),
`MONGO_WRITE_URI` (+`MONGO_WRITE_USERNAME/PASSWORD/DB`),
`BOOTSTRAP_STREAMS` (dev only), `HEALTH_ADDR` (default `:8081`).
