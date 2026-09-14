# pr-self-audit record
pr: 418
branch: fix/restricted-inherit-via-roommeta
sha: b75f8b74
audited_at: 2026-08-31T02:45:13Z
verdict: reshaped to denorm-only core (roommetacache Meta restricted/externalAccess projection so GetRoomMeta serves them; newSub denorm already on main #298). 3 files +67/-21; machinery deferred to planning #37. build+unit+integration-compile+lint green. tGD disposition posted.
