---
description: Triage SAST findings (gosec / govulncheck / semgrep) — classify each as fix vs justified suppression, write a reviewable report
argument-hint: [gosec|govulncheck|semgrep|--apply-suppressions]
---

You are triaging SAST findings from `make sast` (gosec + govulncheck + semgrep). The SAST job is a blocking CI gate per CLAUDE.md Section 5 (fails on medium+).

# Preconditions

1. Parse `$ARGUMENTS`:
   - Empty: run all three tools (`make sast`).
   - One of `gosec` / `govulncheck` / `semgrep`: run only that tool (`make sast-<tool>`).
   - `--apply-suppressions`: do NOT re-run scans. Read the most recent `docs/sast/triage-*.md` report and apply ONLY the `FALSE_POSITIVE_SUPPRESS` items (insert the proposed `// #nosec <RULE> -- <reason>` lines verbatim).
2. Verify the working tree under `docs/sast/` is clean. If dirty, ask the user to commit or stash first.
3. Resolve today's date in `YYYY-MM-DD`. Compute the report path: `docs/sast/triage-<date>.md`. If it exists, append `-2`, `-3`, etc. until you find a free name.

# Run SAST

Unless `--apply-suppressions`:
1. Run `make sast` (or `make sast-<tool>` per arg).
2. Parse each tool's output. If the Makefile target does not emit machine-readable JSON, invoke the underlying tool directly with a JSON flag for this audit step (acceptable read-only fallback, same exception used for coverage profiles):
   - `gosec -fmt=json -no-fail ./...`
   - `govulncheck -json ./...`
   - `semgrep --json --config=auto`
3. Filter findings to **medium+** severity (matches CI gate).
4. Group findings by file. Build `FILES_WITH_FINDINGS`.
5. If no medium+ findings: print `No medium+ SAST findings.` and stop.

# Dispatch triage agents (parallel — one per file)

For each file in `FILES_WITH_FINDINGS`, dispatch an agent in parallel (single message, multiple Agent tool calls). Each agent must:
- Read `CLAUDE.md` (especially Section 5 on SAST suppression syntax).
- Read the target file in full + enough surrounding code (callers, related types) to understand intent.
- For each finding in its file, classify as one of:
  - **`REAL_FIX`** — propose a concrete code patch (diff snippet). Do NOT apply it. Include a one-paragraph rationale.
  - **`FALSE_POSITIVE_SUPPRESS`** — propose the exact `// #nosec <RULE> -- <reason>` comment to add **directly above** the offending statement. Reason must be non-trivial (the actual safety argument, not "false positive"). Note: golangci-lint's `//nolint:gosec` does NOT suppress standalone gosec — both mechanisms are independent.
  - **`NEEDS_HUMAN_DECISION`** — explain the trade-off and what input is needed (e.g., a business-logic question, a security-vs-perf judgment).
- Return ≤ 400 words: one section per finding with tool, rule ID, file:line, severity, classification, proposed action, justification.

# Synthesize the report — chapter by chapter, commit per chapter

Write the report **incrementally**. Each chapter is its own commit. Do not skip the commit step, do not batch chapters.

For each chapter:
1. Draft content from the relevant agent output.
2. Append to the report file (create on chapter 1).
3. `git add <report-path>`
4. `git commit -m "sast(triage): <chapter-name>"` with the standard `Co-Authored-By: Claude` trailer.

Chapter order:
1. **Header + summary** — date, tools run, total findings, count per classification (`REAL_FIX` / `FALSE_POSITIVE_SUPPRESS` / `NEEDS_HUMAN_DECISION`), count per severity.
2. **One chapter per file** — the file-agent's full output. Title `File: <path>`.
3. **Action list** — flat list ordered by: REAL_FIX items (severity, critical first), then NEEDS_HUMAN_DECISION items, then FALSE_POSITIVE_SUPPRESS items. Each item references its file chapter.

# --apply-suppressions mode

If `$ARGUMENTS == "--apply-suppressions"`:
1. Find the most recent `docs/sast/triage-*.md` in the repo.
2. Parse out every `FALSE_POSITIVE_SUPPRESS` item with its exact proposed comment + file:line.
3. For each, insert the comment directly above the named line. Verify the line still matches the original SAST finding (regex check against the rule). If line drift detected, SKIP that suppression and report it.
4. After applying, run `make sast` to verify the suppressions take effect — no medium+ findings should remain for the suppressed rules.
5. Commit: `sast(suppress): apply <N> reviewed false-positive suppressions from <report-file>` with `Co-Authored-By` trailer.
6. Print summary: count applied, count skipped (line drift), final SAST status.

# Final chat output (5–10 lines, default mode)

- Total findings (by severity)
- Classifications breakdown
- Items requiring human decision (count + brief note)
- Path to the report file
- Next step: `Review the report, then run /sast_triage --apply-suppressions to apply approved suppressions.`

This command **never applies fixes or suppressions by default** — suppressing security findings is consequential. The user reviews the report first.
