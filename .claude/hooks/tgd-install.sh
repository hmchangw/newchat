#!/bin/bash
# tGD framework installer — runs at SessionStart on Claude Code on the web.
#
# Clones the tGD repo at a pinned release tag and runs its setup.sh, which
# symlinks the tgd-* skills into ~/.claude/skills and the /tgd-* slash commands
# into ~/.claude/commands. After this runs, every remote session can use the
# tGD skills and commands immediately.
#
# Isolated from session-start.sh on purpose: registered as its own SessionStart
# entry so a tGD hiccup can never break the superpowers / Go-tooling install.
set -uo pipefail

# Only run in the remote (web) environment. Local users manage their own tGD
# install — the same guard the sibling session-start.sh uses.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Pin to a release tag rather than a moving branch — this hook auto-runs in
# every session, so upstream changes must be an explicit version bump here,
# mirroring how .claude/settings.json pins the 365-skills marketplace by SHA.
TGD_VERSION="v2026.07.11.3"
TGD_REPO="https://github.com/openclawyhwang-hub/tGD.git"
TGD_SRC="${HOME}/.tgd-src"

# Idempotent: fetch/checkout the pinned tag if the clone already exists,
# otherwise shallow-clone it. A clone failure (e.g. network) must not break
# session start, so we bail out cleanly instead of erroring.
if [ -d "$TGD_SRC/.git" ]; then
  git -C "$TGD_SRC" fetch --depth 1 origin "refs/tags/${TGD_VERSION}" 2>/dev/null \
    && git -C "$TGD_SRC" checkout -q FETCH_HEAD 2>/dev/null \
    || echo "tGD: refresh to ${TGD_VERSION} failed; using existing checkout" >&2
else
  if ! git clone --depth 1 --branch "$TGD_VERSION" "$TGD_REPO" "$TGD_SRC" 2>/dev/null; then
    echo "tGD: clone of ${TGD_VERSION} failed; skipping install" >&2
    exit 0
  fi
fi

# setup.sh detects `claude` on PATH and links the core skills/commands first,
# before its optional add-ons (CodeGraph via npm, Understand-Anything via pnpm)
# which may fail under a restricted network. We tolerate a non-zero exit so
# those optional failures never block the session — the core install is already
# done by the time they run.
bash "$TGD_SRC/setup.sh" || echo "tGD: setup.sh reported errors (optional components may have been skipped)" >&2

exit 0
