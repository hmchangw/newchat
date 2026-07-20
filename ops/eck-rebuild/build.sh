#!/usr/bin/env bash
# Local build wrapper for the ECK 1.8.0 rebuild.
# Reads REGISTRY / IMAGE / TAG / GO_VERSION from the env and produces:
#   ${REGISTRY}/${IMAGE}:${TAG}
set -euo pipefail

REGISTRY="${REGISTRY:-josephsylvan}"
IMAGE="${IMAGE:-eck-operator}"
TAG="${TAG:-1.8.0-go1.26}"
GO_VERSION="${GO_VERSION:-1.26.5}"

FULL_TAG="${REGISTRY}/${IMAGE}:${TAG}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo ">> Building ${FULL_TAG} (Go ${GO_VERSION})"
docker build \
  --build-arg GO_VERSION="${GO_VERSION}" \
  --build-arg ECK_VERSION=1.8.0 \
  --build-arg ECK_REF=1.8.0 \
  -t "${FULL_TAG}" \
  -f "${DIR}/Dockerfile" \
  "${DIR}"

echo ">> Inspecting embedded Go toolchain"
CID=$(docker create "${FULL_TAG}")
trap 'docker rm -f "${CID}" >/dev/null 2>&1 || true' EXIT
docker cp "${CID}:/elastic-operator" /tmp/elastic-operator-rebuilt
if command -v go >/dev/null 2>&1; then
  go version /tmp/elastic-operator-rebuilt
  strings /tmp/elastic-operator-rebuilt 2>/dev/null | grep -q crypto/elliptic.panicIfNotOnCurve \
    && echo "   crypto/elliptic.panicIfNotOnCurve present — CVE-2022-23806 fix baked in." \
    || echo "   WARNING: panicIfNotOnCurve symbol not found; verify Go version manually."
else
  echo "   (go not installed on host — skipping toolchain inspection; binary is at /tmp/elastic-operator-rebuilt)"
fi
echo ""
echo ">> Image ready: ${FULL_TAG}"
echo ">> To push:"
echo "     docker login -u ${REGISTRY}   # or with a PAT"
echo "     docker push ${FULL_TAG}"
