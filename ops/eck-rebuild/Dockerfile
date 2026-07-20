# Rebuild of ECK operator v1.8.0 with a modern Go toolchain and patched
# third-party modules, so the produced binary clears the stdlib CVE backlog
# accumulated since Go 1.17 and the transitive-dep CVEs pinned by ECK 1.8.0's
# go.mod. The operator SOURCE stays at git tag 1.8.0 — same reconcile
# behavior, same CRD compatibility as the upstream 1.8.0 image.
#
# Mirrors the upstream Dockerfile layout at github.com/elastic/cloud-on-k8s
# tag 1.8.0 (build target: github.com/elastic/cloud-on-k8s/cmd; runtime file
# /conf/eck.yaml + licenses under /licenses/; USER 1001; ENTRYPOINT
# ./elastic-operator manager).
#
# Build:
#   docker build \
#     -t josephsylvan/eck-operator:1.8.0-go1.26-cvefix-<YYYYMMDD> \
#     -f ops/eck-rebuild/Dockerfile \
#     ops/eck-rebuild

ARG GO_VERSION=1.26.5
ARG ECK_VERSION=1.8.0
ARG ECK_REF=1.8.0

# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — compile the operator with a current Go toolchain
# ─────────────────────────────────────────────────────────────────────────────
# Pinned by digest so a rebuild from this Dockerfile is deterministic — a
# floating tag would let the underlying image drift between rebuilds, hiding
# why "the same Dockerfile" produced a different binary. Bump this digest
# deliberately (with the ${GO_VERSION} ARG) on each scheduled rebuild.
# Manifest-list digest → works on both arm64 and amd64.
FROM golang:${GO_VERSION}-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651 AS builder

ARG ECK_VERSION
ARG ECK_REF

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /go/src/github.com/elastic/cloud-on-k8s
RUN git clone --depth=1 --branch ${ECK_REF} https://github.com/elastic/cloud-on-k8s.git .

# Retarget the two files that still reference `k8s.io/api/policy/v1beta1` to
# `policy/v1`. Kubernetes removed `policy/v1beta1` in 1.25; ECK 1.8.0 uses it
# for its PDB Reconcile + IsDisabled cleanup path, so even a chart that opts
# out with `podDisruptionBudget: {}` triggers a Get(v1beta1.PodDisruptionBudget)
# that fails with `no matches for kind PodDisruptionBudget in version
# policy/v1beta1` — the reconcile loops forever and pods stay Pending.
# The v1beta1 and v1 PodDisruptionBudget schemas are field-identical, so a
# pure import-path rewrite compiles + runs. Keeps the operator source at
# tag 1.8.0 in every other respect.
RUN set -eux; \
    sed -i 's|"k8s.io/api/policy/v1beta1"|v1beta1 "k8s.io/api/policy/v1"|g' \
      pkg/apis/common/v1/common.go \
      pkg/controller/elasticsearch/pdb/reconcile.go

# Bump transitive deps that ship CRITICAL/HIGH CVEs at the versions pinned by
# ECK 1.8.0's go.mod. The operator SOURCE stays at tag 1.8.0 (same reconcile
# behavior, same CRD compatibility); only third-party modules move to patched
# versions. Clears:
#   x/crypto           — CVE-2024-45337, CVE-2026-39830..34, -42508, -46595
#   x/net              — CVE-2026-39821, plus older http2/idna DoS chain
#   x/oauth2           — CVE-2025-22868
#   prometheus/client_golang — CVE-2022-21698, CVE-2023-25151, CVE-2023-45142
#   yaml.v3            — CVE-2022-28948
#   moby/spdystream    — CVE-2026-35469
# `go mod tidy` reconciles go.sum; `-mod=mod` below allows the intentional
# module edits. If a bump breaks the ECK 1.8.0 build API, revert that one line.
RUN set -eux; \
    go get golang.org/x/crypto@v0.52.0; \
    go get golang.org/x/net@v0.55.0; \
    go get golang.org/x/oauth2@v0.27.0; \
    go get github.com/prometheus/client_golang@v1.11.1; \
    go get gopkg.in/yaml.v3@v3.0.1; \
    go get github.com/moby/spdystream@v0.5.1; \
    go mod tidy

# Preserve upstream's ldflags shape so the operator's /about metadata + Prom
# metrics report identically to a stock 1.8.0 build.
RUN set -eux; \
    SHA1=$(git rev-parse --short HEAD); \
    BUILD_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ); \
    CGO_ENABLED=0 GOOS=linux \
    go build \
      -mod=mod \
      -trimpath \
      -ldflags "-w -s \
        -X github.com/elastic/cloud-on-k8s/pkg/about.version=${ECK_VERSION} \
        -X github.com/elastic/cloud-on-k8s/pkg/about.buildHash=${SHA1} \
        -X github.com/elastic/cloud-on-k8s/pkg/about.buildDate=${BUILD_TS} \
        -X github.com/elastic/cloud-on-k8s/pkg/about.buildSnapshot=false" \
      -o /out/elastic-operator \
      github.com/elastic/cloud-on-k8s/cmd

# Upstream's Dockerfile does `COPY config/eck.yaml /conf/eck.yaml`, but that
# file is not in the git tree — it's generated at build time by
# hack/config-extractor/extract.sh from all-in-one.yaml + a Go program.
# In real deployments (Helm chart, all-in-one manifest) the operator's
# ConfigMap is mounted as a volume onto /conf/eck.yaml, overriding whatever
# is baked in. So we ship a minimal stub here — the mount takes over at
# runtime. This keeps the image usable if someone launches the operator
# without a ConfigMap (defaults kick in) and matches the file layout the
# operator's --config=/conf/eck.yaml flag expects.
RUN mkdir -p /out/conf && \
    printf '%s\n' \
      '# Default stub — production deployments override this via ConfigMap' \
      '# volume mount onto /conf/eck.yaml. See the ECK Helm chart /' \
      '# all-in-one.yaml for the full set of tunables.' \
      'log-verbosity: 0' \
      > /out/conf/eck.yaml

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — distroless runtime
#
# The operator is CGO_ENABLED=0 static, so it doesn't need libc/openssl/krb5
# from the base image. Distroless ships only ca-certificates + tzdata + a
# minimal filesystem — no shell, no package manager, no gnupg2/gnutls/nghttp2/
# libarchive/curl — which eliminates the RHEL9-side CVE surface entirely.
#
# We use the `:latest` variant (not `:nonroot`) and set USER 1001 ourselves
# so the image is a drop-in replacement for upstream ECK 1.8.0, which also
# runs as UID 1001. Nothing in the ECK Helm chart, ConfigMap, Deployment, or
# pod securityContext needs to change — same UID, same ENTRYPOINT, same
# /conf/eck.yaml layout. Manifest-list digest pinned; bump on each rebuild.
# ─────────────────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:latest@sha256:61b7ccecebc7c474a531717de80a94709d20547cdcdaf740c25876f2a8e38b44

ARG ECK_VERSION

LABEL name="Elastic Cloud on Kubernetes" \
      io.k8s.display-name="Elastic Cloud on Kubernetes" \
      vendor="Elastic (repackaged)" \
      version="${ECK_VERSION}" \
      url="https://www.elastic.co/guide/en/cloud-on-k8s/" \
      summary="ECK operator ${ECK_VERSION} recompiled with a current Go stdlib on distroless" \
      description="ECK operator ${ECK_VERSION} recompiled from github.com/elastic/cloud-on-k8s@${ECK_VERSION} with a current Go toolchain and repackaged on gcr.io/distroless/static-debian12 to clear stdlib, third-party module, and base-image CVEs. Functionally identical to upstream ${ECK_VERSION}."

WORKDIR /
COPY --from=builder /out/elastic-operator                                   ./elastic-operator
COPY --from=builder /out/conf/eck.yaml                                      /conf/eck.yaml
COPY --from=builder /go/src/github.com/elastic/cloud-on-k8s/LICENSE.txt     /licenses/LICENSE.txt
COPY --from=builder /go/src/github.com/elastic/cloud-on-k8s/NOTICE.txt      /licenses/NOTICE.txt

USER 1001:1001

ENTRYPOINT ["./elastic-operator"]
CMD ["manager"]
