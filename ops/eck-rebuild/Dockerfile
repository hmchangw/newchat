# Rebuild of ECK operator v1.8.0 with a modern Go toolchain, so the produced
# binary embeds a current Go standard library (fixes CVE-2022-23806 and other
# stdlib CVEs accumulated since Go 1.17). The operator source stays at tag
# 1.8.0 — same reconcile behavior, same CRD compatibility as the upstream
# 1.8.0 image.
#
# Mirrors the upstream Dockerfile layout at github.com/elastic/cloud-on-k8s
# tag 1.8.0 (build target: github.com/elastic/cloud-on-k8s/cmd; runtime file
# /conf/eck.yaml + licenses under /licenses/; USER 1001; ENTRYPOINT
# ./elastic-operator manager).
#
# Build:
#   docker build \
#     -t josephsylvan/eck-operator:1.8.0-go1.26 \
#     -f ops/eck-rebuild/Dockerfile \
#     ops/eck-rebuild

ARG GO_VERSION=1.26.5
ARG ECK_VERSION=1.8.0
ARG ECK_REF=1.8.0

# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — compile the operator with a current Go toolchain
# ─────────────────────────────────────────────────────────────────────────────
FROM golang:${GO_VERSION}-bookworm AS builder

ARG ECK_VERSION
ARG ECK_REF

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /go/src/github.com/elastic/cloud-on-k8s
RUN git clone --depth=1 --branch ${ECK_REF} https://github.com/elastic/cloud-on-k8s.git .

# Preserve upstream's ldflags shape so the operator's /about metadata + Prom
# metrics report identically to a stock 1.8.0 build.
RUN set -eux; \
    SHA1=$(git rev-parse --short HEAD); \
    BUILD_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ); \
    CGO_ENABLED=0 GOOS=linux \
    go build \
      -mod=readonly \
      -trimpath \
      -ldflags "-w -s \
        -X github.com/elastic/cloud-on-k8s/pkg/about.version=${ECK_VERSION} \
        -X github.com/elastic/cloud-on-k8s/pkg/about.buildHash=${SHA1} \
        -X github.com/elastic/cloud-on-k8s/pkg/about.buildDate=${BUILD_TS} \
        -X github.com/elastic/cloud-on-k8s/pkg/about.buildSnapshot=false" \
      -o /out/elastic-operator \
      github.com/elastic/cloud-on-k8s/cmd

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — minimal runtime with a current base image
# ─────────────────────────────────────────────────────────────────────────────
FROM redhat/ubi9-minimal:latest

ARG ECK_VERSION

LABEL name="Elastic Cloud on Kubernetes" \
      io.k8s.display-name="Elastic Cloud on Kubernetes" \
      vendor="Elastic (repackaged)" \
      version="${ECK_VERSION}" \
      url="https://www.elastic.co/guide/en/cloud-on-k8s/" \
      summary="ECK operator ${ECK_VERSION} recompiled with a current Go stdlib on ubi9-minimal" \
      description="ECK operator ${ECK_VERSION} recompiled from github.com/elastic/cloud-on-k8s@${ECK_VERSION} with a current Go toolchain and repackaged on ubi9-minimal to clear stdlib and base-image CVEs. Functionally identical to upstream ${ECK_VERSION}."

RUN microdnf update -y --setopt=tsflags=nodocs \
 && microdnf install -y ca-certificates tzdata \
 && microdnf clean all \
 && rm -rf /var/cache/yum /var/cache/dnf

WORKDIR /
COPY --from=builder /out/elastic-operator     ./elastic-operator
COPY --from=builder /go/src/github.com/elastic/cloud-on-k8s/config/eck.yaml /conf/eck.yaml
COPY --from=builder /go/src/github.com/elastic/cloud-on-k8s/LICENSE.txt     /licenses/LICENSE.txt
COPY --from=builder /go/src/github.com/elastic/cloud-on-k8s/NOTICE.txt      /licenses/NOTICE.txt

USER 1001

ENTRYPOINT ["./elastic-operator"]
CMD ["manager"]
