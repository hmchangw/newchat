.PHONY: lint fmt test test-integration generate build deps-up deps-down \
        require-deps up up-detached down dev \
        obs-up obs-down profile tools sast sast-gosec sast-vuln sast-semgrep

DEPS_COMPOSE     := docker-local/compose.deps.yaml
SERVICES_COMPOSE := docker-local/compose.services.yaml
NATS_CREDS       := docker-local/backend.creds
NATS_CONF        := docker-local/nats.conf
NATS_CONTAINER   := chat-local-nats
OBS_COMPOSE      := tools/observability/docker-compose.yml

# --- SAST / dev tooling ------------------------------------------------------
# Pinned tool versions. Keep GOLANGCI_LINT_VERSION in sync with
# .github/workflows/ci.yml. golangci-lint/gosec/govulncheck install via
# `go install` into $(GOBIN_DIR) (no go.mod impact); semgrep is a Python
# tool installed via pipx.
#
# TOOLS_GO_TOOLCHAIN pins the toolchain used to *source-build* the Go
# tools (via GOTOOLCHAIN) so installs are reproducible regardless of the
# runner's Go. Tool versions must themselves be Go 1.25-compatible:
# gosec < v2.26 pins golang.org/x/tools@v0.25.0, which fails to compile
# under any Go 1.25.x ("invalid array length -delta * delta"), so
# GOSEC_VERSION is held at a release whose dependency tree builds on
# Go 1.25. Tracks the repo-wide Go (go.mod / ci.yml); Go fetches the
# pinned toolchain on demand.
GOBIN_DIR             := $(shell go env GOPATH)/bin
TOOLS_GO_TOOLCHAIN    := go1.26.5
GOLANGCI_LINT_VERSION := v2.12.2
GOSEC_VERSION         := v2.26.1
GOVULNCHECK_VERSION   := v1.3.0
SEMGREP_VERSION       := 1.163.0

GOSEC       := $(GOBIN_DIR)/gosec
GOVULNCHECK := $(GOBIN_DIR)/govulncheck

# gosec scope: shipped product code + tests. tools/ holds dev/ops utilities
# (loadgen, nats-debug) that are not deployed services; chat-frontend is
# JS. -tests=true scans *_test.go so PR gating catches issues in test code
# too (mocks are filtered by -exclude-generated). Gate: medium+ severity.
GOSEC_FLAGS := -quiet -severity medium -confidence medium -tests=true \
               -exclude-generated -exclude-dir=tools -exclude-dir=testdata

# semgrep: fail on medium+ (WARNING/ERROR; INFO is informational/low).
SEMGREP_FLAGS := --error --severity=WARNING --severity=ERROR --metrics=off \
                 --exclude=tools --exclude=chat-frontend --exclude=testdata \
                 --exclude=docs --config=p/golang --config=p/security-audit \
                 --config=.semgrep/

# Makefile for the distributed multi-site chat system.

# Run golangci-lint (includes go vet, staticcheck, errcheck, goimports, etc.)
lint:
	golangci-lint run ./...

# Run goimports via golangci-lint to format all .go files
fmt:
	golangci-lint fmt ./...

# Run all unit tests with race detector (excludes integration tests)
test:
ifdef SERVICE
	go test -race ./$(SERVICE)/...
else
	go test -race ./...
endif

# Run integration tests (requires Docker)
test-integration:
ifdef SERVICE
	go test -race -tags integration ./$(SERVICE)/...
else
	go test -race -tags integration ./...
endif

# Regenerate all mocks via go generate
generate:
ifdef SERVICE
	go generate ./$(SERVICE)/...
else
	go generate ./...
endif

# Build a single service binary (requires SERVICE=<name>)
build:
ifndef SERVICE
	$(error SERVICE is required. Usage: make build SERVICE=<name>)
endif
ifeq ($(SERVICE),history-service)
	CGO_ENABLED=0 go build -o bin/$(SERVICE) ./$(SERVICE)/cmd/
else
	CGO_ENABLED=0 go build -o bin/$(SERVICE) ./$(SERVICE)/
endif

# --- Local dev docker targets -------------------------------------------------
# Start third-party deps (NATS, Mongo, Cassandra, ES, Keycloak, Vault, MinIO)
# in the background. Runs setup.sh on first use. Blocks until every dep's
# healthcheck passes, then runs the init one-shots (cassandra schema, vault
# transit key).
deps-up:
	@if [ ! -f $(NATS_CREDS) ] || [ ! -f $(NATS_CONF) ]; then \
	  echo "First-time setup: generating nats.conf + backend.creds..."; \
	  ./docker-local/setup.sh; \
	fi
	docker compose -f $(DEPS_COMPOSE) up -d --wait
	docker compose -f $(DEPS_COMPOSE) --profile init run --rm cassandra-init
	docker compose -f $(DEPS_COMPOSE) --profile init run --rm vault-init

# Stop third-party deps.
deps-down:
	docker compose -f $(DEPS_COMPOSE) down

# Guard: the shared deps must be running and NATS creds/conf present before any
# service starts. A prerequisite of both `up` and `up-detached` so the check
# lives in one place.
require-deps:
	@docker container inspect -f '{{.State.Running}}' $(NATS_CONTAINER) 2>/dev/null | grep -q true || { \
	  echo "Deps are not running. Run 'make deps-up' first."; exit 1; \
	}
	@test -f $(NATS_CREDS) && test -f $(NATS_CONF) || { \
	  echo "Missing $(NATS_CREDS) or $(NATS_CONF). Run './docker-local/setup.sh'."; exit 1; \
	}

# Start microservices. With SERVICE=<name>, starts just that service's compose;
# without, starts every service via compose.services.yaml.
#   up           — foreground, so container logs stream to the terminal; Ctrl-C stops.
#   up-detached  — same bring-up but detached, the single entry point for
#                  orchestration that needs the services in the background
#                  (e.g. the loadgen deploy). Keeping one shared recipe means the
#                  compose command can't drift between the two.
up up-detached: require-deps
ifdef SERVICE
	docker compose -f $(SERVICE)/deploy/docker-compose.yml up $(UP_DETACH) --build
else
	docker compose -f $(SERVICES_COMPOSE) up $(UP_DETACH) --build
endif
up-detached: UP_DETACH := -d

# Hot-reload a single service against the shared deps stack. Requires
# `make deps-up` first. Uses air; install via `make tools`.
dev:
ifndef SERVICE
	$(error SERVICE is required. Usage: make dev SERVICE=<name>)
endif
	@chmod +x tools/dev/dev.sh
	./tools/dev/dev.sh $(SERVICE)

# Stop microservices. SERVICE=<name> stops one; otherwise stops every service.
down:
ifdef SERVICE
	docker compose -f $(SERVICE)/deploy/docker-compose.yml down
else
	docker compose -f $(SERVICES_COMPOSE) down
endif

# --- Local observability targets ----------------------------------------------
# Start cAdvisor + Prometheus + Grafana. Requires `make deps-up` first so the
# chat-local network exists. Dashboard at http://localhost:3001.
obs-up:
	@docker network inspect chat-local >/dev/null 2>&1 || { \
	  echo "chat-local network missing. Run 'make deps-up' first."; exit 1; \
	}
	docker compose -f $(OBS_COMPOSE) up -d --wait

# Stop the observability stack.
obs-down:
	docker compose -f $(OBS_COMPOSE) down

# --- Profiling capture --------------------------------------------------------
# Snapshot pprof profiles (cpu/heap/goroutine) from every message-pipeline
# service into profiles/<UTC-timestamp>[-<label>]/. Requires the stack running
# with profiling enabled (`PPROF_ENABLED=true make up`) and the chat-local
# network (`make deps-up`). Fans out over the network from a one-shot curl
# container — no host ports are published. Tunables:
#   DURATION=<seconds>  CPU profile window (default 30)
#   LABEL=<tag>         appended to the run folder name
#   SERVICES="a b c"    override the default nine-service manifest
PROFILE_IMAGE := curlimages/curl:8.11.1

profile:
	@docker network inspect chat-local >/dev/null 2>&1 || { \
	  echo "chat-local network missing. Run 'make deps-up' first."; exit 1; \
	}
	@mkdir -p profiles
	docker run --rm --network chat-local \
	  -e DURATION="$(DURATION)" -e LABEL="$(LABEL)" -e SERVICES="$(SERVICES)" \
	  -v "$(PWD)/tools/profilecapture/capture.sh:/capture.sh:ro" \
	  -v "$(PWD)/profiles:/out" \
	  --entrypoint sh $(PROFILE_IMAGE) /capture.sh

# --- SAST -------------------------------------------------------------------
# Install pinned dev/SAST tooling. Go tools install into $(GOBIN_DIR) with
# no go.mod impact; semgrep installs via pipx. Idempotent — safe to re-run.
# setuptools is injected into semgrep's venv because semgrep imports
# pkg_resources, which setuptools-less Python 3.12+ (e.g. ubuntu-latest)
# no longer ships by default.
tools:
	GOTOOLCHAIN=$(TOOLS_GO_TOOLCHAIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	GOTOOLCHAIN=$(TOOLS_GO_TOOLCHAIN) go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION)
	GOTOOLCHAIN=$(TOOLS_GO_TOOLCHAIN) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	GOTOOLCHAIN=$(TOOLS_GO_TOOLCHAIN) go install github.com/air-verse/air@v1.62.0
	@if command -v pipx >/dev/null 2>&1; then \
	  pipx install --force semgrep==$(SEMGREP_VERSION) \
	    && pipx inject semgrep setuptools; \
	elif command -v semgrep >/dev/null 2>&1; then \
	  echo "pipx not found, but semgrep is already on PATH — skipping semgrep install"; \
	else \
	  echo "pipx not found and semgrep not on PATH — install pipx, or: pip install --user semgrep==$(SEMGREP_VERSION)" >&2; \
	  exit 1; \
	fi

# Run all SAST scans (gosec, govulncheck, semgrep). All three always run
# (no fail-fast) so every category is reported in one pass; exits non-zero
# if any scan finds an issue. This is the exact command CI enforces.
sast:
	@rc=0; g=PASS; v=PASS; s=PASS; \
	$(MAKE) --no-print-directory sast-gosec   || { rc=1; g=FAIL; }; \
	$(MAKE) --no-print-directory sast-vuln    || { rc=1; v=FAIL; }; \
	$(MAKE) --no-print-directory sast-semgrep || { rc=1; s=FAIL; }; \
	echo "==> SAST summary: gosec=$$g govulncheck=$$v semgrep=$$s"; \
	exit $$rc

# gosec: Go security static analysis (injection, weak crypto, unsafe code).
sast-gosec:
	@test -x "$(GOSEC)" || { echo "gosec not installed — run 'make tools'"; exit 1; }
	$(GOSEC) $(GOSEC_FLAGS) ./...

# govulncheck: known CVEs in dependencies with call-graph reachability.
# Requires outbound network access to https://vuln.go.dev.
sast-vuln:
	@test -x "$(GOVULNCHECK)" || { echo "govulncheck not installed — run 'make tools'"; exit 1; }
	GOTOOLCHAIN=$(TOOLS_GO_TOOLCHAIN) $(GOVULNCHECK) ./...

# semgrep: rule-based SAST (Go security + security-audit rulesets).
# Requires outbound network access to the Semgrep registry on first run.
sast-semgrep:
	@command -v semgrep >/dev/null 2>&1 || { echo "semgrep not installed — run 'make tools' (needs pipx), or: pipx install semgrep==$(SEMGREP_VERSION)"; exit 1; }
	semgrep scan $(SEMGREP_FLAGS) .

# --- Sample data seeder -----------------------------------------------------
# Populate MongoDB and Valkey with a small idempotent dataset for local dev.
# Run after `make deps-up`. Safe to re-run; `seed-reset` wipes the seed
# records first via stable IDs (never DROP DATABASE) so any hand-added
# dev data survives. `seed-dry-run` prints the plan without writing.
.PHONY: seed seed-reset seed-dry-run

seed:
	go run ./tools/seed-sample-data

seed-reset:
	go run ./tools/seed-sample-data --reset

seed-dry-run:
	go run ./tools/seed-sample-data --dry-run
