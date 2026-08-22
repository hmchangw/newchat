.PHONY: lint fmt tidy test test-integration benchmark-natsmetrics test-loadgen-failure test-loadgen-failure-integration coverage-loadgen-failure coverage-loadgen-soak generate build validate-loadgen-k8s deps-up deps-down \
        require-deps up up-detached down dev ui-up ui-down \
        o11y-up o11y-down obs-up obs-down profile tools tools-mockgen sast sast-gosec sast-vuln sast-semgrep sast-semgrep-test \
        fed-deps-up fed-deps-down fed-regen require-fed-deps fed-up fed-up-lean fed-down fed-ui-up fed-ui-down fed-logs \
        fed-seed fed-seed-reset fed-o11y-up fed-o11y-down

DEPS_COMPOSE     := docker-local/compose.deps.yaml
SERVICES_COMPOSE := docker-local/compose.services.yaml
NATS_CREDS       := docker-local/backend.creds
NATS_CONF        := docker-local/nats.conf
ENV_FILE         := docker-local/.env
# Compose auto-loads .env only from the project directory, so `up SERVICE=<name>`
# would otherwise take ${VAR} defaults instead of the env file.
COMPOSE_ENV      := $(if $(wildcard $(ENV_FILE)),--env-file $(ENV_FILE),)
NATS_CONTAINER   := chat-local-nats
UI_COMPOSE       := docker-local/compose.ui.yaml
O11Y_COMPOSE     := docker-local/compose.o11y.yaml
OBS_COMPOSE      := tools/observability/docker-compose.yml
NULL_DEVICE      := $(if $(filter Windows_NT,$(OS)),NUL,/dev/null)
KUBE_DRY_RUN     ?= false
LOADGEN_CHART    := tools/loadgen/deploy/k8s
LOADGEN_VALUES   := $(LOADGEN_CHART)/values-validation.yaml
LOADGEN_LOCAL_VALUES := $(LOADGEN_CHART)/values-local.yaml

FED_DEPS_COMPOSE := docker-local/compose.fed-deps.yaml
SITE_OVERRIDE    := docker-local/compose.site.yaml
FED_ENV_LOCAL    := docker-local/.env.site-local
FED_ENV_REMOTE   := docker-local/.env.site-remote
FED_NATS_LOCAL   := docker-local/nats-site-local.conf
FED_NATS_REMOTE  := docker-local/nats-site-remote.conf
FED_NATS_CONTAINER := chat-fed-nats-site-local
FED_O11Y_COMPOSE := docker-local/compose.fed-o11y.yaml
# Every artifact setup.sh generates for the federated stack. Guards check them
# all together: a bind mount whose source file is missing makes Docker
# materialise a directory in its place, so a partial check lets one absent conf
# turn into a crash-looping NATS and a later setup.sh failing with
# "Is a directory". sys.creds is the SYS-account credential the site-remote
# spoke's second leafnode remote needs — without it JetStream will not start.
FED_SYS_CREDS    := docker-local/sys.creds
FED_GENERATED    := $(FED_NATS_LOCAL) $(FED_NATS_REMOTE) $(FED_ENV_LOCAL) $(FED_ENV_REMOTE) $(FED_SYS_CREDS)

# Services site-remote starts. Empty = every service. Set to trim the remote
# peer; see the tier table in docker-local/README.md for what each drop costs.
FED_REMOTE_SERVICES ?=

# Tier 1: the minimum that keeps federation and a logged-in browser working.
# Dropping inbox-worker kills federation at the destination; dropping
# message-gatekeeper means ivan cannot send at all.
FED_TIER1 := inbox-worker outbox-worker room-service room-worker \
             message-gatekeeper message-worker broadcast-worker user-service \
             history-service auth-service portal-service traefik

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
TOOLS_GO_TOOLCHAIN    := go1.25.13
GOLANGCI_LINT_VERSION := v2.11.4
GOSEC_VERSION         := v2.26.1
GOVULNCHECK_VERSION   := v1.3.0
MOCKGEN_VERSION       := v0.6.0
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
                 --exclude=docs --exclude=.semgrep --config=p/golang \
                 --config=p/security-audit --config=.semgrep/

# Makefile for the distributed multi-site chat system.

# Run golangci-lint (includes go vet, staticcheck, errcheck, goimports, etc.)
lint:
	golangci-lint run ./...

# Run goimports via golangci-lint to format all .go files
fmt:
	golangci-lint fmt ./...

# Synchronize module requirements and checksums after dependency changes.
tidy:
	go mod tidy

# Run all unit tests with race detector (excludes integration tests)
test:
ifdef SERVICE
	go test -race ./$(SERVICE)/...
else
	go test -race ./...
endif

# Measure the repository-owned JetStream delivery tracking overhead. The
# benchmark uses pre-decoded metadata so nats.go reply-subject parsing remains
# outside the result.
benchmark-natsmetrics:
	go test -run '^$$' -bench 'Benchmark(Consumer|Message)_' -benchmem ./pkg/natsmetrics

# Run integration tests (requires Docker)
test-integration:
ifdef SERVICE
	go test -race -tags integration ./$(SERVICE)/...
else
	go test -race -tags integration ./...
endif

FAILURE_TEST_PATTERN := 'Failure|ObservationRuntime|Observer|Recipient|ConsumerSampler|SoakCatalog|SoakSender|SoakRuntimeSelector|SoakPacing|LoadgenNATSHealth'

test-loadgen-failure:
	go test -race -run $(FAILURE_TEST_PATTERN) ./tools/loadgen/...

characterize-loadgen-failure-wal:
	go test -race -run '^TestFailureWALCharacterization$$' -v ./tools/loadgen/...

test-loadgen-failure-integration:
	go test -race -tags integration -run '^Test(FailureObservation_|MongoOutageRecovery_)' ./tools/loadgen/...

FAILURE_COVERAGE_PROFILE ?= coverage-loadgen-failure.out
coverage-loadgen-failure:
	go test -race -run $(FAILURE_TEST_PATTERN) -coverprofile=$(FAILURE_COVERAGE_PROFILE) ./tools/loadgen/...
	go run ./tools/coveragecheck -profile $(FAILURE_COVERAGE_PROFILE) -include tools/loadgen/failure_ -min 80
	go run ./tools/coveragecheck -profile $(FAILURE_COVERAGE_PROFILE) -include tools/loadgen/failure_observer.go -min 90
	go run ./tools/coveragecheck -profile $(FAILURE_COVERAGE_PROFILE) -include tools/loadgen/failure_metrics.go -min 90

# Run only Cassandra Run A tests (unit + integration), then enforce the scoped
# coverage contract. CLI/environment wiring and the Mongo adapter stay in the
# Run A aggregate; the core threshold excludes those two boundary files.
SOAK_COVERAGE_PROFILE ?= coverage-loadgen-soak.out
coverage-loadgen-soak:
	go test -race -tags integration -run Soak -coverprofile=$(SOAK_COVERAGE_PROFILE) ./tools/loadgen/...
	go run ./tools/coveragecheck -profile $(SOAK_COVERAGE_PROFILE) -include tools/loadgen/soak_ -min 80
	go run ./tools/coveragecheck -profile $(SOAK_COVERAGE_PROFILE) -include tools/loadgen/soak_ -exclude soak_main.go -exclude soak_store.go -min 90

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
ifeq ($(OS),Windows_NT)
	@if not exist bin mkdir bin
ifeq ($(SERVICE),history-service)
	set CGO_ENABLED=0&& go build -o bin/$(notdir $(SERVICE)) ./$(SERVICE)/cmd/
else
	set CGO_ENABLED=0&& go build -o bin/$(notdir $(SERVICE)) ./$(SERVICE)/
endif
else
	mkdir -p bin
ifeq ($(SERVICE),history-service)
	CGO_ENABLED=0 go build -o bin/$(SERVICE) ./$(SERVICE)/cmd/
else
	CGO_ENABLED=0 go build -o bin/$(notdir $(SERVICE)) ./$(SERVICE)/
endif
endif

# Lint and render every GitOps phase. The validation values use inert example
# endpoints and an immutable image digest; no Secret value is committed.
validate-loadgen-k8s:
	helm lint --strict $(LOADGEN_CHART) -f $(LOADGEN_VALUES)
	helm lint --strict $(LOADGEN_CHART) -f $(LOADGEN_LOCAL_VALUES)
	helm template cassandra-soak $(LOADGEN_CHART) -f $(LOADGEN_VALUES) --set phase=seed --show-only templates/seed-job.yaml > $(NULL_DEVICE)
	helm template cassandra-soak $(LOADGEN_CHART) -f $(LOADGEN_VALUES) --set phase=soak --show-only templates/soak-deployment.yaml > $(NULL_DEVICE)
	helm template cassandra-soak $(LOADGEN_CHART) -f $(LOADGEN_VALUES) --set phase=soak --set recipientObserver.enabled=true --show-only templates/configmap.yaml > $(NULL_DEVICE)
	helm template cassandra-soak $(LOADGEN_CHART) -f $(LOADGEN_VALUES) --set phase=stopped > $(NULL_DEVICE)
	helm template cassandra-soak $(LOADGEN_CHART) -f $(LOADGEN_VALUES) --set phase=teardown --set teardown.approved=true --show-only templates/teardown-job.yaml > $(NULL_DEVICE)
ifeq ($(KUBE_DRY_RUN),true)
	helm template cassandra-soak $(LOADGEN_CHART) -f $(LOADGEN_VALUES) --set phase=soak | kubectl apply --dry-run=client -f -
else
	@echo "Chart validation passed. Re-run with KUBE_DRY_RUN=true against a reachable Kubernetes context for client dry-run discovery."
endif

# --- Local dev docker targets -------------------------------------------------
# Start third-party deps (NATS, Mongo, Cassandra, ES, Keycloak, Vault, MinIO)
# in the background. Runs setup.sh on first use. Blocks until every dep's
# healthcheck passes, then runs the init one-shots (cassandra schema, vault
# transit key).
deps-up:
	@if [ ! -f $(NATS_CREDS) ] || [ ! -f $(NATS_CONF) ] || [ ! -f $(ENV_FILE) ]; then \
	  echo "First-time setup: generating nats.conf + backend.creds + .env..."; \
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
	@test -f $(NATS_CREDS) && test -f $(NATS_CONF) && test -f $(ENV_FILE) || { \
	  echo "Missing $(NATS_CREDS), $(NATS_CONF) or $(ENV_FILE). Run './docker-local/setup.sh'."; exit 1; \
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
	docker compose $(COMPOSE_ENV) -f $(SERVICE)/deploy/docker-compose.yml up $(UP_DETACH) --build
else
	docker compose $(COMPOSE_ENV) -f $(SERVICES_COMPOSE) up $(UP_DETACH) --build
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
	docker compose $(COMPOSE_ENV) -f $(SERVICE)/deploy/docker-compose.yml down
else
	docker compose $(COMPOSE_ENV) -f $(SERVICES_COMPOSE) down
endif

# Browser UIs (chat-frontend :3000, admin-frontend :3001). Kept out of `make up`
# because chat-frontend's port is the one `npm run dev` wants.
ui-up: require-deps
	docker compose $(COMPOSE_ENV) -f $(UI_COMPOSE) up -d --build

ui-down:
	docker compose $(COMPOSE_ENV) -f $(UI_COMPOSE) down

# --- Federated two-site local dev ---------------------------------------------
# A second site so cross-site federation can be QA'd in a browser: alice on
# site-local (:3000), ivan on site-remote (:3100). See docker-local/README.md.
#
# Cannot run alongside the single-site stack — both publish the same host ports
# for the shared datastores.
fed-deps-up:
	@docker container inspect -f '{{.State.Running}}' $(NATS_CONTAINER) 2>/dev/null | grep -q true && { \
	  echo "Single-site deps are running. Run 'make deps-down' first — the two stacks share host ports."; exit 1; \
	} || true
	@missing=""; \
	for f in $(NATS_CREDS) $(FED_GENERATED); do \
	  [ -f "$$f" ] || missing="$$missing $$f"; \
	done; \
	if [ -n "$$missing" ]; then \
	  echo "Missing generated file(s):$$missing"; \
	  if [ -f $(ENV_FILE) ]; then \
	    cp $(ENV_FILE) $(ENV_FILE).bak; \
	    echo "WARNING: $(ENV_FILE) already exists and is about to be regenerated with new NATS keys."; \
	    echo "         Previous copy saved to $(ENV_FILE).bak — re-apply any local edits (e.g. DEV_MODE=false) after setup."; \
	  fi; \
	  echo "First-time setup: generating NATS confs + env files..."; \
	  ./docker-local/setup.sh; \
	fi
	docker compose -f $(FED_DEPS_COMPOSE) up -d --wait
	KEYSPACE=chat docker compose -f $(FED_DEPS_COMPOSE) --profile init run --rm cassandra-init
	KEYSPACE=chat_remote docker compose -f $(FED_DEPS_COMPOSE) --profile init run --rm cassandra-init
	docker compose -f $(FED_DEPS_COMPOSE) --profile init run --rm vault-init

# Force a full regeneration of the per-site NATS confs and env files, then
# recreate the containers. Needed because `fed-deps-up` only runs setup.sh when
# a generated file is MISSING — so an edit to the conf template never reaches a
# tree that already has them — and because a bind-mounted file changing on disk
# does not restart the process that already read it. Both gaps have bitten;
# this target closes them together.
#
# setup.sh regenerates the NATS operator and account keys, so backend.creds and
# .env are rewritten too. The previous .env is saved to .env.bak: re-apply any
# local edits (e.g. DEV_MODE=false) afterwards.
fed-regen:
	@if [ -f $(ENV_FILE) ]; then \
	  cp $(ENV_FILE) $(ENV_FILE).bak; \
	  echo "WARNING: $(ENV_FILE) regenerated with new NATS keys; previous copy saved to $(ENV_FILE).bak."; \
	  echo "         Re-apply any local edits (e.g. DEV_MODE=false) after this finishes."; \
	fi
	docker compose -f $(FED_DEPS_COMPOSE) down
	rm -f $(FED_GENERATED)
	./docker-local/setup.sh
	$(MAKE) --no-print-directory fed-deps-up

fed-deps-down:
	docker compose -f $(FED_DEPS_COMPOSE) down

# Guard: federated deps must be running and every generated file present —
# same four artifacts fed-deps-up checks, plus the shared backend.creds every
# service bind-mounts (require-deps checks it for the single-site stack).
require-fed-deps:
	@docker container inspect -f '{{.State.Running}}' $(FED_NATS_CONTAINER) 2>/dev/null | grep -q true || { \
	  echo "Federated deps are not running. Run 'make fed-deps-up' first."; exit 1; \
	}
	@missing=""; \
	for f in $(NATS_CREDS) $(FED_GENERATED); do \
	  [ -f "$$f" ] || missing="$$missing $$f"; \
	done; \
	if [ -n "$$missing" ]; then \
	  echo "Missing generated file(s):$$missing. Run './docker-local/setup.sh'."; exit 1; \
	fi

# Both sites. Detached, because two Compose projects cannot both hold the
# foreground — use `make fed-logs` for the streaming view `make up` gives you.
fed-up: require-fed-deps
	docker compose -p chat-site-local --env-file $(FED_ENV_LOCAL) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) up -d --build
	docker compose -p chat-site-remote --env-file $(FED_ENV_REMOTE) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) up -d --build $(FED_REMOTE_SERVICES)

# fed-up with the remote peer trimmed to Tier 1.
fed-up-lean:
	$(MAKE) --no-print-directory fed-up FED_REMOTE_SERVICES="$(FED_TIER1)"

fed-down:
	docker compose -p chat-site-remote --env-file $(FED_ENV_REMOTE) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) down
	docker compose -p chat-site-local --env-file $(FED_ENV_LOCAL) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) down

# chat-frontend :3000/:3100, admin-frontend :3001/:3101.
fed-ui-up: require-fed-deps
	docker compose -p chat-site-local-ui --env-file $(FED_ENV_LOCAL) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) up -d --build
	docker compose -p chat-site-remote-ui --env-file $(FED_ENV_REMOTE) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) up -d --build

fed-ui-down:
	docker compose -p chat-site-remote-ui --env-file $(FED_ENV_REMOTE) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) down
	docker compose -p chat-site-local-ui --env-file $(FED_ENV_LOCAL) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) down

# Streaming logs across both site projects.
fed-logs:
	docker compose -p chat-site-local --env-file $(FED_ENV_LOCAL) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) logs -f & \
	pid=$$!; \
	docker compose -p chat-site-remote --env-file $(FED_ENV_REMOTE) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) logs -f; \
	kill $$pid 2>/dev/null || true

# Seed both sites. The directory (users + hr_employee) goes into both
# databases so either portal can resolve any account; room-owned and
# subscriber-owned rows are routed to their home site. See the seeding section
# of docker-local/README.md.
fed-seed: require-fed-deps
	MONGO_DB=chat go run ./tools/seed-sample-data --site site-local --mongo-db chat
	MONGO_DB=chat_remote VALKEY_ADDRS=localhost:6479 \
	  go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote

# fed-seed with --reset: deletes each site's seed records by stable ID before
# re-populating that site (never DROP DATABASE), so hand-added dev data lives.
fed-seed-reset: require-fed-deps
	MONGO_DB=chat go run ./tools/seed-sample-data --site site-local --mongo-db chat --reset
	MONGO_DB=chat_remote VALKEY_ADDRS=localhost:6479 \
	  go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote --reset

# --- Local observability targets ----------------------------------------------
# Two opt-in stacks, safe to run together: o11y-up receives what services export
# under O11Y_ENABLED (:3003); obs-up is cAdvisor + NATS metrics (:3002).
o11y-up:
	@docker network inspect chat-local >/dev/null 2>&1 || { \
	  echo "chat-local network missing. Run 'make deps-up' first."; exit 1; \
	}
	docker compose -f $(O11Y_COMPOSE) up -d

o11y-down:
	docker compose -f $(O11Y_COMPOSE) down

# The same o11y stack against the federated deps: compose.fed-o11y.yaml
# repoints the inherited chat-local key at chat-site-local and puts the
# collector + Prometheus on both site networks. Guards on chat-site-local the
# way o11y-up guards on chat-local.
fed-o11y-up:
	@docker network inspect chat-site-local >/dev/null 2>&1 || { \
	  echo "chat-site-local network missing. Run 'make fed-deps-up' first."; exit 1; \
	}
	docker compose -f $(O11Y_COMPOSE) -f $(FED_O11Y_COMPOSE) up -d

fed-o11y-down:
	docker compose -f $(O11Y_COMPOSE) -f $(FED_O11Y_COMPOSE) down

# Start cAdvisor + Prometheus + Grafana. Requires `make deps-up` first so the
# chat-local network exists. Dashboard at http://localhost:3002.
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
	$(MAKE) --no-print-directory tools-mockgen
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

tools-mockgen:
	GOTOOLCHAIN=$(TOOLS_GO_TOOLCHAIN) go install go.uber.org/mock/mockgen@$(MOCKGEN_VERSION)

# Run all SAST scans (gosec, govulncheck, semgrep) plus the repo-owned semgrep
# rule tests. All always run (no fail-fast) so every category is reported in one
# pass; exits non-zero if any finds an issue. The rule tests run before the scan
# they validate, so a broken rule reads as a rule failure rather than as a
# suspiciously clean scan. This is the exact command CI enforces.
sast:
	@rc=0; g=PASS; v=PASS; s=PASS; t=PASS; \
	$(MAKE) --no-print-directory sast-gosec        || { rc=1; g=FAIL; }; \
	$(MAKE) --no-print-directory sast-vuln         || { rc=1; v=FAIL; }; \
	$(MAKE) --no-print-directory sast-semgrep-test || { rc=1; t=FAIL; }; \
	$(MAKE) --no-print-directory sast-semgrep      || { rc=1; s=FAIL; }; \
	echo "==> SAST summary: gosec=$$g govulncheck=$$v semgrep=$$s rule-tests=$$t"; \
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

# Test the repo-owned rules against their fixtures, so a pattern edit that
# disables a rule fails here instead of silently passing every later scan.
#
# A rule file is tested when a Go fixture of the same basename sits beside it
# (.semgrep/metrics.yml -> .semgrep/metrics.go); rule files without one are
# skipped, so adding fixtures for another rule needs no Makefile change. The
# fixture must be a sibling because semgrep's test runner matches by basename
# and does not support a separate tests directory. Fixtures contain deliberate
# violations, which is why SEMGREP_FLAGS excludes .semgrep from the scan above.
# No fixture at all is a failure — silently testing nothing is the outcome this
# target exists to prevent.
sast-semgrep-test:
	@command -v semgrep >/dev/null 2>&1 || { echo "semgrep not installed — run 'make tools' (needs pipx), or: pipx install semgrep==$(SEMGREP_VERSION)"; exit 1; }
	@rc=0; n=0; \
	for rule in .semgrep/*.yml; do \
	  fixture="$${rule%.yml}.go"; \
	  [ -f "$$fixture" ] || continue; \
	  n=$$((n+1)); \
	  echo "==> semgrep rule tests: $$rule"; \
	  ( cd .semgrep && semgrep scan --test --metrics=off \
	      --config "$$(basename "$$rule")" "$$(basename "$$fixture")" ) || rc=1; \
	done; \
	if [ "$$n" -eq 0 ]; then echo "no semgrep rule fixtures found — expected at least one" >&2; exit 1; fi; \
	exit $$rc

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
