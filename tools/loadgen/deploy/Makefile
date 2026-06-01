ROOT             := ../../..
DEPS_COMPOSE     := $(ROOT)/docker-local/compose.deps.yaml
SERVICES_COMPOSE := $(ROOT)/docker-local/compose.services.yaml
OVERLAY          := docker-compose.yml
COMPOSE          := docker compose -f $(OVERLAY)
WORKLOAD         ?= messages
STEPS            ?=

# Encryption is the default for broadcast-worker; the broadcast-worker compose
# reads ENCRYPTION_ENABLED via env interpolation (defaulting to true). Flip to
# `ENCRYPTION_ENABLED=false make up` for a plaintext comparison run.
export ENCRYPTION_ENABLED ?= true

.PHONY: up stack-up overlay-up seed teardown run run-dashboards run-max-rps down logs seed-members teardown-members reset-members run-sustained run-capacity

up: stack-up overlay-up

# Bring up the shared docker-local stack: third-party deps, then microservices.
stack-up:
	$(MAKE) -C $(ROOT) deps-up
	docker compose -f $(SERVICES_COMPOSE) up -d --build

overlay-up:
	$(COMPOSE) up -d --build

seed:
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen seed --preset=$(PRESET)

teardown:
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen teardown --preset=$(PRESET)

seed-members:
	@test -n "$(PRESET)" || (echo "PRESET=<members-*> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen seed --workload=members --preset=$(PRESET)

teardown-members:
	@test -n "$(PRESET)" || (echo "PRESET=<members-*> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen teardown --workload=members --preset=$(PRESET)

reset-members:
	@test -n "$(PRESET)" || (echo "PRESET=<members-*> required" && exit 1)
	$(MAKE) teardown-members PRESET=$(PRESET)
	$(MAKE) seed-members PRESET=$(PRESET)

run-sustained:
	@test -n "$(PRESET)" || (echo "PRESET=<members-*> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen members-sustained \
	    --preset=$(PRESET) \
	    --rate=$(or $(RATE),100) \
	    --duration=$(or $(DURATION),60s) \
	    --users-per-add=$(or $(USERS_PER_ADD),10) \
	    --inject=$(or $(INJECT),frontdoor) \
	    --shape=$(or $(SHAPE),users)

run-capacity:
	@test -n "$(PRESET)" || (echo "PRESET=<members-*> required" && exit 1)
	@test -n "$(TARGET_SIZE)" || (echo "TARGET_SIZE=<n> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen members-capacity \
	    --preset=$(PRESET) \
	    --target-size=$(TARGET_SIZE) \
	    --users-per-add=$(or $(USERS_PER_ADD),10) \
	    --inject=$(or $(INJECT),frontdoor) \
	    --shape=$(or $(SHAPE),users)

run:
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen run \
	    --preset=$(PRESET) \
	    --rate=$(or $(RATE),500) \
	    --duration=$(or $(DURATION),60s)

run-dashboards:
	$(COMPOSE) --profile dashboards up -d
	$(MAKE) run PRESET=$(PRESET) RATE=$(RATE) DURATION=$(DURATION)

run-max-rps: ## Ramp RPS to find the max under SLO (WORKLOAD=messages|history PRESET=.. STEPS=..)
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen max-rps \
	    --workload=$(WORKLOAD) \
	    --preset=$(PRESET) \
	    $(if $(STEPS),--steps=$(STEPS),)

down:
	$(COMPOSE) --profile dashboards down -v
	docker compose -f $(SERVICES_COMPOSE) down
	$(MAKE) -C $(ROOT) deps-down

logs:
	$(COMPOSE) logs -f loadgen
