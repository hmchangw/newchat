ROOT             := ../../..
OVERLAY          := docker-compose.yml
COMPOSE          := docker compose -f $(OVERLAY)
WORKLOAD         ?= messages
STEPS            ?=

# Encryption is the default for broadcast-worker; the broadcast-worker compose
# reads ENCRYPTION_ENABLED via env interpolation (defaulting to true). Flip to
# `ENCRYPTION_ENABLED=false make up` for a plaintext comparison run.
export ENCRYPTION_ENABLED ?= true

.PHONY: up stack-up overlay-up seed teardown run run-dashboards run-max-rps run-daily down logs seed-members teardown-members reset-members run-sustained run-capacity seed-roomread teardown-roomread seed-botroom teardown-botroom run-max-room-size

up: stack-up overlay-up

# Bring up the shared docker-local stack via the root Makefile, so the services
# bring-up lives in exactly one place (root `up-detached`) and never drifts from
# `make up`. Third-party deps first, then the microservices (detached).
stack-up:
	$(MAKE) -C $(ROOT) deps-up
	$(MAKE) -C $(ROOT) up-detached

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

seed-roomread:
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen seed --workload=room-read --preset=$(PRESET)

teardown-roomread:
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen teardown --workload=room-read --preset=$(PRESET)

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

run-max-rps: ## Ramp RPS to find the max under SLO (WORKLOAD=messages|thread|history|room-read|read-receipt PRESET=.. STEPS=..)
	@test -n "$(PRESET)" || (echo "PRESET=<name> required" && exit 1)
	$(COMPOSE) --profile dashboards up -d cadvisor prometheus
	$(COMPOSE) exec -T loadgen /loadgen max-rps \
	    --workload=$(WORKLOAD) \
	    --preset=$(PRESET) \
	    $(if $(STEPS),--steps=$(STEPS),)

comma := ,
run-daily:
	@test -n "$(PRESET)" || (echo "PRESET=<daily-light|daily-heavy|daily-power> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen daily \
	    --preset=$(PRESET) \
	    --steps=$(or $(STEPS),1000$(comma)2000$(comma)5000$(comma)10000$(comma)20000) \
	    --hold=$(or $(HOLD),180s) \
	    --csv=/results/daily-$(PRESET)-$$(date +%Y%m%d-%H%M%S).csv

down:
	$(COMPOSE) --profile dashboards down -v
	$(MAKE) -C $(ROOT) down
	$(MAKE) -C $(ROOT) deps-down

seed-botroom:
	@test -n "$(PRESET)" || (echo "PRESET=<botroom-*> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen seed --workload=botroom --preset=$(PRESET)

teardown-botroom:
	@test -n "$(PRESET)" || (echo "PRESET=<botroom-*> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen teardown --workload=botroom --preset=$(PRESET)

run-max-room-size: ## Ramp room size to find the largest a bot can blast (PRESET=.. RATE=.. SIZES=.. ROOMS_PER_SIZE=..)
	@test -n "$(PRESET)" || (echo "PRESET=<botroom-*> required" && exit 1)
	@test -n "$(RATE)" || (echo "RATE=<msgs/sec> required" && exit 1)
	$(COMPOSE) exec -T loadgen /loadgen max-room-size \
	    --preset=$(PRESET) \
	    --rate=$(RATE) \
	    $(if $(SIZES),--sizes=$(SIZES),) \
	    --rooms-per-size=$(or $(ROOMS_PER_SIZE),4) \
	    --reads=$(or $(READS),0) \
	    --hold=$(or $(HOLD),180s) \
	    --csv=/results/botroom-$(PRESET)-$$(date +%Y%m%d-%H%M%S).csv

logs:
	$(COMPOSE) logs -f loadgen
