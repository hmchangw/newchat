COMPOSE ?= docker compose -f user/docker-compose.test.yml

.PHONY: up seed send verify down logs

up:
	$(COMPOSE) up -d --build

seed:
	./test/seed.sh

send:
	@test -n "$(SCENARIO)" || (echo "SCENARIO=<name> required" >&2; exit 1)
	./test/publish.sh $(SCENARIO)

verify:
	@test -n "$(SCENARIO)" || (echo "SCENARIO=<name> required" >&2; exit 1)
	./test/verify/$(SCENARIO).sh

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f broadcast-worker
