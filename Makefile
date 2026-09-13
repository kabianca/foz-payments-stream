.DEFAULT_GOAL := help
.PHONY: help init up down logs produce kill status check prove test lint clean reset

PYTHON ?= .venv/bin/python
COUNT  ?= 300
RATE   ?= 30
LATE   ?= 0.10
DUP    ?= 0.05
SEED   ?= 42

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

init:  ## Create .env and the data folders
	@test -f .env || (cp .env.example .env && \
	  sed -i "s/^FOZ_UID=.*/FOZ_UID=$$(id -u)/" .env && \
	  echo "created .env (FOZ_UID=$$(id -u))")
	@mkdir -p data/delta data/checkpoints
	@echo "ready — now run: make up"

up: init  ## Start Kafka and the streaming job
	docker compose up -d --build stream
	@echo "stream is consuming; try: make produce && make status"

down:  ## Stop everything (keeps Kafka data, Delta tables and the checkpoint)
	docker compose down

logs:  ## Tail the streaming job
	docker compose logs -f stream

produce:  ## Send synthetic events: make produce COUNT=300 RATE=30 LATE=0.10 DUP=0.05 SEED=42
	docker compose run --rm --build producer \
	  --count $(COUNT) --rate $(RATE) --late $(LATE) --dup $(DUP) --seed $(SEED)

kill:  ## kill -9 the Spark JVM mid-flight; Docker restarts it from the checkpoint
	docker compose exec stream pkill -9 java || true
	@echo "killed; watch it come back with: make logs"

status:  ## Table sizes and the last batches (reads ./data from the host)
	FOZ_DELTA_ROOT=data/delta $(PYTHON) -m foz.check --status

check:  ## Verify every invariant over the Delta tables
	FOZ_DELTA_ROOT=data/delta $(PYTHON) -m foz.check

prove: up  ## The whole argument in one run: produce, kill, produce again, check
	./scripts/prove.sh

test:  ## Run the test suite (no Docker, no Kafka; needs Java 17+)
	$(PYTHON) -m pytest -q

lint:  ## Static checks
	$(PYTHON) -m ruff check . || true
	$(PYTHON) -m ruff format --check . || true

clean:  ## Remove Delta tables and the checkpoint (keeps Kafka)
	rm -rf data/delta data/checkpoints

reset: down  ## Destroy volumes, tables and checkpoint and start from nothing
	docker compose down -v
	rm -rf data/delta data/checkpoints
