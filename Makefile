# Transactional Outbox - build, test and demo.
#
#   make example   stand everything up and follow the verifying consumer
#   make test      run the test suite (needs Docker for Testcontainers)
#   make demo-failover   kill the active relay and watch the standby take over

COMPOSE ?= docker compose
MVN     ?= mvn

.PHONY: help build test up down example logs logs-relay logs-consumer demo-failover verify psql clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Compile and package all modules (skips tests)
	$(MVN) -B -DskipTests package

test: ## Run the test suite against a real PostgreSQL via Testcontainers
	$(MVN) -B test

up: ## Start PostgreSQL, NATS, two relays, three producers and the consumer
	$(COMPOSE) up -d --build
	@echo "waiting for the stack to settle..."
	@sleep 5
	@$(COMPOSE) ps

example: up logs-consumer ## Stand up the full demo and follow the verifying consumer

logs: ## Follow all logs
	$(COMPOSE) logs -f

logs-relay: ## Follow both relay instances (only one should be publishing)
	$(COMPOSE) logs -f relay-1 relay-2

logs-consumer: ## Follow the consumer, which asserts FIFO ordering
	$(COMPOSE) logs -f consumer

demo-failover: ## Kill relay-1 and watch relay-2 acquire the lease
	$(COMPOSE) kill relay-1
	@echo "relay-1 killed; relay-2 should take the lease within ~10s"
	$(COMPOSE) logs -f relay-2 consumer

verify: ## Prove the guarantees with SQL: no rolled-back order published, none lost
	@$(COMPOSE) exec -T postgres psql -U outbox -d outbox -f - < verify.sql

psql: ## Open a psql shell against the example database
	$(COMPOSE) exec postgres psql -U outbox -d outbox

down: ## Stop everything and remove volumes
	$(COMPOSE) down -v

clean: down ## Stop everything and remove build output
	$(MVN) -B clean
