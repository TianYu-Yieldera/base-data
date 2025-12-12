.PHONY: all build test lint clean docker-up docker-down init-db run flink-build flink-test deploy-connectors

GO := go
GOFLAGS := -v
BINARY_DIR := bin
INDEXER_BINARY := $(BINARY_DIR)/indexer
API_BINARY := $(BINARY_DIR)/api
FLINK_DIR := flink-jobs

all: lint test build

build: build-indexer build-api

build-indexer:
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -o $(INDEXER_BINARY) ./cmd/indexer

build-api:
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -o $(API_BINARY) ./cmd/api || true

test:
	$(GO) test -v -race ./...

test-coverage:
	$(GO) test -v -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

clean:
	rm -rf $(BINARY_DIR)
	rm -f coverage.out coverage.html

docker-up:
	docker-compose -f deployments/docker/docker-compose.yml up -d

docker-down:
	docker-compose -f deployments/docker/docker-compose.yml down

docker-logs:
	docker-compose -f deployments/docker/docker-compose.yml logs -f

init-db:
	./scripts/init-db.sh

run: build-indexer
	$(INDEXER_BINARY) --config config/local.yaml

run-backfill: build-indexer
	$(INDEXER_BINARY) --config config/local.yaml --backfill --start-block=$(START_BLOCK) --end-block=$(END_BLOCK)

deps:
	$(GO) mod download
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...
	goimports -w .

generate:
	$(GO) generate ./...

schema-register:
	@echo "Registering Avro schemas..."
	@for schema in schemas/avro/*.avsc; do \
		name=$$(basename $$schema .avsc); \
		echo "Registering $$name"; \
		curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
			--data "{\"schema\": $$(cat $$schema | jq -c . | jq -Rs .)}" \
			http://localhost:8081/subjects/$$name/versions || true; \
	done

help:
	@echo "Available targets:"
	@echo "  all           - Run lint, test, and build"
	@echo "  build         - Build all binaries"
	@echo "  test          - Run tests"
	@echo "  lint          - Run linter"
	@echo "  clean         - Clean build artifacts"
	@echo "  docker-up     - Start Docker services"
	@echo "  docker-down   - Stop Docker services"
	@echo "  init-db       - Initialize database schemas"
	@echo "  run           - Run the indexer locally"
	@echo "  deps          - Download and tidy dependencies"
	@echo ""
	@echo "Phase 2 targets:"
	@echo "  flink-build        - Build Flink jobs JAR"
	@echo "  flink-test         - Run Flink job tests"
	@echo "  deploy-connectors  - Deploy Kafka Connect connectors"
	@echo "  flink-submit-risk  - Submit Risk Processor job to Flink"
	@echo "  flink-submit-flow  - Submit Token Flow job to Flink"
	@echo "  flink-submit-dq    - Submit DQ Monitor job to Flink"

# Phase 2: Flink Jobs
flink-build:
	cd $(FLINK_DIR) && sbt clean assembly

flink-test:
	cd $(FLINK_DIR) && sbt test

flink-clean:
	cd $(FLINK_DIR) && sbt clean

# Deploy Kafka Connect connectors
deploy-connectors:
	./scripts/deploy-connectors.sh

# Submit Flink jobs
FLINK_JAR := $(FLINK_DIR)/target/scala-2.12/base-data-flink-assembly.jar
FLINK_JOBMANAGER := localhost:8084

flink-submit-risk: flink-build
	docker exec base-data-flink-jobmanager flink run -d \
		-c com.basedata.flink.job.RiskProcessorJob \
		/opt/flink/usrlib/base-data-flink-assembly.jar

flink-submit-flow: flink-build
	docker exec base-data-flink-jobmanager flink run -d \
		-c com.basedata.flink.job.TokenFlowAggregatorJob \
		/opt/flink/usrlib/base-data-flink-assembly.jar

flink-submit-dq: flink-build
	docker exec base-data-flink-jobmanager flink run -d \
		-c com.basedata.flink.job.DQMonitorJob \
		/opt/flink/usrlib/base-data-flink-assembly.jar

flink-list:
	docker exec base-data-flink-jobmanager flink list -r

flink-cancel:
	docker exec base-data-flink-jobmanager flink cancel $(JOB_ID)
