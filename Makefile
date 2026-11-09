MODULES := pkg services/gateway services/recommender services/ingestor

.PHONY: build test vet lint bench fmt tidy up down smoke load-test

build:
	@for m in $(MODULES); do (cd $$m && go build ./...); done

test:
	@for m in $(MODULES); do (cd $$m && go test -race -count=1 ./...); done

vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...); done

lint:
	golangci-lint run ./...

bench:
	cd services/recommender && go test -bench=. -benchtime=1s -run='^$$' ./internal/engine/

fmt:
	@for m in $(MODULES); do (cd $$m && gofmt -l -w .); done

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy); done

# --- Local stack -------------------------------------------------------------
# Docker runtime on this machine is colima: ensure the VM is up first.

colima-up:
	@colima status >/dev/null 2>&1 || colima start

up: colima-up
	cd deploy && docker compose up --build -d

down:
	cd deploy && docker compose down -v

smoke: ## end-to-end check against the local stack
	./scripts/smoke.sh

load-test: ## requires: go install github.com/tsenart/vegeta@latest
	./scripts/loadtest.sh

seed: ## generate synthetic interactions through the public API
	./scripts/seed.sh
