DATABASE_URL ?= postgres://runstate:runstate@127.0.0.1:54329/runstate_dev?sslmode=disable
TEST_DATABASE_URL ?= postgres://runstate:runstate@127.0.0.1:54329/runstate_test?sslmode=disable

.PHONY: db-up db-stop db-down db-reset migrate build test

db-up:
	docker compose up -d --wait postgres

db-stop:
	docker compose stop postgres

db-down:
	docker compose down

db-reset:
	docker compose down --volumes

migrate:
	RUNSTATE_DATABASE_URL='$(DATABASE_URL)' go run ./cmd/migrate

build:
	mkdir -p bin
	go build -o bin/runstate-api ./cmd/api
	go build -o bin/runstate-worker ./cmd/worker
	go build -o bin/runstate-scheduler ./cmd/scheduler
	go build -o bin/runstate-migrate ./cmd/migrate

test: db-up build
	RUNSTATE_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' RUNSTATE_WORKER_BINARY='$(CURDIR)/bin/runstate-worker' RUNSTATE_SCHEDULER_BINARY='$(CURDIR)/bin/runstate-scheduler' go test ./... -count=1
