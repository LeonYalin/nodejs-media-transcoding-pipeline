.PHONY: up down build-worker logs-worker infra-init dev-api dev load test test-watch test-integration typecheck lint format

up:
	npm run up

down:
	npm run down

build-worker:
	npm run build:worker

logs-worker:
	npm run logs:worker

infra-init:
	npm run infra:init

dev-api:
	npm run dev:api

dev:
	npm run dev

load:
	npm run load

test:
	npm run test

test-watch:
	npm run test:watch

test-integration:
	npm run test:integration

typecheck:
	npm run typecheck

lint:
	npm run lint

format:
	npm run format
