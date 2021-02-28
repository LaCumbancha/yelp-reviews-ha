SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON := /usr/bin/python3.8
GIT_REMOTE = github.com/LaCumbancha/yelp-review-ha

PROJECT_NAME = tp4
GREEN = \033[0;32m
RESET = \033[0m

DOCKER_COMPOSE_YAML = dockerized-system.yaml

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	# Inputs
	GOOS=linux go build -o bin/business-scatter $(GIT_REMOTE)/cmd/nodes/inputs/business-scatter

	# Mappers
	GOOS=linux go build -o bin/funbiz-mapper $(GIT_REMOTE)/cmd/nodes/mappers/funbiz-mapper
	GOOS=linux go build -o bin/funcit-mapper $(GIT_REMOTE)/cmd/nodes/mappers/citbiz-mapper
	GOOS=linux go build -o bin/weekday-mapper $(GIT_REMOTE)/cmd/nodes/mappers/weekday
	GOOS=linux go build -o bin/user-mapper $(GIT_REMOTE)/cmd/nodes/mappers/user
	GOOS=linux go build -o bin/stars-mapper $(GIT_REMOTE)/cmd/nodes/mappers/stars
	GOOS=linux go build -o bin/hash-mapper $(GIT_REMOTE)/cmd/nodes/mappers/hash-text

	# Filters
	GOOS=linux go build -o bin/funbiz-filter $(GIT_REMOTE)/cmd/nodes/filters/funbiz-filter
	GOOS=linux go build -o bin/user-filter $(GIT_REMOTE)/cmd/nodes/filters/user
	GOOS=linux go build -o bin/stars-filter $(GIT_REMOTE)/cmd/nodes/filters/stars

	# Aggregators
	GOOS=linux go build -o bin/funbiz-aggregator $(GIT_REMOTE)/cmd/nodes/aggregators/funbiz-aggregator
	GOOS=linux go build -o bin/funcit-top $(GIT_REMOTE)/cmd/nodes/aggregators/top-funniest-cities
	GOOS=linux go build -o bin/weekday-aggregator $(GIT_REMOTE)/cmd/nodes/aggregators/weekday
	GOOS=linux go build -o bin/user-aggregator $(GIT_REMOTE)/cmd/nodes/aggregators/user
	GOOS=linux go build -o bin/stars-aggregator $(GIT_REMOTE)/cmd/nodes/aggregators/stars
	GOOS=linux go build -o bin/dishash-aggregator $(GIT_REMOTE)/cmd/nodes/aggregators/bots

	# Joiners
	GOOS=linux go build -o bin/funcit-joiner $(GIT_REMOTE)/cmd/nodes/joiners/funny-city
	GOOS=linux go build -o bin/bestuser-joiner $(GIT_REMOTE)/cmd/nodes/joiners/best-users

	# Prettiers
	GOOS=linux go build -o bin/top-funniest-cities $(GIT_REMOTE)/cmd/nodes/prettiers/top-funniest-cities
	GOOS=linux go build -o bin/weekday-histogram $(GIT_REMOTE)/cmd/nodes/prettiers/weekday-histogram
	GOOS=linux go build -o bin/top-users $(GIT_REMOTE)/cmd/nodes/prettiers/top-users
	GOOS=linux go build -o bin/best-users $(GIT_REMOTE)/cmd/nodes/prettiers/best-users
	GOOS=linux go build -o bin/bot-users $(GIT_REMOTE)/cmd/nodes/prettiers/bot-users

	# Outputs
	GOOS=linux go build -o bin/sink $(GIT_REMOTE)/cmd/nodes/outputs/sink

	# Monitors
	GOOS=linux go build -o bin/monitor $(GIT_REMOTE)/cmd/nodes/montiros/common
.PHONY: build

system-build:
	# RabbitMQ
	docker build -f ./cmd/nodes/rabbitmq/Dockerfile -t "rabbitmq:custom" .

	# Inputs
	docker build -f ./cmd/nodes/inputs/business-scatter/Dockerfile -t "biz_scatter:latest" .

	# Mappers
	docker build -f ./cmd/nodes/mappers/funny-business/Dockerfile -t "funbiz_mapper:latest" .
	docker build -f ./cmd/nodes/mappers/city-business/Dockerfile -t "citbiz_mapper:latest" .
	docker build -f ./cmd/nodes/mappers/weekday/Dockerfile -t "weekday_mapper:latest" .
	docker build -f ./cmd/nodes/mappers/user/Dockerfile -t "user_mapper:latest" .
	docker build -f ./cmd/nodes/mappers/stars/Dockerfile -t "stars_mapper:latest" .
	docker build -f ./cmd/nodes/mappers/hash-text/Dockerfile -t "hash_mapper:latest" .

	# Filters
	docker build -f ./cmd/nodes/filters/funny-business/Dockerfile -t "funbiz_filter:latest" .
	docker build -f ./cmd/nodes/filters/user/Dockerfile -t "user_filter:latest" .
	docker build -f ./cmd/nodes/filters/stars/Dockerfile -t "stars_filter:latest" .

	# Aggregators
	docker build -f ./cmd/nodes/aggregators/funny-business/Dockerfile -t "funbiz_aggregator:latest" .
	docker build -f ./cmd/nodes/aggregators/top-funniest-cities/Dockerfile -t "funcit_top:latest" .
	docker build -f ./cmd/nodes/aggregators/weekday/Dockerfile -t "weekday_aggregator:latest" .
	docker build -f ./cmd/nodes/aggregators/user/Dockerfile -t "user_aggregator:latest" .
	docker build -f ./cmd/nodes/aggregators/stars/Dockerfile -t "stars_aggregator:latest" .
	docker build -f ./cmd/nodes/aggregators/bots/Dockerfile -t "bots_aggregator:latest" .

	# Joiners
	docker build -f ./cmd/nodes/joiners/funny-city/Dockerfile -t "funcit_joiner:latest" .
	docker build -f ./cmd/nodes/joiners/best-users/Dockerfile -t "best_joiner:latest" .

	# Prettiers
	docker build -f ./cmd/nodes/prettiers/funniest-cities/Dockerfile -t "funcit_prettier:latest" .
	docker build -f ./cmd/nodes/prettiers/weekday-histogram/Dockerfile -t "weekday_prettier:latest" .
	docker build -f ./cmd/nodes/prettiers/top-users/Dockerfile -t "top_prettier:latest" .
	docker build -f ./cmd/nodes/prettiers/best-users/Dockerfile -t "best_prettier:latest" .
	docker build -f ./cmd/nodes/prettiers/bot-users/Dockerfile -t "bots_prettier:latest" .

	# Outputs
	docker build -f ./cmd/nodes/outputs/sink/Dockerfile -t "sink:latest" .

	# Monitors
	docker build -f ./cmd/nodes/monitors/common/Dockerfile -t "monitor:latest" .
.PHONY: system-build

system-hard-up: system-build
	$(PYTHON) ./scripts/system-builder
	docker-compose -f $(DOCKER_COMPOSE_YAML) --project-name $(PROJECT_NAME) up -d --build --remove-orphans
.PHONY: system-hard-up

system-soft-up:
	$(PYTHON) ./scripts/system-builder
	docker-compose -f $(DOCKER_COMPOSE_YAML) --project-name $(PROJECT_NAME) up -d --build --remove-orphans
.PHONY: system-soft-up

system-down:
	docker-compose -f $(DOCKER_COMPOSE_YAML) --project-name $(PROJECT_NAME) stop -t 1
	docker-compose -f $(DOCKER_COMPOSE_YAML) --project-name $(PROJECT_NAME) down
.PHONY: system-down

system-logs:
	docker-compose -f $(DOCKER_COMPOSE_YAML) --project-name $(PROJECT_NAME) logs -f
.PHONY: system-logs

system-connect:
	docker container exec -it $(service) /bin/sh
.PHONY: system-connect

system-test:
	$(SHELL) ./scripts/system-test
.PHONE: system-test

system-failing:
	$(eval DOWN_SERVICES := $(shell ./scripts/system-failures $(failures)))
	@for service in $(DOWN_SERVICES); do \
		echo -n "Stopping service $$service... " ; \
		docker stop $$service > /dev/null ; \
		echo -e "$(GREEN)done$(RESET)" ; \
	done
.PHONY: system-failing
