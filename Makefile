PY ?= python3
VENV := .venv
BIN := $(VENV)/bin
SPEED ?= 1

.PHONY: install test info dataset up down logs listen listen-json bench-storage clean

install:            ## create a virtualenv and install the project with dev extras
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -U pip
	$(BIN)/pip install -e ".[dev]"

test:               ## run the unit tests
	$(BIN)/pytest -q

info:               ## print the plant hierarchy
	$(BIN)/unslake info

dataset:            ## simulate one day of the factory into data/day-1 (Parquet)
	$(BIN)/unslake simulate --mode file --duration 1d --out data/day-1

up:                 ## start the MQTT broker and the factory (Sparkplug B) in Docker
	SPEED=$(SPEED) docker compose up -d --build

down:               ## stop everything
	docker compose --profile debug --profile json down

logs:               ## follow the factory logs
	docker compose logs -f factory

listen:             ## decode Sparkplug B traffic from the running factory
	$(BIN)/unslake listen --mode sparkplug

listen-json:        ## print plain UNS JSON traffic (start with: docker compose --profile json up -d)
	$(BIN)/unslake listen --mode mqtt --topic "acme-plant/#"

bench-storage:      ## experiment 01: storage footprint of one day of sensor data
	$(BIN)/python benchmarks/storage_footprint.py --duration 1d --repeats 3

clean:
	rm -rf data .pytest_cache
