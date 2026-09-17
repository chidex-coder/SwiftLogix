PY := ./.venv/bin/python
export PYTHONPATH := src

.PHONY: help venv run run-long test query costs clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

venv: ## create the virtualenv and install dependencies
	python3.12 -m venv .venv && ./.venv/bin/pip install -q -r requirements.txt

run: ## 90s live pipeline with two scripted drift incidents (random timing)
	$(PY) -m swiftlogix.cli run --duration 90 --eps 900 --fresh

run-long: ## 3 minutes at higher throughput
	$(PY) -m swiftlogix.cli run --duration 180 --eps 2000 --fresh

test: ## run the test suite
	$(PY) -m pytest tests/ -q

query: ## consumer-facing analytics against the loaded warehouse
	$(PY) -m swiftlogix.cli query

costs: ## print the monthly cost model
	$(PY) -m swiftlogix.cli costs

clean: ## wipe the generated lake and warehouse
	rm -rf data/
