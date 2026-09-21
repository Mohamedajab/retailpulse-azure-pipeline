.PHONY: setup run test lint
setup:
	python -m pip install -e ".[dev]"
run:
	python -m retailpulse.pipeline
test:
	python -m pytest -q
lint:
	python -m ruff check src tests

