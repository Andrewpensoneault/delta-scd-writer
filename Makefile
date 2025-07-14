.PHONY: lint type test format all

lint:
	ruff check . --no-cache

type:
	mypy src/ --no-incremental --ignore-missing-imports --cache-dir=/tmp/mypy_cache

test:
	pytest --cov

format:
	ruff format .

all: format lint type test