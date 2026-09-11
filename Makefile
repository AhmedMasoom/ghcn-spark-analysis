.PHONY: help install lint format test clean notebook

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package with dev + geo extras (editable)
	pip install -e ".[geo,dev]"

lint:  ## Lint with ruff
	ruff check src tests

format:  ## Auto-format with black + ruff import sort
	black src tests
	ruff check --fix src tests

test:  ## Run unit tests
	pytest

notebook:  ## Launch Jupyter
	jupyter notebook notebooks/

clean:  ## Remove caches and build artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
