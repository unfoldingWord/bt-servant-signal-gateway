.PHONY: install dev lint format format-check typecheck test check

install:
	uv sync

dev:
	uv run python -m bt_signal_gateway

lint:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run ty check

test:
	uv run pytest

check: format-check lint typecheck test
