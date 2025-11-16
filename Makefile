format:
	uv sync --group dev
	uv run isort --profile black src tests
	uv run black src tests

unittest:
	uv run pytest tests/unittest

mypy:
	uv sync --group dev
	uv run mypy src tests

integration:
	uv sync --group integration
	docker compose -f docker-compose.pg.yml up -d
	uv run pytest tests/integration
	docker compose -f docker-compose.pg.yml down -v