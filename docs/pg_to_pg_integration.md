# Postgres <-> Postgres Integration Scenario

## Objective
Validate a full FluentDLT pipeline that reads from a PostgreSQL source database and lands the same dataset into a separate PostgreSQL destination. The test must reuse the canonical integration schema (users, events, products) and run both locally (Docker) and in CI.

## Scope
- **Source DB**: PostgreSQL seeded via existing Alembic migrations under `tests/integration/alembic`.
- **Destination DB**: Fresh PostgreSQL instance with matching schema inferred/created by dlt.
- **Tables**: `users`, `events`, `products` (full copy). Incremental cursor coverage on `users.updated_at`.
- **Transformations**: At least one record-level transformer (e.g., uppercase names) to ensure transformer chain executes inside PostgreSQL scenario.

## Pipeline Flow
1. Bring up two Postgres containers (`pg_source`, `pg_dest`) using Docker Compose.
2. Run Alembic migrations against `pg_source` to create and seed schema.
3. Execute a FluentPipeline configured as:
   ```python
   (
       FluentPipeline.from_sql_database(SRC_PG_URL, schema="public")
       .add_transformer(uppercase_names)
       .with_incremental("users.updated_at", initial_value="2024-01-01 00:00:00")
       .to("postgres", destination_config=DEST_PG_URL)
       .with_dataset("integration_pg_to_pg")
   )
   ```
4. Pipeline uses `dlt`'s Postgres destination (same credentials as `DEST_PG_URL`).
5. Validate that destination tables contain the canonical record counts/fields (reuse helpers in `tests/integration/test_data.py`).

## Environment Variables
- `SRC_PG_URL`: SQLAlchemy URL to source PostgreSQL. Default `postgresql+psycopg://fldt:fldt@localhost:5532/fldt_source`.
- `DEST_PG_URL`: SQLAlchemy URL to destination PostgreSQL. Default `postgresql+psycopg://fldt:fldt@localhost:5542/fldt_dest`.
- `DLT_DATA_DIR`: Temp directory per test run to isolate dlt state.

## Test Entry Point
- New pytest module `tests/integration/test_pg_to_pg.py` marked with `@pytest.mark.integration` and `@pytest.mark.postgres`.
- Fixture order:
  1. `pg_source_service` (docker-compose) exposes port + seeds migrations.
  2. `pg_dest_service` provides empty destination.
  3. `postgres_urls` fixture returns both URLs plus Alembic command helper.

## Success Criteria
- Pipeline run succeeds with non-empty `LoadInfo`.
- Destination DB row counts match `TestData` expectations.
- Incremental rerun (second test) only loads delta rows.
- Transformers visibly mutate destination rows (e.g., uppercase names).
- Test suite can be triggered locally via `docker compose up pg-source pg-dest && pytest tests/integration -m postgres`.

## Future Work
- Extend coverage to Postgres → DuckDB and DuckDB → Postgres permutations.
- Add CDC/backfill scenarios once base pipeline stabilizes.


