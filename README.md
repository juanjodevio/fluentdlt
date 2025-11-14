# FluentDLT (`fldt`)

**Fluent Data Loading Toolkit** — Write ETL pipelines that read like English

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=juanjodevio_fluentdlt&metric=alert_status)](https://sonarcloud.io/dashboard?id=juanjodevio_fluentdlt)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=juanjodevio_fluentdlt&metric=security_rating)](https://sonarcloud.io/dashboard?id=juanjodevio_fluentdlt)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=juanjodevio_fluentdlt&metric=bugs)](https://sonarcloud.io/dashboard?id=juanjodevio_fluentdlt)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=juanjodevio_fluentdlt&metric=code_smells)](https://sonarcloud.io/dashboard?id=juanjodevio_fluentdlt)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=juanjodevio_fluentdlt&metric=coverage)](https://sonarcloud.io/dashboard?id=juanjodevio_fluentdlt)

---

## 🧠 Philosophy

**FluentDLT** (`fldt`) is a fluent interface wrapper for [dlt (data load tool)](https://dlthub.com) that makes building data pipelines intuitive and readable. Write pipelines that read like English, with full type safety and comprehensive error handling.

```python
from fldt import FluentPipeline

# Load from SQL, transform, and send to data warehouse
result = (FluentPipeline
    .from_sql_table("postgresql://user:pass@localhost/db", "users")
    .add_transformer(clean_data)
    .with_incremental("updated_at")
    .to("duckdb")
    .run())
```

### Core Principles

- **🔄 Fluent** — Pipelines read like sentences with method chaining
- **⚡ Simple** — Minimal boilerplate, maximum clarity
- **🛡️ Type-Safe** — Full type hints and validation
- **🎯 Explicit** — No magic, clear error messages
- **🚀 Powered by DLT** — Reliable, scalable pipelines under the hood

---

## 🚀 Quick Start

### Installation

```bash
# Basic installation
pip install fldt

# With SQL database support
pip install 'fldt[sql_database]'

# With DuckDB destination
pip install 'fldt[duckdb]'
```

### Basic Example

```python
from fldt import FluentPipeline

# Simple pipeline from raw data to DuckDB
data = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 87},
]

result = (FluentPipeline
    .from_source(data)
    .to("duckdb")
    .with_dataset("analytics")
    .run())
```

---

## 📖 API Reference

### Factory Methods

Create pipelines from various sources:

#### `FluentPipeline.from_source(source)`

Create pipeline from any dlt-compatible source (dlt sources, callables, iterables, raw data).

```python
# From raw data
FluentPipeline.from_source([{"id": 1}, {"id": 2}])

# From callable
FluentPipeline.from_source(lambda: fetch_data())

# From dlt source
FluentPipeline.from_source(my_dlt_source())
```

#### `FluentPipeline.from_sql_table(connection, table, schema=None, **kwargs)`

Load a single SQL table using SQLAlchemy connection.

**Parameters:**
- `connection`: SQLAlchemy Engine or connection string
- `table`: Table name to load
- `schema`: Optional schema name
- `**kwargs`: Additional dlt sql_table arguments

```python
FluentPipeline.from_sql_table(
    "postgresql://user:pass@localhost/db",
    "users",
    schema="public"
)
```

#### `FluentPipeline.from_sql_query(connection, query, **kwargs)`

Load data from a custom SQL query.

**Parameters:**
- `connection`: SQLAlchemy Engine or connection string
- `query`: SQL query to execute
- `**kwargs`: Additional dlt sql_database arguments

```python
FluentPipeline.from_sql_query(
    "postgresql://user:pass@localhost/db",
    "SELECT * FROM users WHERE active = true"
)
```

#### `FluentPipeline.from_sql_database(connection, schema=None, **kwargs)`

Load entire SQL database (all tables).

**Parameters:**
- `connection`: SQLAlchemy Engine or connection string
- `schema`: Optional schema name
- `**kwargs`: Additional dlt sql_database arguments

```python
FluentPipeline.from_sql_database(
    "postgresql://user:pass@localhost/db",
    schema="public"
)
```

### Fluent API Methods

Chain these methods to configure your pipeline:

#### `.to(destination)`

Set the pipeline destination.

```python
.to("duckdb")
.to("postgres")
.to("bigquery")
```

#### `.add_transformer(func)`

Add a transformation function. Transformers are applied sequentially.

```python
def uppercase_names(data):
    for item in data:
        if "name" in item:
            item["name"] = item["name"].upper()
    return data

pipeline.add_transformer(uppercase_names)
```

#### `.with_incremental(cursor_field, initial_value=None, primary_key=None, row_order="asc")`

Configure incremental loading.

```python
.with_incremental("updated_at")
.with_incremental("id", initial_value=1000)
.with_incremental("timestamp", primary_key=["user_id", "id"])
```

#### `.with_name(name)`

Set pipeline name.

```python
.with_name("daily_user_sync")
```

#### `.with_dataset(name)`

Set dataset name for destination.

```python
.with_dataset("analytics")
```

#### `.with_options(**kwargs)`

Set additional pipeline options.

```python
.with_options(dev_mode=True, write_disposition="replace")
```

#### `.run(adapter=None)`

Build and execute the pipeline.

```python
result = pipeline.run()
```

---

## 💡 Usage Examples

### Example 1: Raw Data to DuckDB

```python
from fldt import FluentPipeline

data = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 87},
    {"id": 3, "name": "Charlie", "score": 92},
]

result = (FluentPipeline
    .from_source(data)
    .to("duckdb")
    .with_dataset("students")
    .run())
```

### Example 2: SQL Table with Transformations

```python
from fldt import FluentPipeline

def clean_data(records):
    """Clean and validate data."""
    for record in records:
        record["name"] = record["name"].strip().title()
        record["email"] = record["email"].lower()
    return records

result = (FluentPipeline
    .from_sql_table("postgresql://localhost/db", "users")
    .add_transformer(clean_data)
    .to("duckdb")
    .with_dataset("clean_users")
    .run())
```

### Example 3: Incremental Loading

```python
from fldt import FluentPipeline

result = (FluentPipeline
    .from_sql_table("postgresql://localhost/db", "events")
    .with_incremental("created_at", initial_value="2024-01-01")
    .to("duckdb")
    .with_dataset("events")
    .run())
```

### Example 4: Custom SQL Query

```python
from fldt import FluentPipeline

query = """
    SELECT 
        user_id,
        COUNT(*) as order_count,
        SUM(total) as total_spent
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY user_id
"""

result = (FluentPipeline
    .from_sql_query("postgresql://localhost/db", query)
    .to("duckdb")
    .with_dataset("user_metrics")
    .run())
```

### Example 5: Entire Database Sync

```python
from fldt import FluentPipeline

result = (FluentPipeline
    .from_sql_database("postgresql://localhost/db", schema="public")
    .to("duckdb")
    .with_name("full_db_sync")
    .with_dataset("mirror")
    .run())
```

### Example 6: Multiple Transformations

```python
from fldt import FluentPipeline

def add_timestamp(data):
    from datetime import datetime
    for item in data:
        item["processed_at"] = datetime.utcnow().isoformat()
    return data

def filter_active(data):
    return [item for item in data if item.get("active", True)]

result = (FluentPipeline
    .from_sql_table("postgresql://localhost/db", "users")
    .add_transformer(filter_active)
    .add_transformer(add_timestamp)
    .to("duckdb")
    .run())
```

### Example 7: Complex Pipeline with All Options

```python
from fldt import FluentPipeline

def enrich_data(records):
    """Add computed fields."""
    for record in records:
        record["full_name"] = f"{record['first_name']} {record['last_name']}"
        record["age_group"] = "adult" if record["age"] >= 18 else "minor"
    return records

result = (FluentPipeline
    .from_sql_table(
        "postgresql://user:pass@prod.example.com/app_db",
        "customers",
        schema="sales"
    )
    .add_transformer(enrich_data)
    .with_incremental(
        cursor_field="updated_at",
        initial_value="2024-01-01",
        primary_key="customer_id"
    )
    .with_name("customer_sync")
    .with_dataset("analytics")
    .with_options(write_disposition="merge")
    .to("duckdb")
    .run())
```

---

## 🏗️ Architecture

FluentDLT follows SOLID principles with clean separation of concerns:

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Code                                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FluentPipeline                              │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Factory Methods:                                        │   │
│  │  • from_source()        • from_sql_query()              │   │
│  │  • from_sql_table()     • from_sql_database()           │   │
│  │                                                          │   │
│  │  Fluent API:                                            │   │
│  │  • to()                 • with_name()                   │   │
│  │  • add_transformer()    • with_dataset()                │   │
│  │  • with_incremental()   • with_options()                │   │
│  │  • run()                                                │   │
│  └──────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │ delegates to
                            ▼
        ┌───────────────────────────────────────┐
        │      PipelineBuilder                   │
        │  • Validates configuration            │
        │  • Constructs PipelineConfig          │
        │  • Returns immutable config           │
        └───────────────────┬───────────────────┘
                            │ config
                            ▼
        ┌───────────────────────────────────────┐
        │     PipelineExecutor                   │
        │  • Orchestrates execution             │
        │  • Applies transformations            │
        │  • Manages pipeline lifecycle         │
        └───────────────────┬───────────────────┘
                            │ uses
                            ▼
        ┌───────────────────────────────────────┐
        │    PipelineAdapter (Protocol)          │
        │  • create_pipeline()                  │
        │  • run_pipeline()                     │
        │  • apply_transformations()            │
        └───────────────────┬───────────────────┘
                            │ implements
                            ▼
        ┌───────────────────────────────────────┐
        │         DltAdapter                     │
        │  • Lazy-loads dlt modules             │
        │  • Translates to dlt API              │
        │  • Handles dlt-specific logic         │
        └───────────────────┬───────────────────┘
                            │ calls
                            ▼
        ┌───────────────────────────────────────┐
        │          dlt (data load tool)          │
        │  • Pipeline creation                  │
        │  • Data extraction                    │
        │  • Transformation execution           │
        │  • Destination loading                │
        └───────────────────────────────────────┘

Supporting Components:
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  TransformerChain    │  │  Type System         │  │  Exceptions          │
│  • Sequential apply  │  │  • SourceType        │  │  • ValidationError   │
│  • Error handling    │  │  • DestinationType   │  │  • ConfigError       │
│  • Composition       │  │  • PipelineConfig    │  │  • ExecutionError    │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### Components

- **FluentPipeline** - Main user-facing API with fluent interface
- **PipelineBuilder** - Validates and constructs pipeline configuration
- **PipelineExecutor** - Orchestrates pipeline execution
- **TransformerChain** - Manages sequential data transformations
- **DltAdapter** - Integrates with dlt (lazy-loaded)
- **Type System** - Full type hints for IDE support

### Design Principles

- **Single Responsibility** - Each class has one clear purpose
- **Open/Closed** - Extensible via adapter protocol
- **Dependency Inversion** - Depends on abstractions (PipelineAdapter protocol)
- **Fail Fast** - Validates at configuration time, not execution time
- **Explicit over Implicit** - Clear APIs, no magic

---

## 🛡️ Error Handling

FluentDLT provides clear, actionable error messages:

```python
from fldt import FluentPipeline
from fldt.exceptions import ValidationError, PipelineConfigurationError

try:
    result = (FluentPipeline
        .from_source(data)
        .to("duckdb")
        .run())
except ValidationError as e:
    print(f"Invalid input: {e}")
except PipelineConfigurationError as e:
    print(f"Configuration error: {e}")
```

### Exception Hierarchy

- `FluentDLTError` - Base exception
  - `ValidationError` - Invalid input parameters
  - `PipelineConfigurationError` - Incomplete or invalid configuration
  - `PipelineExecutionError` - Runtime execution failures
  - `AdapterError` - Adapter-specific errors

---

## 📚 Supported Destinations

FluentDLT supports all dlt destinations:

- **Data Warehouses**: BigQuery, Redshift, Snowflake, Databricks
- **Databases**: PostgreSQL, DuckDB, MySQL, SQLite
- **Object Storage**: S3, GCS, Azure Blob Storage
- **Files**: CSV, JSON, Parquet

See [dlt documentation](https://dlthub.com/docs/dlt-ecosystem/destinations) for complete list.

---

## 🧪 Testing

### Unit Tests (Fast, Mock-Based)

```bash
# Run all unit tests
pytest tests/unittest

# With coverage (matches CI)
pytest tests/unittest --cov=src/fldt --cov-report=term-missing --cov-report=xml:coverage.xml --cov-branch

# Specific test file
pytest tests/unittest/test_fluent.py -v

# Lint (black, isort, mypy)
uv run black --check src tests
uv run isort --profile black --check-only src tests
uv run mypy src tests
```

### Integration Tests (Real Databases)

Integration tests use Alembic to create test databases with synthetic data.

**Quick Start (SQLite - No Setup Required):**
```bash
# Install integration dependencies
uv sync --group integration

# Run all integration tests
pytest tests/integration -m integration
```

**With PostgreSQL (Optional):**
```bash
# Set PostgreSQL connection
export TEST_POSTGRES_URL="postgresql://user:pass@localhost/test_db"

# Run integration tests
pytest tests/integration -m integration
```

**Test Database Management:**
```bash
# Migrations are automatic, but you can run manually:
cd tests/integration
alembic upgrade head    # Create schema and seed data
alembic downgrade base  # Clean up
```

### All Tests

```bash
# Run everything (unit + integration)
pytest
```

---

## 🏛️ Project Structure

```
fluentdlt/
├── src/fldt/
│   ├── __init__.py           # Public API exports
│   ├── fluent.py             # FluentPipeline (main API)
│   ├── builder.py            # PipelineBuilder
│   ├── executor.py           # PipelineExecutor
│   ├── transformers.py       # TransformerChain
│   ├── types.py              # Type definitions
│   ├── exceptions.py         # Exception hierarchy
│   └── adapters/
│       ├── protocol.py       # PipelineAdapter protocol
│       └── dlt_adapter.py    # DLT implementation
├── tests/
│   ├── unittest/             # Unit tests (189 tests, mock-based)
│   │   ├── test_fluent.py
│   │   ├── test_builder.py
│   │   ├── test_executor.py
│   │   ├── test_transformers.py
│   │   ├── test_adapters.py
│   │   ├── test_types.py
│   │   ├── test_exceptions.py
│   │   └── conftest.py       # Shared fixtures
│   └── integration/          # Integration tests (15 tests, Alembic-managed)
│       ├── alembic/          # Database migrations
│       │   ├── versions/     # Migration files (schema + data)
│       │   ├── env.py        # Alembic environment
│       │   └── script.py.mako
│       ├── alembic.ini       # Alembic configuration
│       ├── conftest.py       # Database fixtures (SQLite + PostgreSQL)
│       ├── test_data.py      # Test data constants and validators
│       └── test_integration.py
├── pyproject.toml            # Project configuration
├── README.md                 # This file
└── LICENSE                   # Apache 2.0 License
```

---

## 🤝 Contributing

Contributions are welcome! Please ensure:

1. All tests pass (`pytest`)
2. Code is type-checked (`mypy src/fldt --strict`)
3. Code is formatted (`black src tests`)
4. Code is linted (`ruff check src tests`)

---

## 📄 License

Licensed under the **Apache License 2.0**

© 2025 Juan Palomino M.

> **Note:** FluentDLT builds upon the [dlt](https://github.com/dlt-hub/dlt) library (Apache 2.0).

---

## ✨ Credits

Built with ❤️ and powered by:

- **[dlt](https://github.com/dlt-hub/dlt)** — Data load tool
- **[SQLAlchemy](https://www.sqlalchemy.org/)** — SQL toolkit
- **[Python](https://www.python.org/)** — The best programming language

---

**Made with ❤️ for the data engineering community**
