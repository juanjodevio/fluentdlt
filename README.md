# FluentDLT (`fldt`)

**Fluent Data Loading Toolkit** — Write ETL pipelines that read like English

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## 🧠 Philosophy

**FluentDLT** bridges readability and performance. Instead of YAML or heavy orchestration, you write pipelines that read like intent:

```python
from fldt import Fluent

# Each Fluent instance handles one source
Fluent() \
    .from_s3("s3://raw/data/*.jsonl", table_name="events") \
    .to("bigquery", dataset="raw")

Fluent() \
    .from_table("postgresql://user:pw@host/db", "analytics.users", 
                primary_key="id", incremental="updated_at") \
    .to("bigquery", dataset="raw")
```

> "Declarative enough for configs.  
> Explicit enough for Python."

### Core Principles

- **🔄 Fluent** — pipelines read like sentences (`from → to`)
- **⚡ Minimal** — no boilerplate, no configs, just data flows
- **🧩 Composable** — chain multiple sources before sending to a destination
- **🚀 Powered by DLT-Hub** — reliable, scalable pipelines under the hood

---

## 🚀 Quick Start

### Installation

```bash
pip install fldt
```

### Examples

#### Example 1: S3 → Redshift

```python
from fldt import Fluent

Fluent(pipeline_name="s3_to_redshift") \
    .from_s3("s3://data-lake/raw/events/*.csv", table_name="events") \
    .to("redshift", credentials="redshift://user:pw@host:5439/db", dataset="analytics")
```

#### Example 2: Postgres Table → BigQuery

```python
from fldt import Fluent

Fluent() \
    .from_table("postgresql://user:pw@host/db", "public.users", 
                primary_key="id", incremental="updated_at") \
    .to("bigquery", dataset="users_raw", write_disposition="merge")
```

#### Example 3: SQL Query → S3

```python
from fldt import Fluent

Fluent() \
    .from_query(
        "postgresql://user:pw@host/db",
        "SELECT * FROM orders WHERE created_at >= now() - interval '1 day'",
        table_name="recent_orders"
    ) \
    .to("s3", dataset="exports")
```

#### Example 4: Full Database → BigQuery

```python
from fldt import Fluent

# Load entire database (all tables)
Fluent(pipeline_name="full_db_sync") \
    .from_sql_database("postgresql://user:pw@host/db") \
    .to("bigquery", dataset="analytics")

# Or load specific tables only
Fluent() \
    .from_sql_database("postgresql://user:pw@host/db", 
                       tables=["public.users", "public.orders"]) \
    .to("bigquery", dataset="analytics")
```

#### Example 5: DuckDB Table → BigQuery

```python
from fldt import Fluent

# Load a single table from DuckDB
Fluent() \
    .from_table("duckdb:///path/to/source.db", "events", 
                primary_key="id", incremental="timestamp") \
    .to("bigquery", dataset="analytics")
```

#### Example 6: Postgres Table → DuckDB

```python
from fldt import Fluent

# Extract single table from Postgres to local DuckDB
Fluent(pipeline_name="pg_to_local_duckdb") \
    .from_table("postgresql://user:pw@prod-host/db", "public.orders",
                incremental="created_at") \
    .to("duckdb", credentials="duckdb:///data/analytics.db", dataset="raw")
```

---

## 🧩 API Overview

### Core Methods

| Method | Description |
|--------|-------------|
| `Fluent(pipeline_name=None)` | Initialize a new pipeline builder (one source per instance) |
| `.from_s3(url_glob, table_name, file_format=None, ...)` | Load CSV/JSONL/Parquet files from S3 |
| `.from_sql_database(credentials, tables=None, ...)` | Load entire database or specific tables |
| `.from_table(credentials, table, primary_key=None, incremental=None, ...)` | Load a single database table |
| `.from_query(credentials, query, table_name, ...)` | Load from a SQL query |
| `.to(destination, credentials=None, dataset="raw", write_disposition="append", ...)` | Execute the pipeline to destination |

### Source Methods

#### `.from_s3(url_glob, table_name, file_format=None, **kwargs)`

Load files from S3.

**Parameters:**
- `url_glob` (str): S3 URL pattern (e.g., `"s3://bucket/data/*.csv"`)
- `table_name` (str, **required**): Table name for the data
- `file_format` (str, optional): File format (`csv`, `jsonl`, `parquet`) - auto-detected if not provided
- `**kwargs`: Additional arguments passed to DLT filesystem source

**Returns:** Self for method chaining

**Note:** `table_name` must always be explicitly provided. This ensures clear, unambiguous table naming in your data warehouse.

---

#### `.from_sql_database(credentials, tables=None, **kwargs)`

Load entire database or specific tables.

**Parameters:**
- `credentials` (str): Database connection string (e.g., `"postgresql://user:pw@host/db"`, `"duckdb:///data.db"`)
- `tables` (List[str], optional): Specific table names to extract. If None, extracts all tables.
- `**kwargs`: Additional arguments

**Returns:** Self for method chaining

**Raises:** `ValueError` if a source has already been set

---

#### `.from_table(credentials, table, primary_key=None, incremental=None, **kwargs)`

Load a single database table.

**Parameters:**
- `credentials` (str): Database connection string (e.g., `"postgresql://user:pw@host/db"`, `"duckdb:///data.db"`)
- `table` (str): Table name (e.g., `"public.users"` or `"schema.table"`)
- `primary_key` (str, optional): Primary key column name
- `incremental` (str, optional): Incremental column name for incremental loads
- `**kwargs`: Additional arguments

**Returns:** Self for method chaining

**Raises:** `ValueError` if a source has already been set

---

#### `.from_query(credentials, query, table_name, **kwargs)`

Load from a SQL query.

**Parameters:**
- `credentials` (str): Database connection string (e.g., `"postgresql://user:pw@host/db"`, `"duckdb:///data.db"`)
- `query` (str): SQL query to execute
- `table_name` (str): Name for the resulting table
- `**kwargs`: Additional arguments

**Returns:** Self for method chaining

**Raises:** `ValueError` if a source has already been set

---

### Destination Method

#### `.to(destination, credentials=None, dataset="raw", write_disposition="append", **kwargs)`

Execute the pipeline to the specified destination.

**Parameters:**
- `destination` (str): Destination type (`bigquery`, `redshift`, `s3`, `postgres`, `duckdb`, etc.)
- `credentials` (str, optional): Credentials string for the destination
- `dataset` (str): Dataset/schema name (default: `"raw"`)
- `write_disposition` (str): Write mode - `"append"`, `"replace"`, or `"merge"` (default: `"append"`)
- `**kwargs`: Additional destination-specific arguments

**Returns:** Pipeline run result from DLT

**Raises:** `ValueError` if no sources have been specified

---

## 🏗️ Project Structure

```
fluentdlt/
  pyproject.toml
  README.md
  LICENSE
  src/
    fldt/
      __init__.py          # Package exports (Fluent class)
      fluent.py            # Core Fluent class with method chaining
      dlt_adapter.py       # DLT-Hub integration layer
      sources.py           # Source configuration builders
      destinations.py      # Destination configuration builders
  tests/
    test_smoke.py
  .gitignore
```

---

## ⚙️ Installation & Setup

### Using uv (recommended)

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate  # On Unix/macOS
# .venv\Scripts\activate   # On Windows

# Install package in development mode
uv pip install -e .
```

### Using pip

```bash
pip install fldt
```

---

## 🎯 Usage Patterns

### Pattern 1: Single Source → Destination

```python
from fldt import Fluent

# S3 to BigQuery
Fluent() \
    .from_s3("s3://bucket/data/*.parquet", table_name="events") \
    .to("bigquery", dataset="raw")
```

### Pattern 2: Multiple Tables from Database

```python
from fldt import Fluent

# Option 1: Load specific tables
Fluent() \
    .from_sql_database("postgresql://user:pw@host/db",
                       tables=["public.users", "public.orders"]) \
    .to("redshift", credentials="redshift://...", dataset="analytics")

# Option 2: Each table in separate pipeline (for different processing)
Fluent() \
    .from_table("postgresql://user:pw@host/db", "public.users",
                incremental="updated_at") \
    .to("redshift", credentials="redshift://...", dataset="analytics")

Fluent() \
    .from_table("postgresql://user:pw@host/db", "public.orders",
                primary_key="id") \
    .to("redshift", credentials="redshift://...", dataset="analytics")
```

### Pattern 3: Query-Based Extraction

```python
from fldt import Fluent

# Extract using custom SQL query
Fluent(pipeline_name="daily_transactions") \
    .from_query(
        "mysql://user:pw@host/db",
        "SELECT * FROM transactions WHERE date >= CURDATE()",
        table_name="daily_transactions"
    ) \
    .to("bigquery", dataset="warehouse", write_disposition="merge")
```

### Pattern 4: DuckDB for Local Analytics

```python
from fldt import Fluent

# Extract single table from Postgres to local DuckDB for fast analytics
Fluent() \
    .from_table("postgresql://user:pw@host/db", "orders",
                incremental="created_at") \
    .to("duckdb", credentials="duckdb:///data/analytics.db", dataset="staging")

# Or extract entire database
Fluent() \
    .from_sql_database("postgresql://user:pw@host/db") \
    .to("duckdb", credentials="duckdb:///data/analytics.db", dataset="staging")
```

---

## 🔧 Advanced Features

### Incremental Loading

```python
Fluent() \
    .from_table(
        "postgresql://...",
        "events",
        primary_key="id",
        incremental="created_at"  # Only load new records
    ) \
    .to("bigquery", write_disposition="merge")
```

### Custom File Formats

```python
Fluent() \
    .from_s3(
        "s3://bucket/data/*.csv",
        table_name="data",
        file_format="csv"
    ) \
    .to("redshift", dataset="staging")
```

### Write Dispositions

```python
# Append (default)
.to("bigquery", write_disposition="append")

# Replace (truncate and load)
.to("bigquery", write_disposition="replace")

# Merge (upsert based on primary key)
.to("bigquery", write_disposition="merge")
```

---

## 📚 Supported Destinations

FluentDLT supports all DLT destinations:

- **Data Warehouses**: BigQuery, Redshift, Snowflake, Databricks
- **Databases**: PostgreSQL, DuckDB, MotherDuck, MySQL
- **Object Storage**: S3, GCS, Azure Blob Storage
- **And more**: See [DLT documentation](https://dlthub.com/docs/dlt-ecosystem/destinations)

### DuckDB Support

DuckDB is fully supported as both a source and destination:

**As a Source:**
```python
# Single table
Fluent() \
    .from_table("duckdb:///path/to/source.db", "table_name") \
    .to("bigquery", dataset="raw")

# Full database
Fluent() \
    .from_sql_database("duckdb:///path/to/source.db") \
    .to("bigquery", dataset="raw")
```

**As a Destination:**
```python
Fluent() \
    .from_table("postgresql://...", "events") \
    .to("duckdb", credentials="duckdb:///data/warehouse.db", dataset="analytics")
```

**DuckDB In-Memory:**
```python
# Use in-memory DuckDB (no file)
Fluent() \
    .from_s3("s3://bucket/*.csv", table_name="data") \
    .to("duckdb", credentials="duckdb:///:memory:", dataset="temp")
```

**Installation with DuckDB:**
```bash
pip install fldt[duckdb]
# or
uv pip install fldt[duckdb]
```

---

## 🧪 Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=fldt
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## 📄 License

Licensed under the **Apache License 2.0**

© 2025 Juan Palomino M.

> **Note:** FluentDLT builds upon the [DLT-Hub](https://github.com/dlt-hub/dlt) library (Apache 2.0).

---

## ✨ Credits

Built with ❤️ and powered by:

- **[DLT-Hub](https://github.com/dlt-hub/dlt)** — Data loading made simple
- **[Pandas](https://pandas.pydata.org/)** — Data manipulation and analysis
- **[FSSpec](https://filesystem-spec.readthedocs.io/)** — Unified filesystem interface
- **[Typer](https://typer.tiangolo.com/)** — Modern CLI framework

---

**Made with ❤️ by the FluentDLT team**
