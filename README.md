# FluentDLT (`fldt`)

**Fluent Data Loading Toolkit** — Write ETL pipelines that read like English

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

---

## 🧠 Philosophy

**FluentDLT** bridges readability and performance. Instead of YAML or heavy orchestration, you write pipelines that read like intent:

```python
from fldt import Fluent

Fluent() \
    .from_s3("s3://raw/data/*.jsonl", table_name="events") \
    .from_db("postgresql://user:pw@host/db") \
    .from_table("analytics.users", primary_key="id", incremental="updated_at") \
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

#### Example 2: Postgres → BigQuery

```python
from fldt import Fluent

Fluent() \
    .from_db("postgresql://user:pw@host/db") \
    .from_table("public.users", primary_key="id", incremental="updated_at") \
    .to("bigquery", dataset="users_raw", write_disposition="merge")
```

#### Example 3: Query-based Load

```python
from fldt import Fluent

Fluent() \
    .from_db("postgresql://user:pw@host/db") \
    .from_query(
        "SELECT * FROM orders WHERE created_at >= now() - interval '1 day'",
        table_name="recent_orders"
    ) \
    .to("s3", dataset="exports")
```

#### Example 4: Multiple Sources

```python
from fldt import Fluent

Fluent(pipeline_name="multi_source_pipeline") \
    .from_s3("s3://bucket/events/*.csv", table_name="events") \
    .from_db("postgresql://user:pw@host/db") \
    .from_table("public.users") \
    .from_query("SELECT * FROM orders WHERE status = 'active'", table_name="active_orders") \
    .to("bigquery", dataset="analytics")
```

---

## 🧩 API Overview

### Core Methods

| Method | Description |
|--------|-------------|
| `Fluent(pipeline_name=None)` | Initialize a new pipeline builder |
| `.from_s3(url_glob, table_name, file_format=None, ...)` | Load CSV/JSONL/Parquet files from S3 |
| `.from_db(credentials)` | Set database credentials for subsequent table/query sources |
| `.from_table(table, primary_key=None, incremental=None, ...)` | Add a database table to extract |
| `.from_query(query, table_name, ...)` | Add a SQL query-based resource |
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

#### `.from_db(credentials)`

Set database credentials and create a sql_database source context for subsequent table/query operations.

**Parameters:**
- `credentials` (str): Database connection string (e.g., `"postgresql://user:pw@host/db"`)

**Returns:** Self for method chaining

**Note:** Must be called before `.from_table()` or `.from_query()`

---

#### `.from_table(table, primary_key=None, incremental=None, **kwargs)`

Add a database table resource to the current sql_database source.

**Parameters:**
- `table` (str): Table name (e.g., `"public.users"` or `"schema.table"`)
- `primary_key` (str, optional): Primary key column name
- `incremental` (str, optional): Incremental column name for incremental loads
- `**kwargs`: Additional arguments

**Returns:** Self for method chaining

**Raises:** `ValueError` if `from_db()` has not been called first

---

#### `.from_query(query, table_name, **kwargs)`

Add a SQL query-based resource to the current sql_database source.

**Parameters:**
- `query` (str): SQL query to execute
- `table_name` (str): Name for the resulting table
- `**kwargs`: Additional arguments

**Returns:** Self for method chaining

**Raises:** `ValueError` if `from_db()` has not been called first

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

### Pattern 2: Database Context with Multiple Tables

```python
from fldt import Fluent

# Multiple tables from same database
Fluent() \
    .from_db("postgresql://user:pw@host/db") \
    .from_table("public.users", incremental="updated_at") \
    .from_table("public.orders", primary_key="id") \
    .to("redshift", credentials="redshift://...", dataset="analytics")
```

### Pattern 3: Mixed Sources

```python
from fldt import Fluent

# Combine filesystem and database sources
Fluent(pipeline_name="mixed_sources") \
    .from_s3("s3://bucket/logs/*.jsonl", table_name="logs") \
    .from_db("mysql://user:pw@host/db") \
    .from_query(
        "SELECT * FROM transactions WHERE date >= CURDATE()",
        table_name="daily_transactions"
    ) \
    .to("bigquery", dataset="warehouse", write_disposition="merge")
```

---

## 🔧 Advanced Features

### Incremental Loading

```python
Fluent() \
    .from_db("postgresql://...") \
    .from_table(
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
- **Databases**: PostgreSQL, DuckDB, MotherDuck
- **Object Storage**: S3, GCS, Azure Blob Storage
- **And more**: See [DLT documentation](https://dlthub.com/docs/dlt-ecosystem/destinations)

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
