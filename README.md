
**Fluent Data Loading Toolkit** — Write ETL pipelines that read like English

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)


---

## 🧠 Philosophy

**FluentDLT** bridges readability and performance. Instead of YAML or heavy orchestration, you write pipelines that read like intent:

```python
Fluent() \
    .from_s3("s3://raw/data/*.jsonl") \
    .from_table("analytics.events") \
    .to("bigquery")
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

---

## 🧩 API Overview

| Method | Description |
|--------|-------------|
| `.from_s3(url_glob, table_name=None, file_format=None, ...)` | Load CSV/JSONL files from S3 |
| `.from_db(credentials)` | Set DB connection for subsequent sources |
| `.from_table(table, primary_key=None, incremental=None)` | Add a table to extract |
| `.from_query(query, table_name, ...)` | Add a query-based resource |
| `.to(destination, credentials=None, dataset="raw", write_disposition="append")` | Run the pipeline |

---

## ⚙️ CLI Usage

```bash
# Load from S3 to Redshift
fldt s3 s3://bucket/data/*.csv \
    --destination redshift \
    --credentials "redshift://user:pw@host/db"

# Load database table to Redshift with merge
fldt db_table "postgresql://user:pw@host/db" public.users \
    --destination redshift \
    --write-disposition merge
```

---

## ✨ Credits

Built with ❤️ and powered by:

- **[DLT-Hub](https://github.com/dlt-hub/dlt)** — Data loading made simple
- **[Pandas](https://pandas.pydata.org/)** — Data manipulation and analysis
- **[FSSpec](https://filesystem-spec.readthedocs.io/)** — Unified filesystem interface
- **[Typer](https://typer.tiangolo.com/)** — Modern CLI framework

---

## 🧾 License

Licensed under the **Apache License 2.0**

© 2025 Juan Palomino M.

> **Note:** FluentDLT builds upon the [DLT-Hub](https://github.com/dlt-hub/dlt) library (Apache 2.0).


**Made with ❤️ by the FluentDLT team**
