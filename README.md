
# CURSOR.md — Build Guide for FluentDLT (`fldt`)

## 🧭 Goal

Build a **fluent, Pythonic DSL wrapper around [DLT-Hub](https://github.com/dlt-hub/dlt)**  
that lets users express pipelines like this:

```python
from fldt import Flow

(Flow(pipeline_name="pg_to_redshift")
  .from_sql_database("postgresql://user:pw@host/db")
  .from_table("public.users", primary_key="id", incremental="updated_at")
  .from_filesystem("s3://bucket/path/*.csv", table_name="events")
  .to_database("redshift", credentials="redshift://user:pw@host:5439/db", dataset="raw")
  .run(write_disposition="merge"))
```

### Requirements
- Fluent API: `.from_*().to_*().run()`
- DLT native sources: `sql_table`, `sql_query`, `filesystem`
- Destinations: database (Redshift, BigQuery, etc.) and filesystem (S3/local)
- Modular architecture: Flow façade + Managers + Plugins
- Type-safe, testable, easily extensible

---

## 🏗️ Project Structure

```
fluentdlt/
  pyproject.toml
  README.md
  LICENSE
  src/
    fldt/
      __init__.py
      flow.py
      state.py
      util/
        logging.py
      managers/
        __init__.py
        sources.py
        destinations.py
        runner.py
      destinations/
        __init__.py
        base.py
        database.py
        filesystem.py
  tests/
    test_smoke.py
  .gitignore
```

---

## ⚙️ pyproject.toml

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "fluentdlt"
version = "0.1.0"
description = "FluentDLT (pkg: fldt): fluent, Pythonic wrapper around dlt-hub for readable ETL flows."
authors = [{name = "Juan Palomino M."}]
readme = "README.md"
requires-python = ">=3.9"
dependencies = [
  "dlt>=1.4.0",
  "typer>=0.12.0",
  "rich>=13.7",
  "pandas>=2.2",
  "fsspec>=2024.6.1",
  "s3fs>=2024.6.0"
]

[tool.setuptools.packages.find]
where = ["src"]
```

---

## 📦 src/fldt/__init__.py

```python
from .flow import Flow
__all__ = ["Flow"]
```

---

## 📘 src/fldt/util/logging.py

```python
import logging
from rich.logging import RichHandler

def get_logger(name: str = "fldt") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = RichHandler(markup=True, rich_tracebacks=True)
        fmt = logging.Formatter("%(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger
```

---

## 🧱 src/fldt/state.py

```python
from dataclasses import dataclass, field
from typing import Any, Optional, List

@dataclass
class DestinationConfig:
    kind: Optional[str] = None          # "database" | "filesystem"
    name: Optional[str] = None          # e.g., "redshift"
    credentials: Optional[str] = None
    dataset: Optional[str] = None
    fs_root: Optional[str] = None

@dataclass
class FlowState:
    pipeline_name: str
    dataset: str
    chunk_size: int
    db_credentials: Optional[str] = None
    resources: List[Any] = field(default_factory=list)
    destination: DestinationConfig = field(default_factory=DestinationConfig)
```

---

## 🧩 src/fldt/managers/sources.py

```python
from typing import Optional
import dlt
from dlt.sources.sql_database import sql_table, sql_query
from dlt.sources.filesystem import filesystem
from ..state import FlowState

class SourceManager:
    def __init__(self, state: FlowState):
        self.state = state

    def set_db_credentials(self, credentials: str):
        self.state.db_credentials = credentials

    def add_table(self, *, table: str, schema: Optional[str], primary_key: Optional[str],
                  incremental: Optional[str], reflection_level: str):
        if not self.state.db_credentials:
            raise ValueError("Call from_sql_database(credentials) before from_table().")
        _table = table.split(".")[-1]
        _schema = schema or (table.split(".")[0] if "." in table else None)
        res = sql_table(
            credentials=self.state.db_credentials,
            table=_table,
            schema=_schema,
            chunk_size=self.state.chunk_size,
            reflection_level=reflection_level,
        )
        if incremental:
            res.apply_hints(incremental=dlt.sources.incremental(incremental))
        if primary_key:
            res.apply_hints(primary_key=primary_key)
        self.state.resources.append(res)

    def add_query(self, *, query: str, table_name: str, primary_key: Optional[str], reflection_level: str):
        if not self.state.db_credentials:
            raise ValueError("Call from_sql_database(credentials) before from_query().")
        res = sql_query(
            credentials=self.state.db_credentials,
            query=query,
            table_name=table_name,
            chunk_size=self.state.chunk_size,
            reflection_level=reflection_level,
        )
        if primary_key:
            res.apply_hints(primary_key=primary_key)
        self.state.resources.append(res)

    def add_filesystem(self, *, include_glob: str, table_name: Optional[str], format: Optional[str],
                       base_url: Optional[str], file_filter: Optional[str], file_glob: Optional[str], options: dict):
        res = filesystem(
            include=include_glob,
            table_name=table_name,
            format=format,
            base_url=base_url,
            file_filter=file_filter,
            file_glob=file_glob,
            options=options,
        )
        self.state.resources.append(res)
```

---

## 📦 src/fldt/managers/destinations.py

```python
from ..state import FlowState, DestinationConfig
from ..destinations.database import DatabaseDestination
from ..destinations.filesystem import FilesystemDestination

class DestinationManager:
    def __init__(self, state: FlowState):
        self.state = state

    def set_database(self, *, name: str, credentials: str | None, dataset: str | None):
        self.state.destination = DestinationConfig(kind="database", name=name,
                                                   credentials=credentials, dataset=dataset)

    def set_filesystem(self, *, root_url: str, dataset: str | None):
        self.state.destination = DestinationConfig(kind="filesystem", fs_root=root_url, dataset=dataset)

    def build(self):
        dest = self.state.destination
        if dest.kind == "database":
            return DatabaseDestination()
        if dest.kind == "filesystem":
            return FilesystemDestination()
        raise ValueError("Destination not configured.")
```

---

## 💾 src/fldt/destinations/base.py

```python
from __future__ import annotations
from typing import Protocol
from ..state import FlowState

class DestinationPlugin(Protocol):
    def prepare_env(self, state: FlowState) -> None: ...
    def destination_name(self, state: FlowState) -> str: ...
    def dataset_name(self, state: FlowState) -> str: ...
```
---

## 🧮 src/fldt/destinations/database.py

```python
import os
from ..state import FlowState
from .base import DestinationPlugin

class DatabaseDestination(DestinationPlugin):
    def prepare_env(self, state: FlowState) -> None:
        if state.destination.credentials:
            os.environ.setdefault("DESTINATION__CREDENTIALS", state.destination.credentials)

    def destination_name(self, state: FlowState) -> str:
        if not state.destination.name:
            raise ValueError("Database destination name not set.")
        return state.destination.name

    def dataset_name(self, state: FlowState) -> str:
        return state.destination.dataset or state.dataset
```
---

## 📁 src/fldt/destinations/filesystem.py

```python
import os
from ..state import FlowState
from .base import DestinationPlugin

class FilesystemDestination(DestinationPlugin):
    def prepare_env(self, state: FlowState) -> None:
        root = state.destination.fs_root
        if not root:
            raise ValueError("Filesystem root_url not provided.")
        os.environ.setdefault("DESTINATION__FILESYSTEM__BUCKET_URL", root)

    def destination_name(self, state: FlowState) -> str:
        return "filesystem"

    def dataset_name(self, state: FlowState) -> str:
        return state.destination.dataset or state.dataset
```
---

## 🚀 src/fldt/managers/runner.py

```python
import dlt
from ..state import FlowState
from ..destinations.base import DestinationPlugin

class PipelineRunner:
    def run(self, state: FlowState, plugin: DestinationPlugin, write_disposition: str):
        if not state.resources:
            raise ValueError("No sources staged.")
        plugin.prepare_env(state)
        pipe = dlt.pipeline(
            pipeline_name=state.pipeline_name,
            destination=plugin.destination_name(state),
            dataset_name=plugin.dataset_name(state),
            full_refresh=False,
        )
        return pipe.run(state.resources, write_disposition=write_disposition)
```
---

## 🧠 src/fldt/flow.py

```python
from .state import FlowState
from .managers.sources import SourceManager
from .managers.destinations import DestinationManager
from .managers.runner import PipelineRunner

class Flow:
    def __init__(self, *, pipeline_name="fluentdlt_pipeline", dataset="raw", chunk_size=50_000):
        self.state = FlowState(pipeline_name=pipeline_name, dataset=dataset, chunk_size=chunk_size)
        self.sources = SourceManager(self.state)
        self.dests = DestinationManager(self.state)
        self.runner = PipelineRunner()

    # Sources
    def from_sql_database(self, credentials: str):
        self.sources.set_db_credentials(credentials)
        return self

    def from_table(self, table: str, *, primary_key=None, incremental=None, schema=None, reflection_level="full_with_precision"):
        self.sources.add_table(table=table, schema=schema, primary_key=primary_key,
                               incremental=incremental, reflection_level=reflection_level)
        return self

    def from_query(self, query: str, table_name: str, *, primary_key=None, reflection_level="full_with_precision"):
        self.sources.add_query(query=query, table_name=table_name,
                               primary_key=primary_key, reflection_level=reflection_level)
        return self

    def from_filesystem(self, include_glob: str, *, table_name=None, format=None, base_url=None, file_filter=None, file_glob=None, options=None):
        self.sources.add_filesystem(include_glob=include_glob, table_name=table_name, format=format,
                                    base_url=base_url, file_filter=file_filter, file_glob=file_glob, options=options or {})
        return self

    # Destinations
    def to_database(self, destination: str, *, credentials=None, dataset=None):
        self.dests.set_database(name=destination, credentials=credentials, dataset=dataset)
        return self

    def to_filesystem(self, root_url: str, *, dataset=None):
        self.dests.set_filesystem(root_url=root_url, dataset=dataset)
        return self

    # Execute
    def run(self, *, write_disposition="append"):
        plugin = self.dests.build()
        return self.runner.run(self.state, plugin, write_disposition=write_disposition)
```
---

## 🧪 tests/test_smoke.py

```python
from fldt import Flow

def test_import_and_chaining():
    f = Flow()
    assert hasattr(f, "from_sql_database")
    assert hasattr(f, "to_database")
```
---

## 🧰 .gitignore

```
__pycache__/
*.pyc
.venv/
dist/
build/
*.egg-info/
```

---

## ✅ Developer Workflow

```bash
uv venv && source .venv/bin/activate
pip install -e .
pytest -q
```

---

## 🚦 Next Tasks (for Cursor)

1. **Typing & Docstrings**: add full type hints and doctrings across all modules.
2. **CLI**: implement Typer CLI `fldt` with commands: `db-table`, `db-query`, `fs-to-db`.
3. **Unit tests**: error cases (no destination, table before db, run without sources) and happy paths (mock `dlt.pipeline`).
4. **YAML fanout**: `fldt run config.yml` compiling to Flow sequences.
5. **DataFrame source**: `from_dataframe(df, table_name)` using `@dlt.resource`.

---

## 🧩 Example Usage

**DB → DB**
```python
from fldt import Flow

(Flow(pipeline_name="pg_to_redshift")
  .from_sql_database("postgresql://user:pw@host/db")
  .from_table("public.users", primary_key="id", incremental="updated_at")
  .to_database("redshift", credentials="redshift://user:pw@host:5439/db", dataset="raw")
  .run(write_disposition="merge"))
```

**FS → DB**
```python
(Flow()
  .from_filesystem("s3://bucket/data/*.csv", table_name="events", format="csv")
  .to_database("bigquery", dataset="raw")
  .run())
```

**DB + FS → FS**
```python
(Flow()
  .from_sql_database("postgresql://user:pw@host/db")
  .from_query("SELECT * FROM orders WHERE created_at >= now() - interval '1 day'", table_name="orders")
  .from_filesystem("/data/logs/*.jsonl", table_name="logs", format="jsonl")
  .to_filesystem("s3://output-bucket/exports/")
  .run())
```

---

## 🧠 Future Ideas

- `FlowConfig` YAML loader (`fldt run config.yml`)
- Cloud deployment via AWS Lambda
- Built-in progress reporting (`rich.progress`)
- Observability hooks / data quality checks
- Async/streaming variants

---

**Author:** Juan Palomino M.  
**License:** Apache-2.0  
**Version:** 0.1.0
