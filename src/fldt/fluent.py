"""Fluent pipeline interface for fldt.

This module provides the FluentPipeline class, which is the main user-facing
API for constructing and executing data pipelines with a fluent interface.
"""

import logging
from typing import Any

from fldt.adapters.dlt_adapter import DltAdapter
from fldt.builder import PipelineBuilder
from fldt.executor import PipelineExecutor
from fldt.types import ConnectionType, DestinationType, SourceType, TransformerFunc

logger = logging.getLogger(__name__)

_SQL_DATABASE_IMPORT_ERROR = (
    "dlt sql_database not available. Install with: pip install 'dlt[sql_database]'"
)


class FluentPipeline:
    """Main fluent interface for building and executing data pipelines.

    FluentPipeline provides an ergonomic, chainable API for constructing
    data pipelines. It combines the Builder pattern with method chaining
    to create pipelines that read like English.

    The class delegates configuration to PipelineBuilder and execution to
    PipelineExecutor, focusing solely on providing a clean user interface.

    Example:
        ```python
        # Basic pipeline
        result = (FluentPipeline
            .from_source(my_data)
            .to("duckdb")
            .run())

        # With transformations
        result = (FluentPipeline
            .from_source(my_data)
            .add_transformer(lambda x: x * 2)
            .to("postgres")
            .run())

        # SQL convenience methods
        result = (FluentPipeline
            .from_sql_table("postgresql://...", "users")
            .add_transformer(clean_data)
            .with_incremental("updated_at")
            .to("duckdb")
            .run())
        ```
    """

    def __init__(self, builder: PipelineBuilder | None = None) -> None:
        """Initialize a FluentPipeline.

        Args:
            builder: Optional existing PipelineBuilder. If None, creates new one.
        """
        self._builder = builder or PipelineBuilder()
        logger.debug("Initialized FluentPipeline")

    @classmethod
    def from_source(cls, source: SourceType) -> "FluentPipeline":
        """Create a pipeline from a generic data source.

        This is the most flexible factory method, accepting any dlt-compatible
        source including dlt sources, callables, iterables, and raw data.

        Args:
            source: Data source to extract from.

        Returns:
            New FluentPipeline instance with source configured.

        Raises:
            ValidationError: If source is invalid.

        Example:
            ```python
            # From raw data
            FluentPipeline.from_source([{"id": 1}, {"id": 2}])

            # From callable
            FluentPipeline.from_source(lambda: fetch_data())

            # From dlt source
            FluentPipeline.from_source(my_dlt_source())
            ```
        """
        builder = PipelineBuilder()
        builder.set_source(source)
        logger.info("Created pipeline from generic source")
        return cls(builder)

    @classmethod
    def from_sql_table(
        cls,
        connection: ConnectionType,
        table: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> "FluentPipeline":
        """Create a pipeline from a single SQL table.

        Convenience method for loading data from a single database table
        using dlt's sql_database source.

        Args:
            connection: SQLAlchemy Engine or connection string.
            table: Name of the table to load.
            schema: Optional schema name. If None, uses default schema.
            **kwargs: Additional arguments passed to dlt.sources.sql_database.

        Returns:
            New FluentPipeline instance with SQL table source.

        Raises:
            ValidationError: If connection or table is invalid.
            AdapterError: If dlt sql_database is not available.

        Example:
            ```python
            FluentPipeline.from_sql_table(
                "postgresql://user:pass@localhost/db",
                "users",
                schema="public"
            ).to("duckdb").run()
            ```
        """
        try:
            from dlt.sources.sql_database import sql_table
        except ImportError as e:
            from fldt.exceptions import AdapterError

            raise AdapterError(_SQL_DATABASE_IMPORT_ERROR) from e

        logger.info(
            "Creating pipeline from SQL table",
            extra={"table": table, "schema": schema},
        )

        # Create dlt source
        source = sql_table(
            credentials=connection,
            table=table,
            schema=schema,
            **kwargs,
        )

        builder = PipelineBuilder()
        builder.set_source(source)
        return cls(builder)

    @classmethod
    def from_sql_query(
        cls,
        connection: ConnectionType,
        query: str,
        **kwargs: Any,
    ) -> "FluentPipeline":
        """Create a pipeline from a custom SQL query.

        Convenience method for loading data from a custom SQL query
        using dlt's sql_database source.

        Args:
            connection: SQLAlchemy Engine or connection string.
            query: SQL query to execute.
            **kwargs: Additional arguments passed to dlt.sources.sql_database.

        Returns:
            New FluentPipeline instance with SQL query source.

        Raises:
            ValidationError: If connection or query is invalid.
            AdapterError: If dlt sql_database is not available.

        Example:
            ```python
            FluentPipeline.from_sql_query(
                "postgresql://user:pass@localhost/db",
                "SELECT * FROM users WHERE active = true"
            ).to("duckdb").run()
            ```
        """
        try:
            from dlt.sources.sql_database import sql_database
        except ImportError as e:
            from fldt.exceptions import AdapterError

            raise AdapterError(_SQL_DATABASE_IMPORT_ERROR) from e

        if not query or not isinstance(query, str):
            from fldt.exceptions import ValidationError

            raise ValidationError("Query must be a non-empty string")

        logger.info("Creating pipeline from SQL query")

        # Create dlt source with custom query
        source = sql_database(
            credentials=connection,
            **kwargs,
        ).with_resources(query)

        builder = PipelineBuilder()
        builder.set_source(source)
        return cls(builder)

    @classmethod
    def from_sql_database(
        cls,
        connection: ConnectionType,
        schema: str | None = None,
        **kwargs: Any,
    ) -> "FluentPipeline":
        """Create a pipeline from an entire SQL database.

        Convenience method for loading all tables from a database
        using dlt's sql_database source.

        Args:
            connection: SQLAlchemy Engine or connection string.
            schema: Optional schema name. If None, uses default schema.
            **kwargs: Additional arguments passed to dlt.sources.sql_database.

        Returns:
            New FluentPipeline instance with SQL database source.

        Raises:
            ValidationError: If connection is invalid.
            AdapterError: If dlt sql_database is not available.

        Example:
            ```python
            FluentPipeline.from_sql_database(
                "postgresql://user:pass@localhost/db",
                schema="public"
            ).to("duckdb").run()
            ```
        """
        try:
            from dlt.sources.sql_database import sql_database
        except ImportError as e:
            from fldt.exceptions import AdapterError

            raise AdapterError(_SQL_DATABASE_IMPORT_ERROR) from e

        logger.info(
            "Creating pipeline from SQL database",
            extra={"schema": schema},
        )

        # Create dlt source for entire database
        source = sql_database(
            credentials=connection,
            schema=schema,
            **kwargs,
        )

        builder = PipelineBuilder()
        builder.set_source(source)
        return cls(builder)

    def to(self, destination: DestinationType) -> "FluentPipeline":
        """Set the destination for the pipeline.

        Args:
            destination: Target destination (string name or dlt destination).

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If destination is invalid.

        Example:
            ```python
            pipeline.to("duckdb")
            pipeline.to("postgres")
            pipeline.to("bigquery")
            ```
        """
        self._builder.set_destination(destination)
        return self

    def add_transformer(self, transformer: TransformerFunc) -> "FluentPipeline":
        """Add a transformation function to the pipeline.

        Transformations are applied in the order they are added, with each
        transformer receiving the output of the previous one.

        Args:
            transformer: Callable that transforms data.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If transformer is not callable.

        Example:
            ```python
            pipeline.add_transformer(lambda x: x * 2)
            pipeline.add_transformer(clean_data)
            ```
        """
        self._builder.add_transformer(transformer)
        return self

    def with_incremental(
        self,
        cursor_field: str,
        initial_value: Any = None,
        primary_key: str | list[str] | None = None,
        row_order: str = "asc",
        **kwargs: Any,
    ) -> "FluentPipeline":
        """Configure incremental loading for the pipeline.

        Args:
            cursor_field: Field to use as cursor (e.g., 'updated_at').
            initial_value: Starting value for the cursor.
            primary_key: Primary key field(s) for deduplication.
            row_order: Row ordering - 'asc' or 'desc'.
            **kwargs: Additional incremental loading options.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If configuration is invalid.

        Example:
            ```python
            pipeline.with_incremental("updated_at")
            pipeline.with_incremental("id", initial_value=1000)
            pipeline.with_incremental("timestamp", primary_key=["user_id", "id"])
            ```
        """
        self._builder.set_incremental(
            cursor_field=cursor_field,
            initial_value=initial_value,
            primary_key=primary_key,
            row_order=row_order,
            **kwargs,
        )
        return self

    def with_name(self, name: str) -> "FluentPipeline":
        """Set the pipeline name.

        Args:
            name: Pipeline name.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If name is invalid.

        Example:
            ```python
            pipeline.with_name("daily_user_sync")
            ```
        """
        self._builder.set_pipeline_name(name)
        return self

    def with_dataset(self, name: str) -> "FluentPipeline":
        """Set the dataset name.

        Args:
            name: Dataset name for the destination.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If name is invalid.

        Example:
            ```python
            pipeline.with_dataset("analytics")
            ```
        """
        self._builder.set_dataset_name(name)
        return self

    def with_options(self, **kwargs: Any) -> "FluentPipeline":
        """Set pipeline options.

        Args:
            **kwargs: Pipeline options as keyword arguments.

        Returns:
            Self for method chaining.

        Example:
            ```python
            pipeline.with_options(dev_mode=True, write_disposition="replace")
            ```
        """
        self._builder.set_options(kwargs)
        return self

    def run(self, adapter: DltAdapter | None = None) -> Any:
        """Build and execute the pipeline.

        This method builds the configuration, creates an executor with the
        provided (or default) adapter, and runs the pipeline.

        Args:
            adapter: Optional adapter to use. If None, uses DltAdapter.

        Returns:
            Execution result from the adapter.

        Raises:
            PipelineConfigurationError: If configuration is incomplete.
            PipelineExecutionError: If execution fails.

        Example:
            ```python
            # Use default DltAdapter
            result = pipeline.run()

            # Use custom adapter
            result = pipeline.run(my_adapter)
            ```
        """
        # Build configuration
        config = self._builder.build()

        # Use provided adapter or create default DltAdapter
        execution_adapter = adapter or DltAdapter()

        # Create executor and run
        executor = PipelineExecutor(execution_adapter)
        logger.info("Executing pipeline")
        result = executor.execute(config)

        logger.info("Pipeline execution completed")
        return result

    def __repr__(self) -> str:
        """Return string representation of the pipeline."""
        return f"FluentPipeline(builder={repr(self._builder)})"
