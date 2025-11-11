"""Core Fluent class for building ELT pipelines."""

from typing import Optional, List, Dict, Any
from fldt.dlt_adapter import DLTAdapter
from fldt.sources import SourceBuilder
from fldt.destinations import DestinationBuilder


class Fluent:
    """Fluent interface for building ELT pipelines."""

    def __init__(self, pipeline_name: Optional[str] = None):
        """Initialize a new Fluent pipeline builder.

        Args:
            pipeline_name: Optional name for the pipeline. If not provided,
                          a default name will be generated.
        """
        self.pipeline_name = pipeline_name or "fluent_pipeline"
        self.source: Optional[Dict[str, Any]] = None
        self.dlt_adapter = DLTAdapter()

    def from_s3(
        self,
        url_glob: str,
        table_name: str,
        file_format: Optional[str] = None,
        **kwargs
    ) -> "Fluent":
        """Load files from S3.

        Args:
            url_glob: S3 URL pattern (e.g., "s3://bucket/data/*.csv")
            table_name: Table name for the data (required)
            file_format: Optional file format (csv, jsonl, parquet, etc.)
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a source has already been set
        """
        if self.source is not None:
            raise ValueError("Only one source per Fluent instance is allowed")

        self.source = SourceBuilder.build_filesystem_source(
            url_glob=url_glob,
            table_name=table_name,
            file_format=file_format,
            **kwargs
        )
        return self

    def from_sql_database(
        self,
        credentials: str,
        tables: Optional[List[str]] = None,
        **kwargs
    ) -> "Fluent":
        """Load from a SQL database (all tables or specific tables).

        Args:
            credentials: Database connection string
                       (e.g., "postgresql://user:pw@host/db", "duckdb:///path/to/db")
            tables: Optional list of specific table names to extract
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a source has already been set
        """
        if self.source is not None:
            raise ValueError("Only one source per Fluent instance is allowed")

        self.source = SourceBuilder.build_sql_database_source(
            credentials=credentials,
            tables=tables,
            **kwargs
        )
        return self

    def from_table(
        self,
        credentials: str,
        table: str,
        primary_key: Optional[str] = None,
        incremental: Optional[str] = None,
        **kwargs
    ) -> "Fluent":
        """Load a single database table.

        Args:
            credentials: Database connection string
                       (e.g., "postgresql://user:pw@host/db", "duckdb:///path/to/db")
            table: Table name (e.g., "public.users" or "schema.table")
            primary_key: Optional primary key column name
            incremental: Optional incremental column name for incremental loads
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a source has already been set
        """
        if self.source is not None:
            raise ValueError("Only one source per Fluent instance is allowed")

        self.source = SourceBuilder.build_table_source(
            credentials=credentials,
            table=table,
            primary_key=primary_key,
            incremental=incremental,
            **kwargs
        )
        return self

    def from_query(
        self,
        credentials: str,
        query: str,
        table_name: str,
        **kwargs
    ) -> "Fluent":
        """Load from a SQL query.

        Args:
            credentials: Database connection string
                       (e.g., "postgresql://user:pw@host/db", "duckdb:///path/to/db")
            query: SQL query to execute
            table_name: Name for the resulting table
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a source has already been set
        """
        if self.source is not None:
            raise ValueError("Only one source per Fluent instance is allowed")

        self.source = SourceBuilder.build_query_source(
            credentials=credentials,
            query=query,
            table_name=table_name,
            **kwargs
        )
        return self

    def to(
        self,
        destination: str,
        credentials: Optional[str] = None,
        dataset: str = "raw",
        write_disposition: str = "append",
        **kwargs
    ) -> Any:
        """Execute the pipeline to the specified destination.

        Args:
            destination: Destination type (bigquery, redshift, s3, etc.)
            credentials: Optional credentials string for the destination
            dataset: Dataset/schema name (default: "raw")
            write_disposition: Write mode: "append", "replace", or "merge" (default: "append")
            **kwargs: Additional arguments passed to the destination

        Returns:
            Pipeline run result from DLT

        Raises:
            ValueError: If no sources have been specified
        """
        if self.source is None:
            raise ValueError("A source must be specified before calling to()")

        destination_config = DestinationBuilder.build_destination(
            destination=destination,
            credentials=credentials,
            dataset=dataset,
            write_disposition=write_disposition,
            **kwargs
        )

        return self.dlt_adapter.run_pipeline(
            pipeline_name=self.pipeline_name,
            source=self.source,
            destination=destination_config
        )

