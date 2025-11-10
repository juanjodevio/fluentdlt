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
        self.sources: List[Dict[str, Any]] = []
        self._current_db_source: Optional[Dict[str, Any]] = None
        self.dlt_adapter = DLTAdapter()

    def from_s3(
        self,
        url_glob: str,
        table_name: Optional[str] = None,
        file_format: Optional[str] = None,
        **kwargs
    ) -> "Fluent":
        """Load files from S3.

        Args:
            url_glob: S3 URL pattern (e.g., "s3://bucket/data/*.csv")
            table_name: Optional table name for the data
            file_format: Optional file format (csv, jsonl, parquet, etc.)
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining
        """
        source_config = SourceBuilder.build_filesystem_source(
            url_glob=url_glob,
            table_name=table_name,
            file_format=file_format,
            **kwargs
        )
        self.sources.append(source_config)
        return self

    def from_db(self, credentials: str) -> "Fluent":
        """Set database credentials and create a sql_database source.

        This sets up the context for subsequent from_table() and from_query() calls.

        Args:
            credentials: Database connection string
                       (e.g., "postgresql://user:pw@host/db")

        Returns:
            Self for method chaining
        """
        # Create a new database source
        self._current_db_source = SourceBuilder.build_sql_database_source(
            credentials=credentials
        )
        # Add it to sources list
        self.sources.append(self._current_db_source)
        return self

    def from_table(
        self,
        table: str,
        primary_key: Optional[str] = None,
        incremental: Optional[str] = None,
        **kwargs
    ) -> "Fluent":
        """Add a database table resource to the current sql_database source.

        Must be called after from_db().

        Args:
            table: Table name (e.g., "public.users" or "schema.table")
            primary_key: Optional primary key column name
            incremental: Optional incremental column name for incremental loads
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining

        Raises:
            ValueError: If from_db() has not been called first
        """
        if not self._current_db_source:
            raise ValueError(
                "Database credentials must be set using from_db() before from_table()"
            )

        # Add table resource to the current database source
        SourceBuilder.add_table_resource(
            source=self._current_db_source,
            table=table,
            primary_key=primary_key,
            incremental=incremental,
            **kwargs
        )
        return self

    def from_query(
        self,
        query: str,
        table_name: str,
        **kwargs
    ) -> "Fluent":
        """Add a query-based resource to the current sql_database source.

        Must be called after from_db().

        Args:
            query: SQL query to execute
            table_name: Name for the resulting table
            **kwargs: Additional arguments passed to the source

        Returns:
            Self for method chaining

        Raises:
            ValueError: If from_db() has not been called first
        """
        if not self._current_db_source:
            raise ValueError(
                "Database credentials must be set using from_db() before from_query()"
            )

        # Add query resource to the current database source
        SourceBuilder.add_query_resource(
            source=self._current_db_source,
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
        if not self.sources:
            raise ValueError("At least one source must be specified before calling to()")

        destination_config = DestinationBuilder.build_destination(
            destination=destination,
            credentials=credentials,
            dataset=dataset,
            write_disposition=write_disposition,
            **kwargs
        )

        return self.dlt_adapter.run_pipeline(
            pipeline_name=self.pipeline_name,
            sources=self.sources,
            destination=destination_config
        )

