"""DLT-Hub integration adapter for translating Fluent API to DLT pipelines."""

from typing import List, Dict, Any
import dlt
from dlt.sources import filesystem
from dlt.sources.sql_database import sql_database, sql_table


class DLTAdapter:
    """Adapter class for translating Fluent API calls to DLT pipeline definitions."""

    def __init__(self):
        """Initialize the DLT adapter."""
        pass

    def run_pipeline(
        self,
        pipeline_name: str,
        source: Dict[str, Any],
        destination: Dict[str, Any]
    ) -> Any:
        """Run a DLT pipeline with a single source and destination.

        Args:
            pipeline_name: Name of the pipeline
            source: Source configuration
            destination: Destination configuration

        Returns:
            Pipeline run result
        """
        # Create DLT pipeline
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination["type"],
            dataset_name=destination.get("dataset", "raw")
        )

        # Build DLT source from source configuration
        dlt_source = self._build_dlt_source(source)

        if not dlt_source:
            raise ValueError("No valid source to run")

        # Run the pipeline with the source
        write_disposition = destination.get("write_disposition", "append")
        info = pipeline.run(
            dlt_source,
            write_disposition=write_disposition
        )

        return info

    def _build_dlt_source(self, source_config: Dict[str, Any]) -> Any:
        """Build a DLT source from a source configuration.

        Args:
            source_config: Source configuration dictionary

        Returns:
            DLT source
        """
        source_type = source_config["type"]

        if source_type == "sql_database":
            return self._build_sql_database_source(source_config)
        elif source_type == "table":
            return self._build_table_source(source_config)
        elif source_type == "query":
            return self._build_query_source(source_config)
        elif source_type == "filesystem":
            return self._build_filesystem_source(source_config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")

    def _build_sql_database_source(self, config: Dict[str, Any]) -> Any:
        """Build a DLT sql_database source (full database or specific tables).

        Args:
            config: Source configuration with credentials and optional tables list

        Returns:
            DLT sql_database source
        """
        credentials = config["credentials"]
        tables = config.get("tables")

        # Create database source
        source = sql_database(credentials)

        # If specific tables requested, filter to those
        if tables:
            source = source.with_resources(*tables)

        return source

    def _build_table_source(self, config: Dict[str, Any]) -> Any:
        """Build a DLT source for a table.

        Args:
            config: Source configuration with credentials and table info

        Returns:
            DLT sql_table resource
        """
        credentials = config["credentials"]
        table = config["table"]
        primary_key = config.get("primary_key")
        incremental = config.get("incremental")

        # Create single table resource using sql_table
        resource = sql_table(
            credentials=credentials,
            table=table
        )

        # Apply hints
        hints = {}
        if incremental:
            hints["incremental"] = dlt.sources.incremental(incremental)
        if primary_key:
            hints["primary_key"] = primary_key

        if hints:
            resource = resource.apply_hints(**hints)

        return resource

    def _build_query_source(self, config: Dict[str, Any]) -> Any:
        """Build a DLT source for a query.

        Args:
            config: Source configuration with credentials and query info

        Returns:
            DLT sql_table resource executing the query
        """
        credentials = config["credentials"]
        query = config["query"]
        table_name = config["table_name"]
        primary_key = config.get("primary_key")

        # Use sql_table to execute custom SQL query
        resource = sql_table(
            credentials=credentials,
            table=f"({query})",  # Wrap query as subquery
            table_name=table_name
        )

        # Apply hints if provided
        if primary_key:
            resource = resource.apply_hints(primary_key=primary_key)

        return resource

    def _build_filesystem_source(self, config: Dict[str, Any]) -> Any:
        """Build a filesystem (S3) source.

        Args:
            config: Source configuration

        Returns:
            DLT filesystem source
        """
        url_glob = config["url_glob"]
        file_format = config.get("file_format", "csv")
        table_name = config.get("table_name")

        # Extract bucket URL and file pattern from S3 URL
        # Format: s3://bucket/path/to/files/*.csv
        if url_glob.startswith("s3://"):
            parts = url_glob[5:].split("/", 1)
            bucket = parts[0]
            file_pattern = parts[1] if len(parts) > 1 else "*"
            bucket_url = f"s3://{bucket}"
        else:
            # Assume it's already a bucket URL
            bucket_url = url_glob
            file_pattern = "*"

        # Create filesystem source
        source = filesystem(
            bucket_url=bucket_url,
            file_glob=file_pattern
        )

        # If table name is specified, rename the first resource
        if table_name and hasattr(source, 'resources') and source.resources:
            # Get the first resource and rename it
            first_resource_name = list(source.resources.keys())[0]
            resource = source.resources[first_resource_name]
            # Rename the resource
            source.resources[table_name] = resource.with_name(table_name)
            # Remove the old resource name if it's different
            if first_resource_name != table_name:
                del source.resources[first_resource_name]

        return source

