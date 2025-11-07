"""DLT-Hub integration adapter for translating Fluent API to DLT pipelines."""

from typing import List, Dict, Any, Optional
import dlt
from dlt.sources import filesystem
from dlt.sources.sql_database import sql_database


class DLTAdapter:
    """Adapter class for translating Fluent API calls to DLT pipeline definitions."""

    def __init__(self):
        """Initialize the DLT adapter."""
        pass

    def run_pipeline(
        self,
        pipeline_name: str,
        sources: List[Dict[str, Any]],
        destination: Dict[str, Any]
    ) -> Any:
        """Run a DLT pipeline with the given sources and destination.

        Args:
            pipeline_name: Name of the pipeline
            sources: List of source configurations
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

        # Build DLT sources from source configurations
        # DLT expects sources (which contain resources), so we'll combine them
        dlt_sources = []
        for source_config in sources:
            dlt_source = self._build_dlt_resources(source_config)
            dlt_sources.append(dlt_source)

        # Combine all sources into a single source if multiple sources exist
        if len(dlt_sources) == 1:
            combined_source = dlt_sources[0]
        else:
            # Combine multiple sources - DLT allows combining sources
            # For now, we'll run them sequentially or combine resources
            # This is a simplified approach - in production you might want more sophisticated combining
            combined_source = dlt_sources[0]
            for source in dlt_sources[1:]:
                # Merge resources from additional sources
                if hasattr(source, 'resources') and hasattr(combined_source, 'resources'):
                    combined_source.resources.update(source.resources)

        # Run the pipeline
        write_disposition = destination.get("write_disposition", "append")
        info = pipeline.run(
            combined_source,
            write_disposition=write_disposition
        )

        return info

    def _build_dlt_resources(
        self,
        source_config: Dict[str, Any]
    ) -> Any:
        """Build DLT resources from a source configuration.

        Args:
            source_config: Source configuration dictionary

        Returns:
            DLT resource(s) - can be a single resource or list of resources
        """
        source_type = source_config["type"]

        if source_type == "filesystem":
            return self._build_filesystem_resource(source_config)
        elif source_type == "sql_database":
            return self._build_sql_database_source(source_config)
        elif source_type == "database_table":
            # Legacy support for old API
            return self._build_table_resource(source_config)
        elif source_type == "database_query":
            # Legacy support for old API
            return self._build_query_resource(source_config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")

    def _build_filesystem_resource(self, config: Dict[str, Any]) -> Any:
        """Build a filesystem (S3) resource.

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

    def _build_sql_database_source(self, config: Dict[str, Any]) -> Any:
        """Build a DLT sql_database source with multiple resources.

        Args:
            config: Source configuration with credentials and resources list

        Returns:
            DLT sql_database source
        """
        credentials = config["credentials"]
        resources_config = config.get("resources", [])

        # Create database source
        source = sql_database(credentials)

        # Add all resources (tables and queries)
        table_names = []
        query_resources = []

        for resource_config in resources_config:
            resource_type = resource_config.get("type")
            
            if resource_type == "table":
                table_name = resource_config["name"]
                table_names.append(table_name)
                
                # Store hints for later application
                if "primary_key" in resource_config or "incremental" in resource_config:
                    # We'll apply hints after adding the resource
                    pass
            elif resource_type == "query":
                query = resource_config["query"]
                table_name = resource_config["name"]
                query_resources.append((query, table_name))

        # Add table resources
        if table_names:
            source = source.with_resources(*table_names)

        # Apply hints to table resources
        for resource_config in resources_config:
            if resource_config.get("type") == "table":
                table_name = resource_config["name"]
                if table_name in source.resources:
                    resource = source.resources[table_name]
                    hints = {}
                    
                    if "incremental" in resource_config:
                        hints["incremental"] = dlt.sources.incremental(
                            resource_config["incremental"]
                        )
                    if "primary_key" in resource_config:
                        hints["primary_key"] = resource_config["primary_key"]
                    
                    if hints:
                        resource = resource.apply_hints(**hints)
                        source.resources[table_name] = resource

        # Add query resources
        for query, table_name in query_resources:
            try:
                from dlt.sources.sql_database.helpers import table as sql_table
                query_resource = sql_table(query, table_name)
                source = source.with_resources(query_resource)
            except (ImportError, AttributeError):
                # Fallback: manually add query resource
                source.resources[table_name] = query

        return source

    def _build_table_resource(self, config: Dict[str, Any]) -> Any:
        """Build a database table resource.

        Args:
            config: Source configuration

        Returns:
            DLT database source
        """
        credentials = config["credentials"]
        table = config["table"]
        primary_key = config.get("primary_key")
        incremental = config.get("incremental")

        # Create database source
        source = sql_database(credentials)

        # Get specific table resource
        source = source.with_resources(table)

        # Apply hints for incremental loading and primary key
        if incremental or primary_key:
            hints = {}
            if incremental:
                hints["incremental"] = dlt.sources.incremental(incremental)
            if primary_key:
                hints["primary_key"] = primary_key
            
            # Apply hints to the source
            for resource_name in source.resources.keys():
                resource = source.resources[resource_name]
                if hints:
                    resource = resource.apply_hints(**hints)
                source.resources[resource_name] = resource

        return source

    def _build_query_resource(self, config: Dict[str, Any]) -> Any:
        """Build a query-based resource.

        Args:
            config: Source configuration

        Returns:
            DLT database source with query resource
        """
        credentials = config["credentials"]
        query = config["query"]
        table_name = config["table_name"]

        # Create database source with custom query
        # DLT's sql_database supports custom SQL queries via the table() helper
        # We'll create a source that executes the query
        source = sql_database(credentials)
        
        # Add custom query resource
        # DLT allows adding custom SQL queries as resources
        # The exact API may need adjustment based on DLT version
        try:
            # Try to add query as a custom resource
            from dlt.sources.sql_database.helpers import table as sql_table
            query_resource = sql_table(query, table_name)
            source = source.with_resources(query_resource)
        except (ImportError, AttributeError):
            # Fallback: create source and manually add query resource
            # This is a workaround - actual implementation depends on DLT API
            source.resources[table_name] = query

        return source

