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
        dlt_sources = []
        for source_config in sources:
            dlt_source = self._build_dlt_source(source_config)
            if dlt_source:
                dlt_sources.append(dlt_source)

        if not dlt_sources:
            raise ValueError("No valid sources to run")

        # Collect all resources from all sources
        # DLT's pipeline.run() can accept a list of sources/resources
        all_resources = []
        for source in dlt_sources:
            if hasattr(source, 'resources') and source.resources:
                # Extract all resources from this source
                all_resources.extend(source.resources.values())
            else:
                # If it's a single resource or source without resources attribute
                all_resources.append(source)

        # Run the pipeline with all resources
        write_disposition = destination.get("write_disposition", "append")
        info = pipeline.run(
            all_resources,
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
        elif source_type == "filesystem":
            return self._build_filesystem_source(source_config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")

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
        
        for resource_config in resources_config:
            resource_type = resource_config.get("type")
            
            if resource_type == "table":
                table_name = resource_config["name"]
                table_names.append(table_name)

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

        # Handle query resources using sql_table for custom SQL queries
        for resource_config in resources_config:
            if resource_config.get("type") == "query":
                query = resource_config["query"]
                table_name = resource_config["name"]
                
                # Use sql_table to execute custom SQL query
                # sql_table can execute any SQL query and treat the result as a table
                query_resource = sql_table(
                    credentials=credentials,
                    table=f"({query})",  # Wrap query as subquery
                    table_name=table_name
                )
                
                # Apply hints if provided
                hints = {}
                if "primary_key" in resource_config:
                    hints["primary_key"] = resource_config["primary_key"]
                
                if hints:
                    query_resource = query_resource.apply_hints(**hints)
                
                # Add the query resource to the source
                source.resources[table_name] = query_resource

        return source

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

