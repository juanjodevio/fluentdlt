"""Source configuration builders for different data sources."""

from typing import Optional, List, Dict, Any
import re


class SourceBuilder:
    """Builder class for creating source configurations."""

    @staticmethod
    def build_sql_database_source(
        credentials: str,
        resources: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Build configuration for a DLT sql_database source.

        Args:
            credentials: Database connection string
            resources: Optional list of resource configurations

        Returns:
            Source configuration dictionary
        """
        config = {
            "type": "sql_database",
            "credentials": credentials,
            "resources": resources or []
        }
        return config

    @staticmethod
    def add_table_resource(
        source: Dict[str, Any],
        table: str,
        primary_key: Optional[str] = None,
        incremental: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a table resource to a sql_database source.

        Args:
            source: Source configuration dictionary (modified in place)
            table: Table name (schema.table format)
            primary_key: Optional primary key column
            incremental: Optional incremental column for incremental loads
            **kwargs: Additional resource arguments
        """
        if source.get("type") != "sql_database":
            raise ValueError("source must be a sql_database source")

        resource = {
            "type": "table",
            "name": table,
            **kwargs
        }

        if primary_key:
            resource["primary_key"] = primary_key

        if incremental:
            resource["incremental"] = incremental

        source.setdefault("resources", []).append(resource)

    @staticmethod
    def add_query_resource(
        source: Dict[str, Any],
        query: str,
        table_name: str,
        **kwargs
    ) -> None:
        """Add a query resource to a sql_database source.

        Args:
            source: Source configuration dictionary (modified in place)
            query: SQL query to execute
            table_name: Name for the resulting table
            **kwargs: Additional resource arguments
        """
        if source.get("type") != "sql_database":
            raise ValueError("source must be a sql_database source")

        resource = {
            "type": "query",
            "name": table_name,
            "query": query,
            **kwargs
        }

        source.setdefault("resources", []).append(resource)

    @staticmethod
    def build_filesystem_source(
        url_glob: str,
        table_name: Optional[str] = None,
        file_format: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Build configuration for a filesystem (S3) source.

        Args:
            url_glob: S3 URL pattern (e.g., "s3://bucket/data/*.csv")
            table_name: Optional table name
            file_format: Optional file format (auto-detected if not provided)
            **kwargs: Additional source arguments

        Returns:
            Source configuration dictionary
        """
        # Auto-detect file format from URL if not provided
        if not file_format:
            file_format = SourceBuilder._detect_file_format(url_glob)

        # Extract table name from URL if not provided
        if not table_name:
            table_name = SourceBuilder._extract_table_name_from_url(url_glob)

        config = {
            "type": "filesystem",
            "url_glob": url_glob,
            "table_name": table_name,
            "file_format": file_format,
            **kwargs
        }

        return config

    @staticmethod
    def _detect_file_format(url: str) -> str:
        """Detect file format from URL extension.

        Args:
            url: File URL

        Returns:
            Detected file format (csv, jsonl, parquet, etc.)
        """
        url_lower = url.lower()
        if url_lower.endswith(".csv") or "*.csv" in url_lower:
            return "csv"
        elif url_lower.endswith(".jsonl") or "*.jsonl" in url_lower or url_lower.endswith(".ndjson"):
            return "jsonl"
        elif url_lower.endswith(".json") or "*.json" in url_lower:
            return "json"
        elif url_lower.endswith(".parquet") or "*.parquet" in url_lower:
            return "parquet"
        else:
            # Default to csv for unknown formats
            return "csv"

    @staticmethod
    def _extract_table_name_from_url(url: str) -> str:
        """Extract a table name from a URL.

        Args:
            url: File URL

        Returns:
            Extracted table name
        """
        # Extract filename from URL
        match = re.search(r"([^/]+)(?:\.\w+)?$", url)
        if match:
            filename = match.group(1)
            # Remove common prefixes and clean up
            filename = filename.replace("*", "").replace(".", "_")
            return filename if filename else "data"
        return "data"

