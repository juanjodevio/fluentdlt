"""Source configuration builders for different data sources."""

from typing import Optional, List, Dict, Any


class SourceBuilder:
    """Builder class for creating source configurations."""

    @staticmethod
    def build_sql_database_source(
        credentials: str,
        tables: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Build configuration for a DLT sql_database source.

        Args:
            credentials: Database connection string
            tables: Optional list of specific table names to extract

        Returns:
            Source configuration dictionary
        """
        config = {
            "type": "sql_database",
            "credentials": credentials,
            "tables": tables,
            **kwargs
        }
        return config

    @staticmethod
    def build_table_source(
        credentials: str,
        table: str,
        primary_key: Optional[str] = None,
        incremental: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Build configuration for a single database table source.

        Args:
            credentials: Database connection string
            table: Table name (schema.table format)
            primary_key: Optional primary key column
            incremental: Optional incremental column for incremental loads
            **kwargs: Additional source arguments

        Returns:
            Source configuration dictionary
        """
        config = {
            "type": "table",
            "credentials": credentials,
            "table": table,
            **kwargs
        }

        if primary_key:
            config["primary_key"] = primary_key

        if incremental:
            config["incremental"] = incremental

        return config

    @staticmethod
    def build_query_source(
        credentials: str,
        query: str,
        table_name: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Build configuration for a query-based source.

        Args:
            credentials: Database connection string
            query: SQL query to execute
            table_name: Name for the resulting table
            **kwargs: Additional source arguments

        Returns:
            Source configuration dictionary
        """
        config = {
            "type": "query",
            "credentials": credentials,
            "query": query,
            "table_name": table_name,
            **kwargs
        }

        return config

    @staticmethod
    def build_filesystem_source(
        url_glob: str,
        table_name: str,
        file_format: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Build configuration for a filesystem (S3) source.

        Args:
            url_glob: S3 URL pattern (e.g., "s3://bucket/data/*.csv")
            table_name: Table name for the data (required)
            file_format: Optional file format (auto-detected if not provided)
            **kwargs: Additional source arguments

        Returns:
            Source configuration dictionary
        """
        # Auto-detect file format from URL if not provided
        if not file_format:
            file_format = SourceBuilder._detect_file_format(url_glob)

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


