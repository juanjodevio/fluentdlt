"""Destination configuration builders."""

from typing import Optional, Dict, Any


class DestinationBuilder:
    """Builder class for creating destination configurations."""

    @staticmethod
    def build_destination(
        destination: str,
        credentials: Optional[str] = None,
        dataset: str = "raw",
        write_disposition: str = "append",
        **kwargs
    ) -> Dict[str, Any]:
        """Build configuration for a destination.

        Args:
            destination: Destination type (bigquery, redshift, s3, etc.)
            credentials: Optional credentials string
            dataset: Dataset/schema name
            write_disposition: Write mode (append, replace, merge)
            **kwargs: Additional destination arguments

        Returns:
            Destination configuration dictionary
        """
        destination_lower = destination.lower()

        config = {
            "type": destination_lower,
            "dataset": dataset,
            "write_disposition": write_disposition,
            **kwargs
        }

        if credentials:
            config["credentials"] = credentials

        # Destination-specific configurations
        if destination_lower == "bigquery":
            config.update(DestinationBuilder._build_bigquery_config(**kwargs))
        elif destination_lower == "redshift":
            config.update(DestinationBuilder._build_redshift_config(**kwargs))
        elif destination_lower == "s3":
            config.update(DestinationBuilder._build_s3_config(**kwargs))
        elif destination_lower == "duckdb":
            config.update(DestinationBuilder._build_duckdb_config(**kwargs))

        return config

    @staticmethod
    def _build_bigquery_config(**kwargs) -> Dict[str, Any]:
        """Build BigQuery-specific configuration.

        Args:
            **kwargs: Additional arguments

        Returns:
            BigQuery configuration dictionary
        """
        config = {}
        if "project_id" in kwargs:
            config["project_id"] = kwargs["project_id"]
        if "location" in kwargs:
            config["location"] = kwargs["location"]
        return config

    @staticmethod
    def _build_redshift_config(**kwargs) -> Dict[str, Any]:
        """Build Redshift-specific configuration.

        Args:
            **kwargs: Additional arguments

        Returns:
            Redshift configuration dictionary
        """
        config = {}
        if "cluster_id" in kwargs:
            config["cluster_id"] = kwargs["cluster_id"]
        if "region" in kwargs:
            config["region"] = kwargs["region"]
        return config

    @staticmethod
    def _build_s3_config(**kwargs) -> Dict[str, Any]:
        """Build S3-specific configuration.

        Args:
            **kwargs: Additional arguments

        Returns:
            S3 configuration dictionary
        """
        config = {}
        if "bucket" in kwargs:
            config["bucket"] = kwargs["bucket"]
        if "path" in kwargs:
            config["path"] = kwargs["path"]
        return config

    @staticmethod
    def _build_duckdb_config(**kwargs) -> Dict[str, Any]:
        """Build DuckDB-specific configuration.

        Args:
            **kwargs: Additional arguments

        Returns:
            DuckDB configuration dictionary
        """
        config = {}
        if "db_path" in kwargs:
            config["db_path"] = kwargs["db_path"]
        return config

