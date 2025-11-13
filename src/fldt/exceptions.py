"""Custom exceptions for fldt.

This module defines the exception hierarchy for the fldt package.
All exceptions inherit from FluentDLTError for easy catching of
package-specific errors.
"""


class FluentDLTError(Exception):
    """Base exception for all fldt errors.

    All custom exceptions in the fldt package inherit from this class,
    allowing users to catch all package-specific errors with a single
    except clause.
    """

    pass


class ValidationError(FluentDLTError):
    """Raised when input validation fails.

    This exception is raised during pipeline configuration when:
    - Invalid source or destination types are provided
    - Required parameters are missing
    - Parameter values are out of acceptable ranges
    - Type constraints are violated
    """

    pass


class PipelineConfigurationError(FluentDLTError):
    """Raised when pipeline configuration is invalid or incomplete.

    This exception is raised when:
    - Required configuration is missing (e.g., no destination set)
    - Configuration parameters are incompatible
    - Pipeline cannot be constructed from given configuration
    """

    pass


class PipelineExecutionError(FluentDLTError):
    """Raised when pipeline execution fails.

    This exception is raised during pipeline runtime when:
    - Source extraction fails
    - Transformation fails
    - Destination loading fails
    - Unexpected runtime errors occur

    The original exception is typically chained via __cause__.
    """

    pass


class AdapterError(FluentDLTError):
    """Raised when adapter operations fail.

    This exception is raised when:
    - Adapter initialization fails
    - Adapter method calls fail
    - Underlying dlt operations fail
    - Adapter-specific errors occur

    The original exception is typically chained via __cause__.
    """

    pass

