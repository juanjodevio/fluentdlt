"""Unit tests for fldt.exceptions module."""

import pytest

from fldt.exceptions import (
    AdapterError,
    FluentDLTError,
    PipelineConfigurationError,
    PipelineExecutionError,
    ValidationError,
)


class TestExceptionHierarchy:
    """Test exception inheritance and hierarchy."""

    def test_all_exceptions_inherit_from_base(self):
        """All custom exceptions should inherit from FluentDLTError."""
        assert issubclass(ValidationError, FluentDLTError)
        assert issubclass(PipelineConfigurationError, FluentDLTError)
        assert issubclass(PipelineExecutionError, FluentDLTError)
        assert issubclass(AdapterError, FluentDLTError)

    def test_base_exception_inherits_from_exception(self):
        """FluentDLTError should inherit from built-in Exception."""
        assert issubclass(FluentDLTError, Exception)

    def test_exception_inheritance_chain(self):
        """Verify complete inheritance chain."""
        # ValidationError -> FluentDLTError -> Exception -> BaseException
        assert issubclass(ValidationError, Exception)
        assert issubclass(ValidationError, BaseException)


class TestFluentDLTError:
    """Test base FluentDLTError exception."""

    def test_can_raise_and_catch_base_error(self):
        """FluentDLTError can be raised and caught."""
        with pytest.raises(FluentDLTError) as exc_info:
            raise FluentDLTError("Test error")
        assert str(exc_info.value) == "Test error"

    def test_can_catch_all_custom_exceptions(self):
        """Catching FluentDLTError catches all package exceptions."""
        exceptions = [
            ValidationError("validation failed"),
            PipelineConfigurationError("config failed"),
            PipelineExecutionError("execution failed"),
            AdapterError("adapter failed"),
        ]

        for exc in exceptions:
            with pytest.raises(FluentDLTError):
                raise exc

    def test_base_error_with_no_message(self):
        """FluentDLTError can be raised without message."""
        with pytest.raises(FluentDLTError):
            raise FluentDLTError()


class TestValidationError:
    """Test ValidationError exception."""

    def test_validation_error_with_message(self):
        """ValidationError displays custom message."""
        with pytest.raises(ValidationError) as exc_info:
            raise ValidationError("Invalid source type")
        assert "Invalid source type" in str(exc_info.value)

    def test_validation_error_is_fluent_dlt_error(self):
        """ValidationError can be caught as FluentDLTError."""
        with pytest.raises(FluentDLTError):
            raise ValidationError("validation failed")

    def test_validation_error_with_formatted_message(self):
        """ValidationError supports formatted messages."""
        invalid_type = int
        with pytest.raises(ValidationError) as exc_info:
            raise ValidationError(f"Expected str, got {invalid_type}")
        assert "int" in str(exc_info.value)


class TestPipelineConfigurationError:
    """Test PipelineConfigurationError exception."""

    def test_configuration_error_with_message(self):
        """PipelineConfigurationError displays custom message."""
        with pytest.raises(PipelineConfigurationError) as exc_info:
            raise PipelineConfigurationError("Missing destination")
        assert "Missing destination" in str(exc_info.value)

    def test_configuration_error_is_fluent_dlt_error(self):
        """PipelineConfigurationError can be caught as FluentDLTError."""
        with pytest.raises(FluentDLTError):
            raise PipelineConfigurationError("config error")


class TestPipelineExecutionError:
    """Test PipelineExecutionError exception."""

    def test_execution_error_with_message(self):
        """PipelineExecutionError displays custom message."""
        with pytest.raises(PipelineExecutionError) as exc_info:
            raise PipelineExecutionError("Pipeline failed")
        assert "Pipeline failed" in str(exc_info.value)

    def test_execution_error_with_cause(self):
        """PipelineExecutionError can chain original exception."""
        original = ValueError("Invalid value")
        try:
            raise PipelineExecutionError("Pipeline failed") from original
        except PipelineExecutionError as exc:
            assert exc.__cause__ is original
            assert isinstance(exc.__cause__, ValueError)

    def test_execution_error_is_fluent_dlt_error(self):
        """PipelineExecutionError can be caught as FluentDLTError."""
        with pytest.raises(FluentDLTError):
            raise PipelineExecutionError("execution failed")


class TestAdapterError:
    """Test AdapterError exception."""

    def test_adapter_error_with_message(self):
        """AdapterError displays custom message."""
        with pytest.raises(AdapterError) as exc_info:
            raise AdapterError("DLT adapter initialization failed")
        assert "DLT adapter" in str(exc_info.value)

    def test_adapter_error_with_cause(self):
        """AdapterError can chain original exception."""
        original = ImportError("dlt not installed")
        try:
            raise AdapterError("Adapter failed") from original
        except AdapterError as exc:
            assert exc.__cause__ is original
            assert isinstance(exc.__cause__, ImportError)

    def test_adapter_error_is_fluent_dlt_error(self):
        """AdapterError can be caught as FluentDLTError."""
        with pytest.raises(FluentDLTError):
            raise AdapterError("adapter failed")


class TestExceptionUsagePatterns:
    """Test common exception usage patterns."""

    def test_catch_specific_then_base(self):
        """Can catch specific exception types before base."""

        def risky_operation(fail_type):
            if fail_type == "validation":
                raise ValidationError("Bad input")
            elif fail_type == "execution":
                raise PipelineExecutionError("Runtime error")
            else:
                raise FluentDLTError("Generic error")

        # Catch specific
        with pytest.raises(ValidationError):
            risky_operation("validation")

        # Catch another specific
        with pytest.raises(PipelineExecutionError):
            risky_operation("execution")

        # Catch base
        with pytest.raises(FluentDLTError):
            risky_operation("other")

    def test_exception_chaining_with_context(self):
        """Exceptions can be chained to preserve context."""
        try:
            try:
                raise ValueError("Original problem")
            except ValueError as e:
                raise PipelineExecutionError("Pipeline failed") from e
        except PipelineExecutionError as exc:
            assert exc.__cause__ is not None
            assert isinstance(exc.__cause__, ValueError)
            assert "Original problem" in str(exc.__cause__)

    def test_multiple_exception_handling(self):
        """Can handle multiple exception types in one block."""
        errors_caught = []

        for error_class in [ValidationError, PipelineConfigurationError, AdapterError]:
            try:
                raise error_class("test")
            except FluentDLTError as e:
                errors_caught.append(type(e))

        assert len(errors_caught) == 3
        assert ValidationError in errors_caught
        assert PipelineConfigurationError in errors_caught
        assert AdapterError in errors_caught
