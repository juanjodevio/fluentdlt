"""Unit tests for fldt.builder module."""

from __future__ import annotations

from typing import Any, Callable, Iterable

import pytest

from fldt.builder import PipelineBuilder
from fldt.exceptions import PipelineConfigurationError, ValidationError


class TestPipelineBuilderInitialization:
    """Test PipelineBuilder initialization."""

    def test_builder_initializes_empty(self) -> None:
        """PipelineBuilder initializes with no configuration."""
        builder = PipelineBuilder()
        assert builder._source is None
        assert builder._destination is None
        assert builder._transformers == []
        assert builder._incremental is None


class TestPipelineBuilderSetSource:
    """Test setting pipeline source."""

    def test_set_source_with_valid_data(self) -> None:
        """set_source() accepts valid source data."""
        builder = PipelineBuilder()
        source: list[dict[str, int]] = [{"id": 1}, {"id": 2}]

        result = builder.set_source(source)

        assert result is builder  # Returns self for chaining
        assert builder._source is source

    def test_set_source_with_callable(self) -> None:
        """set_source() accepts callable sources."""
        builder = PipelineBuilder()

        def source() -> list[dict[str, int]]:
            return [{"id": 1}]

        builder.set_source(source)

        assert builder._source is source

    def test_set_source_rejects_none(self) -> None:
        """set_source() raises ValidationError for None."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_source(None)  # type: ignore

        assert "cannot be None" in str(exc_info.value)

    def test_set_source_can_be_updated(self) -> None:
        """set_source() can be called multiple times."""
        builder = PipelineBuilder()
        source1: list[int] = [1, 2, 3]
        source2: list[int] = [4, 5, 6]

        builder.set_source(source1)
        builder.set_source(source2)

        assert builder._source is source2


class TestPipelineBuilderSetDestination:
    """Test setting pipeline destination."""

    def test_set_destination_with_string(self) -> None:
        """set_destination() accepts string destination names."""
        builder = PipelineBuilder()

        result = builder.set_destination("duckdb")

        assert result is builder
        assert builder._destination == "duckdb"

    def test_set_destination_with_object(self) -> None:
        """set_destination() accepts destination objects."""
        builder = PipelineBuilder()
        dest_obj = object()

        builder.set_destination(dest_obj)

        assert builder._destination is dest_obj

    def test_set_destination_rejects_empty_string(self) -> None:
        """set_destination() raises ValidationError for empty strings."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_destination("")

        assert "cannot be empty" in str(exc_info.value)

    def test_set_destination_rejects_blank_string(self) -> None:
        """set_destination() raises ValidationError for blank strings."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_destination("   ")

        assert "cannot be blank" in str(exc_info.value)

    def test_set_destination_rejects_none(self) -> None:
        """set_destination() raises ValidationError for None."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_destination(None)  # type: ignore

        assert "cannot be empty" in str(exc_info.value)


class TestPipelineBuilderAddTransformer:
    """Test adding transformers to pipeline."""

    def test_add_single_transformer(self) -> None:
        """add_transformer() adds a transformer to the list."""
        builder = PipelineBuilder()

        def transformer(x: int) -> int:
            return x * 2

        result = builder.add_transformer(transformer)

        assert result is builder
        assert len(builder._transformers) == 1
        assert builder._transformers[0] is transformer

    def test_add_multiple_transformers(self) -> None:
        """add_transformer() can be called multiple times."""
        builder = PipelineBuilder()

        def t1(x: int) -> int:
            return x * 2

        def t2(x: int) -> int:
            return x + 10

        builder.add_transformer(t1).add_transformer(t2)

        assert len(builder._transformers) == 2
        assert builder._transformers[0] is t1
        assert builder._transformers[1] is t2

    def test_add_transformer_validates_callable(self) -> None:
        """add_transformer() raises ValidationError if not callable."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.add_transformer("not callable")  # type: ignore

        assert "callable" in str(exc_info.value)
        assert "str" in str(exc_info.value)

    def test_add_transformer_accepts_various_callables(self) -> None:
        """add_transformer() accepts different callable types."""
        builder = PipelineBuilder()

        # Lambda
        builder.add_transformer(lambda x: x)

        # Function
        def func(x: Any) -> Any:
            return x

        builder.add_transformer(func)

        # Callable class
        class Passthrough:
            def __call__(self, x: Any) -> Any:
                return x

        builder.add_transformer(Passthrough())

        assert len(builder._transformers) == 3


class TestPipelineBuilderSetIncremental:
    """Test configuring incremental loading."""

    def test_set_incremental_with_cursor_only(self) -> None:
        """set_incremental() works with just cursor_field."""
        builder = PipelineBuilder()

        result = builder.set_incremental("updated_at")

        assert result is builder
        assert builder._incremental is not None
        assert builder._incremental["cursor_field"] == "updated_at"
        assert builder._incremental["row_order"] == "asc"  # default

    def test_set_incremental_with_all_params(self) -> None:
        """set_incremental() accepts all parameters."""
        builder = PipelineBuilder()

        builder.set_incremental(
            cursor_field="updated_at",
            initial_value="2024-01-01",
            primary_key="id",
            row_order="desc",
        )

        config = builder._incremental
        assert config is not None
        assert config["cursor_field"] == "updated_at"
        assert config["initial_value"] == "2024-01-01"
        assert config["primary_key"] == "id"
        assert config["row_order"] == "desc"

    def test_set_incremental_with_composite_primary_key(self) -> None:
        """set_incremental() accepts list as primary_key."""
        builder = PipelineBuilder()

        builder.set_incremental("updated_at", primary_key=["tenant_id", "id"])

        assert builder._incremental["primary_key"] == ["tenant_id", "id"]

    def test_set_incremental_with_kwargs(self) -> None:
        """set_incremental() accepts additional kwargs."""
        builder = PipelineBuilder()

        builder.set_incremental(
            "updated_at",
            custom_option=True,
            another_option="value",
        )

        config = builder._incremental
        assert config is not None
        assert config["custom_option"] is True
        assert config["another_option"] == "value"

    def test_set_incremental_validates_cursor_field(self) -> None:
        """set_incremental() validates cursor_field."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_incremental("")

        assert "cursor_field" in str(exc_info.value)

    def test_set_incremental_validates_row_order(self) -> None:
        """set_incremental() validates row_order values."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_incremental("updated_at", row_order="invalid")

        assert "row_order" in str(exc_info.value)
        assert "asc" in str(exc_info.value)
        assert "desc" in str(exc_info.value)


class TestPipelineBuilderSetPipelineName:
    """Test setting pipeline name."""

    def test_set_pipeline_name_with_valid_string(self) -> None:
        """set_pipeline_name() accepts valid string."""
        builder = PipelineBuilder()

        result = builder.set_pipeline_name("my_pipeline")

        assert result is builder
        assert builder._pipeline_name == "my_pipeline"

    def test_set_pipeline_name_rejects_empty_string(self) -> None:
        """set_pipeline_name() raises ValidationError for empty string."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_pipeline_name("")

        assert "non-empty string" in str(exc_info.value)

    def test_set_pipeline_name_rejects_blank_string(self) -> None:
        """set_pipeline_name() raises ValidationError for blank string."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_pipeline_name("   ")

        assert "non-empty string" in str(exc_info.value)

    def test_set_pipeline_name_rejects_non_string(self) -> None:
        """set_pipeline_name() raises ValidationError for non-string."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_pipeline_name(123)  # type: ignore

        assert "non-empty string" in str(exc_info.value)


class TestPipelineBuilderSetDatasetName:
    """Test setting dataset name."""

    def test_set_dataset_name_with_valid_string(self) -> None:
        """set_dataset_name() accepts valid string."""
        builder = PipelineBuilder()

        result = builder.set_dataset_name("my_dataset")

        assert result is builder
        assert builder._dataset_name == "my_dataset"

    def test_set_dataset_name_rejects_empty_string(self) -> None:
        """set_dataset_name() raises ValidationError for empty string."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_dataset_name("")

        assert "non-empty string" in str(exc_info.value)


class TestPipelineBuilderSetOptions:
    """Test setting pipeline options."""

    def test_set_option_with_valid_key_value(self) -> None:
        """set_option() adds option to options dict."""
        builder = PipelineBuilder()

        result = builder.set_option("dev_mode", True)

        assert result is builder
        assert builder._options["dev_mode"] is True

    def test_set_option_multiple_times(self) -> None:
        """set_option() can be called multiple times."""
        builder = PipelineBuilder()

        builder.set_option("key1", "value1")
        builder.set_option("key2", "value2")

        assert builder._options["key1"] == "value1"
        assert builder._options["key2"] == "value2"

    def test_set_option_validates_key(self) -> None:
        """set_option() validates key is non-empty string."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_option("", "value")

        assert "non-empty string" in str(exc_info.value)

    def test_set_options_with_dict(self) -> None:
        """set_options() sets multiple options at once."""
        builder = PipelineBuilder()
        options: dict[str, str] = {"key1": "value1", "key2": "value2", "key3": "value3"}

        result = builder.set_options(options)

        assert result is builder
        assert builder._options["key1"] == "value1"
        assert builder._options["key2"] == "value2"
        assert builder._options["key3"] == "value3"

    def test_set_options_validates_dict_type(self) -> None:
        """set_options() raises ValidationError if not dict."""
        builder = PipelineBuilder()

        with pytest.raises(ValidationError) as exc_info:
            builder.set_options("not a dict")  # type: ignore

        assert "dictionary" in str(exc_info.value)


class TestPipelineBuilderBuild:
    """Test building pipeline configuration."""

    def test_build_with_minimal_config(self) -> None:
        """build() creates config with source and destination."""
        builder = PipelineBuilder()
        builder.set_source([1, 2, 3])
        builder.set_destination("duckdb")

        config = builder.build()

        assert config["source"] == [1, 2, 3]
        assert config["destination"] == "duckdb"
        assert config["transformers"] == []
        assert config["incremental"] is None
        assert config["pipeline_name"] is None
        assert config["dataset_name"] is None
        assert config["options"] == {}

    def test_build_with_full_config(self) -> None:
        """build() creates config with all options."""
        builder = PipelineBuilder()
        source: list[int] = [1, 2, 3]

        def transformer(x: int) -> int:
            return x * 2

        builder.set_source(source)
        builder.set_destination("postgres")
        builder.add_transformer(transformer)
        builder.set_incremental("updated_at")
        builder.set_pipeline_name("my_pipeline")
        builder.set_dataset_name("my_dataset")
        builder.set_option("dev_mode", True)

        config = builder.build()

        assert config["source"] is source
        assert config["destination"] == "postgres"
        assert len(config["transformers"]) == 1
        assert config["incremental"] is not None
        assert config["pipeline_name"] == "my_pipeline"
        assert config["dataset_name"] == "my_dataset"
        assert config["options"]["dev_mode"] is True

    def test_build_requires_source(self) -> None:
        """build() raises PipelineConfigurationError without source."""
        builder = PipelineBuilder()
        builder.set_destination("duckdb")

        with pytest.raises(PipelineConfigurationError) as exc_info:
            builder.build()

        assert "Source must be set" in str(exc_info.value)

    def test_build_requires_destination(self) -> None:
        """build() raises PipelineConfigurationError without destination."""
        builder = PipelineBuilder()
        builder.set_source([1, 2, 3])

        with pytest.raises(PipelineConfigurationError) as exc_info:
            builder.build()

        assert "Destination must be set" in str(exc_info.value)

    def test_build_returns_copy_of_transformers(self) -> None:
        """build() returns copy of transformers list."""
        builder = PipelineBuilder()
        builder.set_source([1])
        builder.set_destination("duckdb")
        builder.add_transformer(lambda x: x)

        config = builder.build()
        config["transformers"].append(lambda x: x * 2)

        # Original builder should be unchanged
        assert len(builder._transformers) == 1

    def test_build_returns_copy_of_options(self) -> None:
        """build() returns copy of options dict."""
        builder = PipelineBuilder()
        builder.set_source([1])
        builder.set_destination("duckdb")
        builder.set_option("key", "value")

        config = builder.build()
        config["options"]["key"] = "modified"

        # Original builder should be unchanged
        assert builder._options["key"] == "value"


class TestPipelineBuilderMethodChaining:
    """Test method chaining functionality."""

    def test_full_method_chaining(self) -> None:
        """All setter methods support chaining."""
        config = (
            PipelineBuilder()
            .set_source([1, 2, 3])
            .set_destination("duckdb")
            .add_transformer(lambda x: x * 2)
            .add_transformer(lambda x: x + 10)
            .set_incremental("updated_at")
            .set_pipeline_name("my_pipeline")
            .set_dataset_name("my_dataset")
            .set_option("dev_mode", True)
            .build()
        )

        assert config["source"] == [1, 2, 3]
        assert config["destination"] == "duckdb"
        assert len(config["transformers"]) == 2
        assert config["incremental"] is not None
        assert config["pipeline_name"] == "my_pipeline"


class TestPipelineBuilderRepr:
    """Test string representation."""

    def test_repr_shows_state(self) -> None:
        """repr() shows builder configuration state."""
        builder = PipelineBuilder()
        repr_empty = repr(builder)

        assert "source=unset" in repr_empty
        assert "destination=unset" in repr_empty
        assert "transformers=0" in repr_empty

        builder.set_source([1])
        builder.set_destination("duckdb")
        builder.add_transformer(lambda x: x)

        repr_filled = repr(builder)
        assert "source=set" in repr_filled
        assert "destination=set" in repr_filled
        assert "transformers=1" in repr_filled
