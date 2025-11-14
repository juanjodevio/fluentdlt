"""Unit tests for fldt.transformers module."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from fldt.exceptions import PipelineExecutionError, ValidationError
from fldt.transformers import TransformerChain

Transformer = Callable[[Any], Any]


class TestTransformerChainInitialization:
    """Test TransformerChain initialization."""

    def test_empty_chain_initialization(self) -> None:
        """TransformerChain can be initialized without transformers."""
        chain = TransformerChain()
        assert len(chain) == 0
        assert not chain

    def test_initialization_with_transformers(self) -> None:
        """TransformerChain can be initialized with a list of transformers."""
        transformers = [lambda x: x * 2, lambda x: x + 1]
        chain = TransformerChain(transformers)
        assert len(chain) == 2

    def test_initialization_validates_transformers(self) -> None:
        """TransformerChain validates transformers during initialization."""
        invalid_transformers = [lambda x: x, "not a function", lambda x: x * 2]

        with pytest.raises(ValidationError) as exc_info:
            TransformerChain(invalid_transformers)

        assert "callable" in str(exc_info.value)

    def test_initialization_with_none(self) -> None:
        """TransformerChain handles None as empty list."""
        chain = TransformerChain(None)
        assert len(chain) == 0


class TestTransformerChainAdd:
    """Test adding transformers to the chain."""

    def test_add_single_transformer(self) -> None:
        """add() adds a transformer to the chain."""
        chain = TransformerChain()

        def transformer(x: int) -> int:
            return x * 2

        result = chain.add(transformer)

        assert len(chain) == 1
        assert result is chain  # Returns self for chaining

    def test_add_multiple_transformers(self) -> None:
        """Multiple transformers can be added sequentially."""
        chain = TransformerChain()

        chain.add(lambda x: x * 2)
        chain.add(lambda x: x + 10)
        chain.add(lambda x: x / 2)

        assert len(chain) == 3

    def test_add_method_chaining(self) -> None:
        """add() supports method chaining."""
        chain = TransformerChain()

        result = chain.add(lambda x: x).add(lambda x: x).add(lambda x: x)

        assert result is chain
        assert len(chain) == 3

    def test_add_validates_callable(self) -> None:
        """add() raises ValidationError if transformer not callable."""
        chain = TransformerChain()

        with pytest.raises(ValidationError) as exc_info:
            chain.add("not callable")  # type: ignore

        assert "callable" in str(exc_info.value)
        assert "str" in str(exc_info.value)

    def test_add_various_callable_types(self) -> None:
        """add() accepts various callable types."""
        chain = TransformerChain()

        # Lambda
        chain.add(lambda x: x)

        # Function
        def my_func(x: Any) -> Any:
            return x

        chain.add(my_func)

        # Callable class
        class MyCallable:
            def __call__(self, x: Any) -> Any:
                return x

        chain.add(MyCallable())

        assert len(chain) == 3


class TestTransformerChainApply:
    """Test applying transformers to data."""

    def test_apply_empty_chain_returns_original(self) -> None:
        """apply() returns original data when chain is empty."""
        chain = TransformerChain()
        data = [1, 2, 3]

        result = chain.apply(data)

        assert result is data

    def test_apply_single_transformer(self) -> None:
        """apply() applies single transformer correctly."""
        chain = TransformerChain()
        chain.add(lambda x: [item * 2 for item in x])

        result = chain.apply([1, 2, 3])

        assert result == [2, 4, 6]

    def test_apply_multiple_transformers_in_sequence(self) -> None:
        """apply() chains multiple transformers in order."""
        chain = TransformerChain()
        chain.add(lambda x: [item + 10 for item in x])  # [11, 12, 13]
        chain.add(lambda x: [item * 2 for item in x])  # [22, 24, 26]

        result = chain.apply([1, 2, 3])

        assert result == [22, 24, 26]

    def test_apply_order_matters(self) -> None:
        """apply() applies transformers in the order they were added."""
        chain1 = TransformerChain()
        chain1.add(lambda x: x + 10)
        chain1.add(lambda x: x * 2)

        chain2 = TransformerChain()
        chain2.add(lambda x: x * 2)
        chain2.add(lambda x: x + 10)

        assert chain1.apply(5) == 30  # (5 + 10) * 2
        assert chain2.apply(5) == 20  # (5 * 2) + 10

    def test_apply_with_different_data_types(self) -> None:
        """apply() works with various data types."""
        chain = TransformerChain()
        chain.add(lambda x: x.upper())

        assert chain.apply("hello") == "HELLO"

        # Numeric
        chain2 = TransformerChain()
        chain2.add(lambda x: x * 2)
        assert chain2.apply(5) == 10

        # Dict
        chain3 = TransformerChain()
        chain3.add(lambda d: {**d, "new": "value"})
        assert chain3.apply({"a": 1}) == {"a": 1, "new": "value"}

    def test_apply_transformer_receives_previous_output(self) -> None:
        """Each transformer receives output from previous transformer."""
        chain = TransformerChain()

        # Track what each transformer receives
        received: list[tuple[str, Any]] = []

        def track1(x: int) -> int:
            received.append(("track1", x))
            return x * 2

        def track2(x: int) -> int:
            received.append(("track2", x))
            return x + 10

        chain.add(track1).add(track2)
        result = chain.apply(5)

        assert result == 20  # (5 * 2) + 10
        assert received == [("track1", 5), ("track2", 10)]

    def test_apply_handles_transformer_errors(self) -> None:
        """apply() raises PipelineExecutionError if transformer fails."""
        chain = TransformerChain()
        chain.add(lambda x: x / 0)  # Will raise ZeroDivisionError

        with pytest.raises(PipelineExecutionError) as exc_info:
            chain.apply(10)

        assert "Transformation 1 failed" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, ZeroDivisionError)

    def test_apply_reports_correct_transformer_index_on_error(self) -> None:
        """apply() reports which transformer failed."""
        chain = TransformerChain()
        chain.add(lambda x: x * 2)
        chain.add(lambda x: x / 0)  # Second transformer fails
        chain.add(lambda x: x + 10)

        with pytest.raises(PipelineExecutionError) as exc_info:
            chain.apply(5)

        assert "Transformation 2 failed" in str(exc_info.value)

    def test_apply_stops_on_first_error(self) -> None:
        """apply() stops execution when a transformer fails."""
        chain = TransformerChain()

        executed: list[int] = []

        def transform1(x: int) -> int:
            executed.append(1)
            return x * 2

        def transform2(x: int) -> int:
            executed.append(2)
            raise ValueError("Intentional error")

        def transform3(x: int) -> int:
            executed.append(3)
            return x + 10

        chain.add(transform1).add(transform2).add(transform3)

        with pytest.raises(PipelineExecutionError):
            chain.apply(5)

        # Only first two should execute
        assert executed == [1, 2]


class TestTransformerChainCompose:
    """Test composing transformer chains."""

    def test_compose_two_chains(self) -> None:
        """compose() combines two chains."""
        chain1 = TransformerChain()
        chain1.add(lambda x: x * 2)

        chain2 = TransformerChain()
        chain2.add(lambda x: x + 10)

        combined = chain1.compose(chain2)

        assert len(combined) == 2
        assert combined.apply(5) == 20  # (5 * 2) + 10

    def test_compose_maintains_order(self) -> None:
        """compose() applies first chain's transformers before second chain's."""
        chain1 = TransformerChain()
        chain1.add(lambda x: x + 1)
        chain1.add(lambda x: x * 2)

        chain2 = TransformerChain()
        chain2.add(lambda x: x - 5)

        combined = chain1.compose(chain2)
        result = combined.apply(10)

        # Should be: ((10 + 1) * 2) - 5 = 17
        assert result == 17

    def test_compose_with_empty_chain(self) -> None:
        """compose() works with empty chains."""
        chain1 = TransformerChain()
        chain1.add(lambda x: x * 2)

        chain2 = TransformerChain()  # Empty

        combined = chain1.compose(chain2)
        assert len(combined) == 1
        assert combined.apply(5) == 10

    def test_compose_empty_with_non_empty(self) -> None:
        """compose() works when first chain is empty."""
        chain1 = TransformerChain()  # Empty
        chain2 = TransformerChain()
        chain2.add(lambda x: x * 2)

        combined = chain1.compose(chain2)
        assert len(combined) == 1
        assert combined.apply(5) == 10

    def test_compose_returns_new_chain(self) -> None:
        """compose() returns a new chain without modifying originals."""
        chain1 = TransformerChain()
        chain1.add(lambda x: x * 2)

        chain2 = TransformerChain()
        chain2.add(lambda x: x + 10)

        combined = chain1.compose(chain2)

        # Original chains unchanged
        assert len(chain1) == 1
        assert len(chain2) == 1
        # New chain has both
        assert len(combined) == 2

    def test_compose_validates_input(self) -> None:
        """compose() raises ValidationError if argument is not TransformerChain."""
        chain = TransformerChain()

        with pytest.raises(ValidationError) as exc_info:
            chain.compose("not a chain")  # type: ignore

        assert "TransformerChain" in str(exc_info.value)

    def test_compose_multiple_chains(self) -> None:
        """Multiple chains can be composed together."""
        chain1 = TransformerChain([lambda x: x * 2])
        chain2 = TransformerChain([lambda x: x + 10])
        chain3 = TransformerChain([lambda x: x - 5])

        result = chain1.compose(chain2).compose(chain3)

        assert len(result) == 3
        assert result.apply(5) == 15  # ((5 * 2) + 10) - 5


class TestTransformerChainClear:
    """Test clearing transformer chains."""

    def test_clear_removes_all_transformers(self) -> None:
        """clear() removes all transformers from chain."""
        chain = TransformerChain()
        chain.add(lambda x: x).add(lambda x: x).add(lambda x: x)

        assert len(chain) == 3
        chain.clear()
        assert len(chain) == 0

    def test_clear_returns_self(self) -> None:
        """clear() returns self for method chaining."""
        chain = TransformerChain()
        result = chain.clear()
        assert result is chain

    def test_clear_on_empty_chain(self) -> None:
        """clear() works on empty chain."""
        chain = TransformerChain()
        chain.clear()
        assert len(chain) == 0


class TestTransformerChainProperties:
    """Test TransformerChain properties and magic methods."""

    def test_len_returns_transformer_count(self) -> None:
        """len() returns number of transformers."""
        chain = TransformerChain()
        assert len(chain) == 0

        chain.add(lambda x: x)
        assert len(chain) == 1

        chain.add(lambda x: x)
        assert len(chain) == 2

    def test_bool_empty_chain_is_falsy(self) -> None:
        """Empty chain evaluates to False."""
        chain = TransformerChain()
        assert not chain

    def test_bool_non_empty_chain_is_truthy(self) -> None:
        """Non-empty chain evaluates to True."""
        chain = TransformerChain()
        chain.add(lambda x: x)
        assert chain

    def test_repr_shows_transformer_count(self) -> None:
        """repr() shows number of transformers."""
        chain = TransformerChain()
        assert "transformers=0" in repr(chain)

        chain.add(lambda x: x)
        assert "transformers=1" in repr(chain)

    def test_transformers_property_returns_copy(self) -> None:
        """transformers property returns a copy, not the internal list."""
        chain = TransformerChain()
        chain.add(lambda x: x)

        transformers_copy = chain.transformers
        transformers_copy.append(lambda x: x * 2)

        # Original chain should be unchanged
        assert len(chain) == 1

    def test_transformers_property_contains_added_transformers(self) -> None:
        """transformers property contains the transformers added to chain."""

        def func1(x: int) -> int:
            return x * 2

        def func2(x: int) -> int:
            return x + 10

        chain = TransformerChain()
        chain.add(func1).add(func2)

        transformers = chain.transformers
        assert len(transformers) == 2
        assert transformers[0] is func1
        assert transformers[1] is func2


class TestTransformerChainUsagePatterns:
    """Test common usage patterns and edge cases."""

    def test_reusable_transformer_functions(self) -> None:
        """Same transformer function can be added multiple times."""

        def double(x: int) -> int:
            return x * 2

        chain = TransformerChain()
        chain.add(double).add(double).add(double)

        assert chain.apply(2) == 16  # 2 * 2 * 2 * 2

    def test_stateful_transformer(self) -> None:
        """Transformers can maintain state (though not recommended)."""

        class Counter:
            def __init__(self) -> None:
                self.count = 0

            def __call__(self, x: Any) -> Any:
                self.count += 1
                return x

        counter = Counter()
        chain = TransformerChain([counter, counter, counter])

        chain.apply(10)
        assert counter.count == 3

    def test_complex_transformation_pipeline(self) -> None:
        """Complex multi-step transformation works correctly."""

        # Simulate a data processing pipeline
        def parse_numbers(data: list[str]) -> list[int]:
            return [int(x) for x in data]

        def filter_positive(data: list[int]) -> list[int]:
            return [x for x in data if x > 0]

        def square(data: list[int]) -> list[int]:
            return [x * x for x in data]

        def sum_all(data: list[int]) -> int:
            return sum(data)

        chain = TransformerChain()
        chain.add(parse_numbers).add(filter_positive).add(square).add(sum_all)

        result = chain.apply(["-5", "3", "-2", "4", "1"])
        # Positive: [3, 4, 1]
        # Squared: [9, 16, 1]
        # Sum: 26
        assert result == 26

    def test_transformer_with_none_data(self) -> None:
        """Transformers can handle None as data."""
        chain = TransformerChain()
        chain.add(lambda x: x if x is not None else 0)

        result = chain.apply(None)
        assert result == 0

    def test_transformer_that_changes_data_type(self) -> None:
        """Transformers can change the data type."""
        chain = TransformerChain()
        chain.add(lambda x: str(x))  # int -> str
        chain.add(lambda x: len(x))  # str -> int

        result = chain.apply(12345)
        assert result == 5  # Length of "12345"
