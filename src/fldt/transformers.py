"""Transformer chain management for fldt.

This module provides the TransformerChain class which manages a sequence
of transformation functions applied to data in a pipeline. Transformations
are applied in order, with each receiving the output of the previous one.
"""

import logging
from typing import Any

from fldt.exceptions import PipelineExecutionError, ValidationError
from fldt.types import TransformerFunc

logger = logging.getLogger(__name__)


class TransformerChain:
    """Manages a chain of transformation functions.

    The TransformerChain class encapsulates a sequence of transformers and
    provides methods to add, validate, and apply them to data. Transformers
    are applied in the order they were added, with each transformer receiving
    the output of the previous one.

    This class follows the Single Responsibility Principle by focusing solely
    on transformation management, delegating pipeline execution to other
    components.

    Example:
        ```python
        chain = TransformerChain()
        chain.add(lambda x: x * 2)
        chain.add(lambda x: x + 10)

        result = chain.apply([1, 2, 3])  # [12, 14, 16]
        ```
    """

    def __init__(self, transformers: list[TransformerFunc] | None = None) -> None:
        """Initialize the transformer chain.

        Args:
            transformers: Optional initial list of transformers. Each must be
                callable. If None, starts with an empty chain.

        Raises:
            ValidationError: If any transformer is not callable.
        """
        self._transformers: list[TransformerFunc] = []

        if transformers:
            for transformer in transformers:
                self.add(transformer)

    def add(self, transformer: TransformerFunc) -> "TransformerChain":
        """Add a transformer to the chain.

        The transformer will be appended to the end of the chain and will
        receive the output of the previous transformer when applied.

        Args:
            transformer: A callable that accepts data and returns transformed data.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If transformer is not callable.
        """
        if not callable(transformer):
            raise ValidationError(
                f"Transformer must be callable, got {type(transformer).__name__}"
            )

        self._transformers.append(transformer)
        logger.debug(
            "Added transformer to chain",
            extra={
                "transformer": str(transformer),
                "chain_length": len(self._transformers),
            },
        )
        return self

    def apply(self, data: Any) -> Any:
        """Apply all transformers in sequence to the data.

        Transformers are applied in the order they were added. Each transformer
        receives the output of the previous one (or the original data for the
        first transformer).

        Args:
            data: The input data to transform.

        Returns:
            The transformed data after all transformers have been applied.

        Raises:
            PipelineExecutionError: If any transformer fails during execution.
        """
        if not self._transformers:
            logger.debug("No transformers to apply, returning original data")
            return data

        logger.info(
            "Applying transformer chain",
            extra={"transformer_count": len(self._transformers)},
        )

        result = data
        for idx, transformer in enumerate(self._transformers):
            try:
                logger.debug(
                    f"Applying transformer {idx + 1}/{len(self._transformers)}",
                    extra={"transformer": str(transformer)},
                )
                result = transformer(result)
            except Exception as e:
                logger.error(
                    f"Transformer {idx + 1} failed",
                    extra={
                        "transformer": str(transformer),
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                raise PipelineExecutionError(
                    f"Transformation {idx + 1} failed: {e}"
                ) from e

        logger.info("Transformer chain completed successfully")
        return result

    def compose(self, other: "TransformerChain") -> "TransformerChain":
        """Compose this chain with another chain.

        Creates a new TransformerChain containing transformers from both chains.
        The transformers from this chain are applied first, followed by the
        transformers from the other chain.

        Args:
            other: Another TransformerChain to compose with.

        Returns:
            A new TransformerChain containing transformers from both chains.

        Raises:
            ValidationError: If other is not a TransformerChain.
        """
        if not isinstance(other, TransformerChain):
            raise ValidationError(
                f"Can only compose with TransformerChain, got {type(other).__name__}"
            )

        # Create new chain with combined transformers
        combined_transformers = self._transformers + other._transformers
        logger.debug(
            "Composing transformer chains",
            extra={
                "chain1_length": len(self._transformers),
                "chain2_length": len(other._transformers),
                "combined_length": len(combined_transformers),
            },
        )
        return TransformerChain(combined_transformers)

    def clear(self) -> "TransformerChain":
        """Remove all transformers from the chain.

        Returns:
            Self for method chaining.
        """
        logger.debug(
            f"Clearing transformer chain (had {len(self._transformers)} transformers)"
        )
        self._transformers.clear()
        return self

    def __len__(self) -> int:
        """Return the number of transformers in the chain."""
        return len(self._transformers)

    def __repr__(self) -> str:
        """Return a string representation of the chain."""
        return f"TransformerChain(transformers={len(self._transformers)})"

    def __bool__(self) -> bool:
        """Return True if chain has transformers, False otherwise."""
        return bool(self._transformers)

    @property
    def transformers(self) -> list[TransformerFunc]:
        """Get a copy of the transformers list.

        Returns a copy to prevent external modification of the internal list.
        """
        return self._transformers.copy()
