# Contributing to FluentDLT

Thank you for your interest in contributing to **fldt** (Fluent Data Loading Toolkit)!

This document provides guidelines and best practices for contributing to the project.

---

## 🧭 Core Principles

All contributions must align with our design philosophy:

- **Explicit over Implicit** - Clear, readable code beats clever tricks
- **Simple over Complex** - Straightforward solutions preferred
- **Maintainability First** - Code is read more than written
- **Type Safety** - Full type hints required
- **SOLID Principles** - Clean architecture, separation of concerns
- **Zen of Python** - Follow PEP 20 guidelines

---

## 🛠️ Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://astral.sh/uv) (recommended) or pip
- Git

### Setup Steps

```bash
# Clone the repository
git clone https://github.com/yourusername/fluentdlt.git
cd fluentdlt

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On Unix/macOS
# .venv\Scripts\activate   # On Windows

# Install package with development dependencies
uv sync --group dev

# Verify installation
uv run pytest
```

---

## 📋 Before You Contribute

### Check Existing Issues

Before starting work:
1. Check [existing issues](https://github.com/yourusername/fluentdlt/issues)
2. Comment on relevant issues to avoid duplicate work
3. For major changes, open an issue first to discuss approach

### Understanding the Architecture

Review the architecture before making changes:

```
FluentPipeline (user API)
    ↓
PipelineBuilder (configuration)
    ↓
PipelineExecutor (orchestration)
    ↓
PipelineAdapter (abstraction)
    ↓
DltAdapter (dlt integration)
```

Each layer has a single responsibility. Changes should respect this separation.

---

## ✅ Code Quality Standards

### Type Hints (Required)

All code must have complete type hints:

```python
# ✅ Good
def process_data(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Process data with transformations."""
    return [transform(item) for item in data]

# ❌ Bad
def process_data(data):
    return [transform(item) for item in data]
```

### Docstrings (Required)

All public APIs must have docstrings following PEP 257:

```python
def set_incremental(
    self,
    cursor_field: str,
    initial_value: Any = None,
) -> "FluentPipeline":
    """Configure incremental loading for the pipeline.

    Args:
        cursor_field: Field to use as cursor (e.g., 'updated_at').
        initial_value: Starting value for the cursor.

    Returns:
        Self for method chaining.

    Raises:
        ValidationError: If cursor_field is invalid.

    Example:
        ```python
        pipeline.with_incremental("updated_at", initial_value="2024-01-01")
        ```
    """
    ...
```

### Error Handling (Required)

- Validate inputs early (fail fast)
- Provide clear, actionable error messages
- Use appropriate exception types
- Chain exceptions to preserve context

```python
# ✅ Good
if not cursor_field or not isinstance(cursor_field, str):
    raise ValidationError("cursor_field must be a non-empty string")

try:
    result = risky_operation()
except SomeError as e:
    raise PipelineExecutionError(f"Operation failed: {e}") from e

# ❌ Bad
result = risky_operation()  # No validation or error handling
```

### Logging (Recommended)

Use structured logging for important operations:

```python
import logging

logger = logging.getLogger(__name__)

logger.info(
    "Creating pipeline",
    extra={
        "source_type": type(source).__name__,
        "destination": destination,
    }
)
```

---

## 🧪 Testing Requirements

### All Changes Must Include Tests

- **Unit tests** for all new code
- **Integration tests** for end-to-end scenarios (when applicable)
- Minimum **85% coverage** for new code
- All tests must pass

### Running Tests

**Unit Tests (Fast, Mock-Based):**
```bash
# Run all unit tests
uv run pytest tests/unittest

# With coverage
uv run pytest tests/unittest --cov=src/fldt --cov-report=term-missing

# Specific test file
uv run pytest tests/unittest/test_fluent.py -v
```

**Integration Tests (Real Databases):**
```bash
# Install integration dependencies
uv sync --group integration

# Run with SQLite (default, no setup needed)
uv run pytest tests/integration -m integration

# Run with PostgreSQL (optional)
export TEST_POSTGRES_URL="postgresql://user:pass@localhost/test_db"
uv run pytest tests/integration -m integration
```

**All Tests:**
```bash
uv run pytest
```

### Writing Good Tests

```python
class TestFeature:
    """Test suite for specific feature."""

    def test_feature_with_valid_input(self):
        """Feature works with valid input."""
        result = feature(valid_input)
        assert result == expected_output

    def test_feature_validates_input(self):
        """Feature raises ValidationError for invalid input."""
        with pytest.raises(ValidationError) as exc_info:
            feature(invalid_input)
        assert "specific error message" in str(exc_info.value)

    def test_feature_handles_errors(self):
        """Feature handles errors gracefully."""
        with pytest.raises(PipelineExecutionError):
            feature(error_causing_input)
```

### Test Organization

- **Unit tests** (`tests/unittest/`): Mock-based, fast, no external dependencies
- **Integration tests** (`tests/integration/`): Real databases, Alembic-managed
- One test class per feature/method
- Descriptive test names (`test_feature_does_what_when_condition`)
- Group related tests in classes
- Use fixtures from `conftest.py` for common setups

**Test Databases:**
- DuckDB files from integration tests are stored in `tests/.test_dbs/`
- This directory is gitignored and cleaned before each test run
- All test pipelines write to a single `test.duckdb` file (efficient!)
- SQLite test databases are created in temporary files and cleaned up automatically

### Adding Integration Tests

Integration tests use Alembic migrations for test data:

**1. Create a new migration (if needed):**
```bash
cd tests/integration
alembic revision -m "create_new_table"
```

**2. Edit the migration file:**
```python
def upgrade() -> None:
    """Create new table and seed data."""
    op.create_table(
        'new_table',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(100)),
    )
    op.execute("INSERT INTO new_table (id, name) VALUES (1, 'Test')")
```

**3. Update `test_data.py` with expected data:**
```python
class TestData:
    NEW_TABLE = [{"id": 1, "name": "Test"}]
```

**4. Write integration test:**
```python
@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
def test_new_table(self, test_database, clean_dlt_state):
    result = FluentPipeline.from_sql_table(test_database, "new_table").to("duckdb").run()
    assert result is not None
```

---

## 📊 Code Quality (SonarCloud)

All pull requests must stay green on the SonarCloud quality gate. The GitHub Action workflow runs unit tests with coverage, uploads `coverage.xml`, and publishes the scan to SonarCloud. If the quality gate fails (bugs, code smells, low coverage), the PR cannot merge until the issues are resolved.

### Dashboard & Project Keys

- Sonar dashboard: `https://sonarcloud.io/project/overview?id=juanjodevio`
- Configuration lives in `sonar-project.properties`
- Secrets: repository-level `SONAR_TOKEN` plus the built-in `GITHUB_TOKEN`

Update `sonar.organization` and `sonar.projectKey` after the project is created in SonarCloud. Keep the `sonar.python.coverage.reportPaths=coverage.xml` entry untouched—coverage uploads rely on it.

### Local Workflow

```bash
# Run unit tests with XML coverage (matches CI)
uv run pytest tests/unittest --cov=src/fldt --cov-report=term-missing --cov-report=xml

# Optional: inspect coverage locally
coverage html
```

Before opening a PR:
1. Run the command above to regenerate `coverage.xml`
2. Fix any issues flagged by `uv run ruff check src tests`
3. If SonarCloud reports outstanding issues, link the remediation in your PR description

### Troubleshooting

- **Missing coverage in Sonar:** ensure `coverage.xml` is committed to `.gitignore` (already handled) but still present in the workspace before the Sonar step.
- **Analysis skipped on forks:** forked repos do not have `SONAR_TOKEN`—push to the main repo or request a temporary token from a maintainer.
- **False positives:** discuss in the PR and add a justification before using `# noqa` or `# pragma: no cover`.

---

## 🎨 Code Style

### Formatting

We use **black** for formatting:

```bash
# Format code
uv run black src tests

```

### Import Organization

Imports must be sorted with **isort** (enforced by ruff):

```python
# Standard library
import logging
from typing import Any

# Third-party
from dlt.sources.sql_database import sql_table

# Local
from fldt.exceptions import ValidationError
from fldt.types import PipelineConfig
```

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `FluentPipeline`, `PipelineBuilder`)
- **Functions/Methods**: `snake_case` (e.g., `set_source`, `add_transformer`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_DATASET`)
- **Private**: Prefix with `_` (e.g., `_validate_config`, `_builder`)

---

## 🔍 Code Review Checklist

Before submitting a PR, verify:

- [ ] All tests pass (`pytest -m "not integration"`)
- [ ] Type checking passes (`mypy src/fldt --strict`)
- [ ] Code is formatted (`black src tests`)
- [ ] Linting passes (`ruff check src tests`)
- [ ] Coverage ≥ 85% for new code
- [ ] Docstrings added for public APIs
- [ ] Error messages are clear and actionable
- [ ] No breaking changes to public API (or justified)
- [ ] CHANGELOG.md updated
- [ ] Examples in README updated (if applicable)

---

## 🚀 Contribution Workflow

### 1. Fork and Branch

```bash
# Fork the repo on GitHub, then:
git clone https://github.com/yourusername/fluentdlt.git
cd fluentdlt

# Create a feature branch
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Write code following the guidelines above
- Add tests for new functionality
- Update documentation as needed

### 3. Test Locally

```bash
# Run tests
uv run pytest -m "not integration"

# Check types
uv run mypy src/fldt --strict

# Check linting
uv run ruff check src tests

# Check coverage
uv run pytest --cov=src/fldt --cov-report=term-missing
```

### 4. Commit

Write clear, descriptive commit messages:

```bash
# Good commit message format:
git commit -m "feat: add support for Azure SQL sources

- Implement from_azure_sql_table() factory method
- Add Azure connection string validation
- Include comprehensive tests
- Update README with Azure examples
"
```

**Commit Message Prefixes:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test additions or changes
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Build/config changes

### 5. Submit Pull Request

- Push your branch to your fork
- Create a Pull Request against `main`
- Fill out the PR template
- Link related issues
- Request review from maintainers

---

## 🎯 Areas for Contribution

### High Priority

- Additional SQL database source support (MySQL, SQLite, etc.)
- File-based sources (`from_csv`, `from_json`, `from_parquet`)
- Cloud storage support (S3, Azure, GCS)
- More comprehensive integration tests
- Performance benchmarks

### Medium Priority

- Async/await support
- Retry logic and error recovery
- Pipeline result introspection
- CLI interface
- Additional transformation utilities

### Low Priority

- Additional convenience methods
- Performance optimizations
- Documentation improvements
- Example notebooks

---

## 📝 Pull Request Guidelines

### PR Title Format

```
[type]: Brief description (max 72 chars)

Examples:
feat: Add from_csv() factory method with S3 support
fix: Handle None values in TransformerChain
docs: Update README with incremental loading examples
```

### PR Description Template

```markdown
## Description
Brief description of the change and why it's needed.

## Type of Change
- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass locally

## Checklist
- [ ] Code follows project style guidelines
- [ ] Type hints added
- [ ] Docstrings added for public APIs
- [ ] Error handling implemented
- [ ] Tests achieve ≥85% coverage
- [ ] CHANGELOG.md updated
- [ ] README.md updated (if needed)

## Related Issues
Fixes #123
Relates to #456
```

---

## 🐛 Reporting Bugs

### Before Reporting

1. Check existing issues
2. Verify bug exists in latest version
3. Try to isolate minimal reproduction

### Bug Report Template

```markdown
## Bug Description
Clear description of the bug.

## To Reproduce
```python
from fldt import FluentPipeline

# Minimal code to reproduce
result = FluentPipeline.from_source(data).to("duckdb").run()
```

## Expected Behavior
What should happen.

## Actual Behavior
What actually happens (include error messages).

## Environment
- fldt version: 0.1.0
- Python version: 3.11.5
- OS: Ubuntu 22.04
- dlt version: 1.5.0

---

## 💡 Feature Requests

### Before Requesting

1. Check if feature already exists
2. Search existing feature requests
3. Consider if it fits project scope

### Feature Request Template

```markdown
## Feature Description
Clear description of the proposed feature.

## Use Case
Why is this needed? What problem does it solve?

## Proposed API
    ```python
    # How would the feature be used?
    result = FluentPipeline.from_new_source(...).to("duckdb").run()
    ```

## Alternatives Considered
What other approaches were considered?

## Implementation Notes
Any thoughts on implementation approach?
```

---

## 📚 Additional Resources

- [Project README](README.md)
- [Architecture Overview](README.md#architecture)
- [API Reference](README.md#api-reference)
- [PEP 8 - Style Guide](https://peps.python.org/pep-0008/)
- [PEP 20 - Zen of Python](https://peps.python.org/pep-0020/)
- [PEP 257 - Docstring Conventions](https://peps.python.org/pep-0257/)
- [dlt Documentation](https://dlthub.com/docs)

---

## 🤝 Code of Conduct

### Our Standards

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Assume good intentions
- Keep discussions professional

### Unacceptable Behavior

- Harassment or discriminatory language
- Personal attacks
- Trolling or inflammatory comments
- Publishing others' private information

---

## 📞 Getting Help

- **Questions**: Open a [GitHub Discussion](https://github.com/yourusername/fluentdlt/discussions)
- **Bugs**: Open a [GitHub Issue](https://github.com/yourusername/fluentdlt/issues)
- **Security**: Email security@example.com (do not open public issues)

---

## 🏆 Recognition

Contributors are recognized in:
- Project README
- Release notes
- Git commit history

Significant contributors may be added as project maintainers.

---

## 📄 License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

**Thank you for making fldt better! 🎉**

