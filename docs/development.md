---
title: Development
---

# Development & Contributing

Contributions are welcome. Please open GitHub issues for bugs and proposals.

## Project Layout

- `django_durable/`: app package
  - `models.py`: `WorkflowExecution`, `HistoryEvent`, `ActivityTask`
  - `engine.py`: durable replay, worker helpers, signals
  - `registry.py`: `register` decorators
  - `retry.py`: `RetryPolicy` and backoff helpers
  - `management/commands/`: worker and utilities (`durable_*`)
  - `admin.py`: Django admin integration
- `testproj/`: sample project, integration tests, and benchmark
- `docs/`: Sphinx + MyST Markdown documentation

## Environment Setup

```bash
git clone https://github.com/<you>/django-durable
cd django-durable
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
```

## Testing

```bash
python manage.py migrate --noinput
pytest -q
```

The test project (`testproj/`) includes end-to-end tests for workers, retries, timeouts, signals, and synchronization helpers.

## Style and Type Checks

```bash
ruff check .
isort --check-only .
mypy
```

Auto-format:

```bash
ruff format . && isort .
```

## Docs

Build documentation locally (HTML):

```bash
sphinx-build -b html docs docs/_build/html
```

Or via Nox:

```bash
nox -s docs
```

Markdown is parsed with MyST; code reference uses `autodoc`.

## CI

If GitHub Actions is enabled, typical steps are: install, lint, type check, migrate DB, run tests, build docs.

## Release & Versioning

- Semantic Versioning: MAJOR.MINOR.PATCH.
- Bump `version` in `pyproject.toml`.
- Tag releases as `vX.Y.Z`.
- Publish to PyPI using your preferred workflow.

