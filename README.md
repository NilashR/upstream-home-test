# Upstream Home Test

A Python project using Poetry for dependency management.

## Setup

1. Install Poetry if you haven't already:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. Install dependencies:
   ```bash
   poetry install
   ```

3. Activate the virtual environment:
   ```bash
   poetry shell
   ```

## Development

- Run tests: `poetry run pytest`
- Format code: `poetry run black .`
- Lint code: `poetry run ruff check .`
- Type check: `poetry run mypy .`

## Python Version

This project supports Python 3.10, 3.11, and 3.12.
