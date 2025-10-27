# Upstream Home Test - Medallion Architecture Data Pipeline

A high-performance data pipeline implementing the medallion architecture (Bronze, Silver, Gold layers) using the latest Polars library for blazing-fast data processing.

## Architecture

- **Bronze Layer**: Raw vehicle messages from API, partitioned by date/hour as Parquet files
- **Silver Layer**: Cleaned and standardized data with manufacturer cleanup, VIN filtering, and gear position normalization  
- **Gold Layer**: Analytical reports (future implementation)

## Key Features

- 🚀 **Latest Polars 1.34.0**: Blazing-fast DataFrame operations with Rust performance
- 📊 **Medallion Architecture**: Bronze → Silver → Gold data processing layers
- 🔄 **Real-time Processing**: API ingestion with automatic partitioning and compression
- ✅ **Data Validation**: Pydantic v2 schemas with strict validation at layer boundaries
- 📝 **Structured Logging**: JSON logging with metrics and observability
- 🧪 **Comprehensive Testing**: 25+ tests covering all functionality

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

## Usage

### Bronze Layer (Raw Data Ingestion)
```bash
# Fetch 10K messages from API and store as partitioned Parquet files
poetry run bronze-ingest [amount]

# Example: Fetch 1000 messages
poetry run bronze-ingest 1000
```

### Silver Layer (Data Transformation)
```bash
# Transform Bronze data: clean manufacturer, filter nulls, map gear positions
poetry run silver-transform
```

### Development Commands
- Run tests: `poetry run pytest`
- Format code: `poetry run black .`
- Lint code: `poetry run ruff check .`
- Type check: `poetry run mypy .`

## Dependencies

- **Polars 1.34.0**: Latest stable version with performance improvements
- **PyArrow 14.0.0**: High-performance columnar data format
- **Pydantic 2.5.0**: Data validation and serialization
- **httpx 0.26.0**: Modern HTTP client for API requests

## Python Version

This project supports Python 3.10, 3.11, and 3.12.
