# Upstream Home Test - Medallion Architecture Data Pipeline

A high-performance data pipeline implementing the medallion architecture (Bronze, Silver, Gold layers) using the latest Polars library for blazing-fast data processing, with comprehensive SQL injection detection and security monitoring.

## 🏗️ Architecture

- **Bronze Layer**: Raw vehicle messages from API, partitioned by date/hour as Parquet files
- **Silver Layer**: Cleaned and standardized data with manufacturer cleanup, VIN filtering, and gear position normalization  
- **Gold Layer**: Analytical reports with SQL-based queries and DuckDB processing
- **Security Layer**: SQL injection detection and monitoring with detailed reporting

## ✨ Key Features

- 🚀 **Latest Polars 1.34.0**: Blazing-fast DataFrame operations with Rust performance
- 📊 **Medallion Architecture**: Bronze → Silver → Gold data processing layers
- 🔄 **Batch Processing**: API ingestion with automatic partitioning and compression
- ✅ **Data Validation**: Pydantic v2 schemas with strict validation at layer boundaries
- 📝 **Structured Logging**: JSON logging with metrics and observability
- 🧪 **Comprehensive Testing**: 45+ tests covering essntial functionality
- 🔒 **Security Monitoring**: SQL injection detection with detailed violation reporting
- 📈 **Analytical Reports**: SQL-based Gold layer reports with DuckDB
- 🛠️ **Refactored Codebase**: Small functions with single responsibilities for maintainability

## 🚀 Quick Start

### Prerequisites
- Python 3.12+ (recommended)
- Poetry for dependency management

### Installation

1. **Install Poetry** (if not already installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Clone and setup the project**:
   ```bash
   git clone <repository-url>
   cd upstream_home_test
   poetry install
   poetry shell
   ```

3. **Run the complete pipeline**:
   ```bash
   poetry run python -m upstream_home_test.scripts.run_pipeline
   ```

## 📊 Data Pipeline Usage

### 🥉 Bronze Layer (Raw Data Ingestion)
```bash
# Fetch vehicle messages from API and store as partitioned Parquet files
poetry run python -m upstream_home_test.pipelines.bronze_ingestion

# With custom parameters
poetry run python -m upstream_home_test.pipelines.bronze_ingestion --amount 5000 --output-dir data/bronze
```

**CLI Arguments:**
- `--amount`: Number of messages to fetch from API (default: 10000)
- `--output-dir`: Output directory for Bronze parquet files (default: data/bronze)

**Features:**
- Fetches vehicle messages from REST API
- Validates data against Bronze schema using Pydantic v2
- Partitions data by date/hour for optimal query performance
- Splits large partitions (>125MB) into multiple files
- Uses zstd compression for efficient storage

### 🥈 Silver Layer (Data Transformation)
```bash
# Transform Bronze data: clean manufacturer, filter nulls, map gear positions
poetry run python -m upstream_home_test.pipelines.silver_transform

# With custom paths
poetry run python -m upstream_home_test.pipelines.silver_transform --bronze-dir data/bronze --output-path data/silver
```

**CLI Arguments:**
- `--bronze-dir`: Path to Bronze parquet directory (default: data/bronze)
- `--output-path`: Output directory for Silver parquet files (default: data/silver)

**Features:**
- Filters out rows with null VIN values
- Cleans manufacturer field (removes trailing spaces)
- Maps gear positions to integers 
- Handles timezone-aware timestamps
- Drops unused columns for efficiency

### 🥇 Gold Layer (Analytical Reports)
```bash
# Generate analytical reports from Silver data
poetry run python -m upstream_home_test.pipelines.gold_reports

# With specific reports
poetry run python -m upstream_home_test.pipelines.gold_reports --reports fastest_vehicles_per_hour vin_last_state --silver-dir data/silver
```

**CLI Arguments:**
- `--reports`: List of report names to run (default: fastest_vehicles_per_hour vin_last_state)
- `--silver-dir`: Path to Silver parquet directory (default: data/silver)

**Features:**
- SQL-based report generation using DuckDB
- Available reports:
  - `fastest_vehicles_per_hour`: Top speed analysis by hour
  - `vin_last_state`: Latest state for each vehicle VIN
- Automatic cleanup of old report files
- Parquet output with zstd compression

### 🔍 Security Monitoring (SQL Injection Detection)
```bash
# Scan Bronze data for potential SQL injection patterns
poetry run python -m upstream_home_test.utils.sql_injection_detector

# With custom parameters
poetry run python -m upstream_home_test.utils.sql_injection_detector --columns vin manufacturer --data-path data/bronze --output-dir data/sql_injection_report
```

**CLI Arguments:**
- `--columns`: Columns to scan for SQL injection patterns (default: vin manufacturer model)
- `--patterns`: Regex patterns to detect SQL injection (default: common SQL keywords/quotes)
- `--data-path`: Path to directory with parquet files to scan (default: data/bronze)
- `--output-dir`: Directory to write parquet report to (default: data/sql_injection_report)

**Features:**
- Scans all Bronze layer Parquet files
- Detects common SQL injection patterns using regex
- Generates detailed violation reports
- Saves results as Parquet files with full message context
- Checks columns: `vin`, `manufacturer`, `model`

## 🛠️ Complete Pipeline Execution

### Automated Pipeline Script
```bash
# Run the complete pipeline: Bronze → Silver → Gold → Security
poetry run python -m upstream_home_test.scripts.run_pipeline
```

This script executes:
1. **Bronze Ingestion**: Fetches 10,000 vehicle messages
2. **Silver Transformation**: Cleans and standardizes the data
3. **Gold Reports**: Generates analytical reports
4. **Security Scan**: Detects SQL injection patterns

### Manual Pipeline Steps
```bash
# Step 1: Ingest raw data
poetry run python -m upstream_home_test.pipelines.bronze_ingestion --amount 10000

# Step 2: Transform data
poetry run python -m upstream_home_test.pipelines.silver_transform --bronze-dir data/bronze --output-path data/silver

# Step 3: Generate reports
poetry run python -m upstream_home_test.pipelines.gold_reports --reports fastest_vehicles_per_hour vin_last_state --silver-dir data/silver

# Step 4: Security scan
poetry run python -m upstream_home_test.utils.sql_injection_detector --data-path data/bronze --output-dir data/sql_injection_report
```

## 📁 Output Structure

```
data/
├── bronze/                    # Raw vehicle messages
│   └── date=2025-10-28/
│       ├── hour=07/
│       │   └── data.parquet
│       └── hour=08/
│           └── data.parquet
├── silver/                    # Cleaned and transformed data
│   └── date=2025-10-28/
│       ├── hour=07/
│       │   └── data.parquet
│       └── hour=08/
│           └── data.parquet
├── gold/                      # Analytical reports
│   ├── fastest_vehicles_per_hour_20251028_104435.parquet
│   └── vin_last_state_20251028_104435.parquet
└── sql_injection_report/      # Security monitoring
    └── sql_injection_report.parquet
```

## 🔧 Development

### Code Quality
```bash
# Run all tests
poetry run pytest

# Run specific test files
poetry run pytest tests/test_bronze_ingestion.py
poetry run pytest tests/test_silver_transform.py
poetry run pytest tests/test_gold_reports.py
poetry run pytest tests/test_sql_injection_detector.py

# Format code
poetry run black .

# Lint code
poetry run ruff check .

# Type checking
poetry run mypy .
```

### Project Structure
```
src/upstream_home_test/
├── pipelines/                 # Data processing pipelines
│   ├── bronze_ingestion.py   # API data ingestion
│   ├── silver_transform.py   # Data cleaning and transformation
│   ├── gold_reports.py       # Analytical report generation
│   └── reports/              # SQL report definitions
│       ├── queries/          # SQL query files
│       └── sql_report_runner.py
├── io/                       # Input/Output utilities
│   ├── api_client.py         # REST API client
│   └── parquet_writer.py     # Parquet file operations
├── schemas/                  # Data validation schemas
│   ├── bronze.py            # Raw data schema
│   └── silver.py            # Cleaned data schema
├── utils/                    # Utility functions
│   ├── logging_config.py    # Logging configuration
│   ├── timing.py            # Performance timing utilities
│   └── sql_injection_detector.py  # Security monitoring
├── scripts/                  # Execution scripts
│   └── run_pipeline.sh      # Complete pipeline runner
└── constant.py              # Project constants
```

## 📊 Performance & Monitoring

### Logging
- **Structured JSON logging** with timestamps and metrics
- **Pipeline step tracking** with duration measurements
- **Error handling** with detailed error context
- **Log file**: `logs/pipeline.log`

### Metrics Tracked
- **Processing duration** in milliseconds
- **Row counts** (input, filtered, output)
- **File operations** (files written, partitions created)
- **API performance** (request duration, success rates)
- **Security violations** (patterns detected, affected rows)

## 🔒 Security Features

### SQL Injection Detection
- **Pattern matching** using regex for common SQL injection attempts
- **Multi-column scanning** across VIN, manufacturer, and model fields
- **Detailed reporting** with full message context as JSON
- **Parquet output** for further analysis and audit trails

### Data Validation
- **Pydantic v2 schemas** with strict validation
- **Type safety** with comprehensive type hints
- **Schema evolution** support for future data changes

## 🧪 Testing

The project includes comprehensive test coverage:

- **45+ test cases** across all modules
- **Unit tests** for individual functions and classes
- **Mock testing** for external dependencies


Run tests:
```bash
poetry run pytest -v                    # All tests
poetry run pytest tests/ -v             # All tests with      
```

## 📦 Dependencies

### Core Dependencies
- **Polars 1.34.0**: High-performance DataFrame operations
- **PyArrow 14.0.0**: Columnar data format support
- **Pydantic 2.5.0**: Data validation and serialization
- **httpx 0.26.0**: Modern HTTP client for API requests
- **DuckDB**: In-process SQL analytics engine

### Development Dependencies
- **pytest**: Testing framework
- **black**: Code formatting
- **ruff**: Fast Python linter
- **mypy**: Static type checking

## 🐍 Python Version Support

- **Python 3.12** (recommended)
- **Python 3.11** (supported)
- **Python 3.10** (supported)
