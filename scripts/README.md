# Pipeline Execution Scripts

This directory contains executable scripts for running the medallion architecture data pipeline.

## Available Scripts

### 🥉 Bronze Layer Ingestion
**Script:** `bronze_ingest.py`  
**Command:** `poetry run bronze-ingest [amount]`

Fetches raw vehicle messages from the API and stores them as partitioned Parquet files in the Bronze layer.

**Examples:**
```bash
# Fetch 10,000 messages (default)
poetry run bronze-ingest

# Fetch 1,000 messages
poetry run bronze-ingest 1000

# Fetch 50,000 messages
poetry run bronze-ingest 50000
```

### 🥈 Silver Layer Transformation
**Script:** `silver_transform.py`  
**Command:** `poetry run silver-transform [bronze_dir] [output_path]`

Reads data from the Bronze layer, applies cleaning and standardization transformations, and writes cleaned data to the Silver layer.

**Examples:**
```bash
# Use default paths
poetry run silver-transform

# Custom bronze directory
poetry run silver-transform data/bronze

# Custom bronze directory and output path
poetry run silver-transform data/bronze data/silver/cleaned.parquet
```

### 🏗️ Complete Pipeline
**Script:** `run_pipeline.py`  
**Command:** `poetry run run-pipeline [amount] [bronze_dir] [output_path]`

Runs the complete pipeline from Bronze to Silver layers in sequence.

**Examples:**
```bash
# Run with defaults (10,000 messages)
poetry run run-pipeline

# Run with 5,000 messages
poetry run run-pipeline 5000

# Run with custom paths
poetry run run-pipeline 10000 data/bronze data/silver/
```

## Direct Script Execution

You can also run the scripts directly using Python (requires Poetry environment):

```bash
# Bronze ingestion
poetry run python scripts/bronze_ingest.py 1000

# Silver transformation
poetry run python scripts/silver_transform.py

# Complete pipeline
poetry run python scripts/run_pipeline.py 5000
```

## Output Structure

The scripts create the following directory structure:

```
data/
├── bronze/                          # Raw data (Bronze layer)
│   └── date=YYYY-MM-DD/
│       └── hour=HH/
│           └── data.parquet
└── silver/                          # Cleaned data (Silver layer)
    └── vehicle_messages_cleaned.parquet
```

## Features

- ✅ **Command-line arguments** for flexible execution
- ✅ **Progress indicators** with emojis and clear messaging
- ✅ **Error handling** with informative error messages
- ✅ **Performance metrics** showing duration and throughput
- ✅ **Data quality metrics** showing filtering rates
- ✅ **Structured logging** with JSON format for observability

## Requirements

- Python 3.10+ with Poetry
- All dependencies installed via `poetry install`
- API endpoint accessible at `http://localhost:9900`
