#!/bin/bash

# Data Pipeline Runner Script
# Runs Bronze -> Silver -> Gold -> SQL Injection Detection in sequence

set -e  # Exit on any error

echo "🚀 Starting Data Pipeline Execution"
echo "=================================="

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

echo "📁 Working directory: $(pwd)"

# Activate Poetry environment
echo "🔧 Activating Poetry environment..."
poetry install

echo ""
echo "🥉 Step 1: Bronze Layer Ingestion"
echo "================================="
poetry run python -m upstream_home_test.pipelines.bronze_ingestion

echo ""
echo "🥈 Step 2: Silver Layer Transformation"
echo "====================================="
poetry run python -m upstream_home_test.pipelines.silver_transform

echo ""
echo "🥇 Step 3: Gold Layer Reports"
echo "============================"
poetry run python -m upstream_home_test.pipelines.gold_reports

echo ""
echo "🔍 Step 4: SQL Injection Detection"
echo "================================="
poetry run python -m upstream_home_test.utils.sql_injection_detector

echo ""
echo "✅ Pipeline execution completed successfully!"
echo "============================================"
echo "📊 Check the following directories for output:"
echo "   - Bronze: data/bronze/"
echo "   - Silver: data/silver/"
echo "   - Gold: data/gold/"
echo "   - Logs: logs/pipeline.log"
