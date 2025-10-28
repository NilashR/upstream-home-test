#!/bin/bash

# Data Pipeline Runner Script
# Runs Bronze -> Silver -> Gold -> SQL Injection Detection in sequence

set -e  # Exit on any error

echo "🚀 Starting Data Pipeline Execution"
echo "=================================="

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

echo "📁 Working directory: $PROJECT_ROOT"

# Activate Poetry environment
echo "🔧 Activating Poetry environment..."
poetry install --no-dev

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
poetry run python -c "
from upstream_home_test.utils.sql_injection_detector import sql_injection_report, print_injection_report

# Common SQL injection patterns
patterns = [
    r\"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)\",
    r\"(\bOR\b|\bAND\b).*?(\bOR\b|\bAND\b)\",
    r\"(--|#|/\*|\*/)\",
    r\"(\bUNION\b.*?\bSELECT\b)\",
    r\"(\bDROP\b.*?\bTABLE\b)\"
]

# Columns to check for SQL injection
columns = ['vin', 'manufacturer', 'model']

print('Running SQL injection detection...')
report = sql_injection_report(columns, patterns)
print_injection_report(report)
"

echo ""
echo "✅ Pipeline execution completed successfully!"
echo "============================================"
echo "📊 Check the following directories for output:"
echo "   - Bronze: data/bronze/"
echo "   - Silver: data/silver/"
echo "   - Gold: data/gold/"
echo "   - Logs: logs/pipeline.log"
