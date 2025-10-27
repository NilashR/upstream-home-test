#!/usr/bin/env python3
"""Complete medallion architecture pipeline runner.

This script runs the complete data pipeline from Bronze to Silver layers.

Usage:
    python scripts/run_pipeline.py [amount] [bronze_dir] [output_path]
    
Examples:
    python scripts/run_pipeline.py                                    # Run with defaults
    python scripts/run_pipeline.py 5000                              # Fetch 5,000 messages
    python scripts/run_pipeline.py 10000 data/bronze data/silver/    # Custom paths
"""

import sys
import time
from pathlib import Path

# Add the src directory to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from upstream_home_test.pipelines.bronze_ingestion import run_bronze_ingestion
from upstream_home_test.pipelines.silver_transform import run_silver_transform


def main():
    """Main entry point for complete pipeline execution."""
    try:
        # Parse command line arguments
        amount = 10000
        bronze_dir = "data/bronze"
        output_path = "data/silver/vehicle_messages_cleaned.parquet"
        
        if len(sys.argv) > 1:
            try:
                amount = int(sys.argv[1])
            except ValueError:
                print(f"❌ Invalid amount argument: {sys.argv[1]}. Using default: 10,000")
                amount = 10000
        
        if len(sys.argv) > 2:
            bronze_dir = sys.argv[2]
        if len(sys.argv) > 3:
            output_path = sys.argv[3]
        
        print("🏗️  Starting Medallion Architecture Pipeline")
        print("=" * 50)
        print(f"📊 Amount: {amount:,} messages")
        print(f"📂 Bronze directory: {bronze_dir}")
        print(f"💾 Silver output: {output_path}")
        print("=" * 50)
        
        pipeline_start = time.time()
        
        # Step 1: Bronze Layer Ingestion
        print("\n🥉 STEP 1: Bronze Layer Ingestion")
        print("-" * 40)
        bronze_result = run_bronze_ingestion(amount, bronze_dir)
        
        print(f"✅ Bronze completed: {bronze_result['messages_fetched']:,} messages")
        print(f"   📁 Files: {bronze_result['files_written']}")
        print(f"   🗂️  Partitions: {bronze_result['partitions']}")
        print(f"   ⏱️  Duration: {bronze_result['duration_ms']:.2f}ms")
        
        # Step 2: Silver Layer Transformation
        print("\n🥈 STEP 2: Silver Layer Transformation")
        print("-" * 40)
        silver_result = run_silver_transform(bronze_dir, output_path)
        
        print(f"✅ Silver completed: {silver_result['output_rows']:,} cleaned rows")
        print(f"   📊 Input: {silver_result['input_rows']:,}")
        print(f"   🚫 Filtered: {silver_result['filtered_rows']:,}")
        print(f"   ⏱️  Duration: {silver_result['duration_ms']:.2f}ms")
        
        # Pipeline Summary
        total_duration = time.time() - pipeline_start
        print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print(f"📊 Total messages processed: {bronze_result['messages_fetched']:,}")
        print(f"📈 Cleaned messages: {silver_result['output_rows']:,}")
        print(f"🚫 Filtered out: {silver_result['filtered_rows']:,}")
        print(f"⏱️  Total duration: {total_duration:.2f}s")
        
        # Calculate efficiency metrics
        if silver_result['input_rows'] > 0:
            filter_rate = (silver_result['filtered_rows'] / silver_result['input_rows']) * 100
            print(f"📉 Data quality: {100 - filter_rate:.1f}% valid records")
        
        print("\n📁 Output files:")
        print(f"   🥉 Bronze: {bronze_dir}/")
        print(f"   🥈 Silver: {output_path}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
