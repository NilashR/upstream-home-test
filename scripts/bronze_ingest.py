#!/usr/bin/env python3
"""Bronze layer ingestion script.

This script fetches vehicle messages from the API and stores them as partitioned Parquet files
in the Bronze layer of the medallion architecture.

Usage:
    python scripts/bronze_ingest.py [amount]
    
Examples:
    python scripts/bronze_ingest.py          # Fetch 10,000 messages (default)
    python scripts/bronze_ingest.py 1000     # Fetch 1,000 messages
    python scripts/bronze_ingest.py 50000    # Fetch 50,000 messages
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from upstream_home_test.pipelines.bronze_ingestion import run_bronze_ingestion


def main():
    """Main entry point for Bronze ingestion script."""
    try:
        # Parse command line arguments
        amount = 10000
        if len(sys.argv) > 1:
            try:
                amount = int(sys.argv[1])
                print(f"🚀 Starting Bronze ingestion with {amount:,} messages...")
            except ValueError:
                print(f"❌ Invalid amount argument: {sys.argv[1]}. Using default: 10,000")
                amount = 10000
        else:
            print(f"🚀 Starting Bronze ingestion with default amount: {amount:,} messages...")
        
        # Run pipeline
        result = run_bronze_ingestion(amount)
        
        # Print results
        print("\n✅ Bronze ingestion completed successfully!")
        print(f"📊 Messages fetched: {result['messages_fetched']:,}")
        print(f"📁 Files written: {result['files_written']}")
        print(f"🗂️  Partitions created: {result['partitions']}")
        print(f"⏱️  Duration: {result['duration_ms']:.2f}ms")
        
    except KeyboardInterrupt:
        print("\n⚠️  Bronze ingestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Bronze ingestion failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
