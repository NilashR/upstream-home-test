#!/usr/bin/env python3
"""Silver layer transformation script.

This script reads data from the Bronze layer, applies cleaning and standardization
transformations, and writes the cleaned data to the Silver layer.

Usage:
    python scripts/silver_transform.py [bronze_dir] [output_path]
    
Examples:
    python scripts/silver_transform.py                                    # Use default paths
    python scripts/silver_transform.py data/bronze                        # Custom bronze directory
    python scripts/silver_transform.py data/bronze data/silver/cleaned.parquet  # Custom paths
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from upstream_home_test.pipelines.silver_transform import run_silver_transform


def main():
    """Main entry point for Silver transformation script."""
    try:
        # Parse command line arguments
        bronze_dir = "data/bronze"
        output_path = "data/silver/vehicle_messages_cleaned.parquet"
        
        if len(sys.argv) > 1:
            bronze_dir = sys.argv[1]
            print(f"📂 Using bronze directory: {bronze_dir}")
        if len(sys.argv) > 2:
            output_path = sys.argv[2]
            print(f"💾 Using output path: {output_path}")
        
        print(f"🔄 Starting Silver transformation...")
        print(f"   📥 Input: {bronze_dir}")
        print(f"   📤 Output: {output_path}")
        
        # Run pipeline
        result = run_silver_transform(bronze_dir, output_path)
        
        # Print results
        print("\n✅ Silver transformation completed successfully!")
        print(f"📊 Input rows: {result['input_rows']:,}")
        print(f"🚫 Filtered rows: {result['filtered_rows']:,}")
        print(f"📈 Output rows: {result['output_rows']:,}")
        print(f"⏱️  Duration: {result['duration_ms']:.2f}ms")
        
        # Calculate filtering percentage
        if result['input_rows'] > 0:
            filter_pct = (result['filtered_rows'] / result['input_rows']) * 100
            print(f"📉 Filtering rate: {filter_pct:.1f}%")
        
    except KeyboardInterrupt:
        print("\n⚠️  Silver transformation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Silver transformation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
