"""Silver layer transformation pipeline."""

import time
from pathlib import Path
from typing import Any, Dict

import polars as pl

from upstream_home_test.io.parquet_writer import GenericParquetWriter, ParquetWriteError
from upstream_home_test.schemas.silver import GEAR_POSITION_MAPPING, map_gear_position
from upstream_home_test.utils.logging_config import log_pipeline_step, setup_logging


def run_silver_transform(
    bronze_dir: str = "data/bronze",
    output_path: str = "data/silver"
) -> Dict[str, Any]:
    """Run the complete Silver layer transformation pipeline.
    
    This pipeline:
    1. Reads all Bronze layer Parquet files
    2. Filters out rows with null VIN
    3. Cleans manufacturer field (removes trailing spaces)
    4. Maps gear positions to integers (P=0, R=1, N=2, D=3, L=4)
    5. Writes cleaned data to Silver layer Parquet file
    
    Args:
        bronze_dir: Directory containing Bronze layer Parquet files
        output_path: Output path for Silver layer Parquet file
        
    Returns:
        Dictionary with transformation statistics
        
    Raises:
        ParquetWriteError: If Parquet writing fails
    """
    # Set up logging (don't clear log file to preserve bronze logs)
    logger = setup_logging(clear_log_file=False)
    
    pipeline_start = time.time()
    
    try:
        # Step 1: Read Bronze layer data
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Starting Silver layer transformation",
            metrics={"bronze_dir": bronze_dir, "output_path": output_path}
        )
        
        # Scan all Bronze Parquet files
        bronze_pattern = f"{bronze_dir}/**/*.parquet"
        df = pl.scan_parquet(bronze_pattern).collect()
        
        if df.is_empty():
            log_pipeline_step(
                logger=logger,
                step="silver_transform",
                event="No Bronze data found to transform",
                metrics={"bronze_dir": bronze_dir},
                level="WARNING"
            )
            return {
                "status": "completed",
                "input_rows": 0,
                "output_rows": 0,
                "filtered_rows": 0,
                "duration_ms": 0
            }
        
        input_rows = len(df)
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event=f"Read {input_rows} rows from Bronze layer",
            metrics={"input_rows": input_rows}
        )
        
        # Step 2: Apply transformations
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Applying data transformations",
            metrics={"input_rows": input_rows}
        )
        
        # Filter out null VINs
        df_filtered = df.filter(pl.col("vin").is_not_null())
        filtered_rows = input_rows - len(df_filtered)
        
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event=f"Filtered {filtered_rows} rows with null VIN",
            metrics={"filtered_rows": filtered_rows, "remaining_rows": len(df_filtered)}
        )
        
        # Apply transformations
        df_cleaned = df_filtered.with_columns([
            # Clean manufacturer field in place (remove trailing spaces)
            pl.col("manufacturer").str.strip_chars().alias("manufacturer"),
            
            # Map gear positions to integers
            pl.col("gearPosition").map_elements(
                map_gear_position, 
                return_dtype=pl.Int64
            ).alias("gearPosition"),
        ])  # Remove original gearPosition column
        
        # Step 3: Write to Silver layer
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Writing cleaned data to Silver layer",
            metrics={"output_rows": len(df_cleaned)}
        )
        
        # Create generic parquet writer for Silver layer with partitioning (same as Bronze)
        writer = GenericParquetWriter(
            output_dir=output_path,
            partitioning_enabled=True,
            compression="zstd",
            logger=logger
        )
        
        write_stats = writer.write(df_cleaned)
        
        # Calculate total duration
        total_duration_ms = (time.time() - pipeline_start) * 1000
        
        # Log completion
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Silver layer transformation completed successfully",
            metrics={
                "input_rows": input_rows,
                "filtered_rows": filtered_rows,
                "output_rows": len(df_cleaned),
                "total_duration_ms": round(total_duration_ms, 2)
            }
        )
        
        return {
            "status": "completed",
            "input_rows": input_rows,
            "filtered_rows": filtered_rows,
            "output_rows": len(df_cleaned),
            "duration_ms": round(total_duration_ms, 2)
        }
        
    except ParquetWriteError as e:
        duration_ms = (time.time() - pipeline_start) * 1000
        error_msg = f"Parquet write error during Silver transformation: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise
        
    except Exception as e:
        duration_ms = (time.time() - pipeline_start) * 1000
        error_msg = f"Unexpected error during Silver transformation: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise RuntimeError(error_msg) from e


def main():
    """CLI entry point for Silver transformation."""
    import sys
    
    try:
        # Parse command line arguments
        bronze_dir = "data/bronze"
        output_path = "data/silver"
        
        if len(sys.argv) > 1:
            bronze_dir = sys.argv[1]
            print(f"Using bronze directory: {bronze_dir}")
        if len(sys.argv) > 2:
            output_path = sys.argv[2]
            print(f"Using output path: {output_path}")
        
        # Run pipeline
        result = run_silver_transform(bronze_dir, output_path)
        
        # Print results
        print(f"Silver transformation completed successfully!")
        print(f"Input rows: {result['input_rows']}")
        print(f"Filtered rows: {result['filtered_rows']}")
        print(f"Output rows: {result['output_rows']}")
        print(f"Duration: {result['duration_ms']:.2f}ms")
        
    except Exception as e:
        print(f"Silver transformation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
