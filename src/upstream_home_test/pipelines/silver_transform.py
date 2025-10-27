"""Silver layer transformation pipeline."""

import time
from pathlib import Path
from typing import Any, Dict

import polars as pl

from upstream_home_test.io.parquet_writer import ParquetWriteError, write_silver_parquet
from upstream_home_test.schemas.silver import GEAR_POSITION_MAPPING, map_gear_position
from upstream_home_test.utils.logging_config import log_pipeline_step, setup_logging


def run_silver_transform(
    bronze_dir: str = "data/bronze",
    output_path: str = "data/silver/vehicle_messages_cleaned.parquet"
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
    # Set up logging
    logger = setup_logging()
    
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
            # Clean manufacturer field
            pl.col("manufacturer").alias("manufacturer_original"),
            pl.col("manufacturer").str.strip_chars().alias("manufacturer_cleaned"),
            
            # Map gear positions to integers
            pl.col("gearPosition").map_elements(
                map_gear_position, 
                return_dtype=pl.Int64
            ).alias("gear_position"),
        ]).drop("gearPosition")  # Remove original gearPosition column
        
        # Step 3: Validate sample of cleaned data
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Validating cleaned data",
            metrics={"rows_to_validate": min(100, len(df_cleaned))}
        )
        
        # Sample validation (first 100 rows)
        sample_size = min(100, len(df_cleaned))
        if sample_size > 0:
            sample_df = df_cleaned.head(sample_size)
            validation_errors = validate_silver_sample(sample_df)
            
            if validation_errors:
                log_pipeline_step(
                    logger=logger,
                    step="silver_transform",
                    event=f"Validation found {len(validation_errors)} errors in sample",
                    metrics={"validation_errors": validation_errors[:5]},
                    level="WARNING"
                )
        
        # Step 4: Write to Silver layer
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Writing cleaned data to Silver layer",
            metrics={"output_rows": len(df_cleaned)}
        )
        
        write_stats = write_silver_parquet(df_cleaned, output_path)
        
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


def validate_silver_sample(df: pl.DataFrame) -> list[str]:
    """Validate a sample of Silver layer data.
    
    Args:
        df: Polars DataFrame to validate
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    try:
        # Check for required columns
        required_columns = ["vin", "manufacturer_original", "manufacturer_cleaned", "gear_position", "timestamp"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check VIN is not null
        null_vin_count = df.filter(pl.col("vin").is_null()).height
        if null_vin_count > 0:
            errors.append(f"Found {null_vin_count} rows with null VIN")
        
        # Check gear position values
        invalid_gear_count = df.filter(
            pl.col("gear_position").is_not_null() & 
            ~pl.col("gear_position").is_in([0, 1, 2, 3, 4])
        ).height
        if invalid_gear_count > 0:
            errors.append(f"Found {invalid_gear_count} rows with invalid gear positions")
        
        # Check manufacturer consistency
        inconsistent_manufacturer = df.filter(
            pl.col("manufacturer_original").str.strip_chars() != pl.col("manufacturer_cleaned")
        ).height
        if inconsistent_manufacturer > 0:
            errors.append(f"Found {inconsistent_manufacturer} rows with inconsistent manufacturer fields")
        
    except Exception as e:
        errors.append(f"Validation error: {str(e)}")
    
    return errors


def main():
    """CLI entry point for Silver transformation."""
    import sys
    
    try:
        # Parse command line arguments
        bronze_dir = "data/bronze"
        output_path = "data/silver/vehicle_messages_cleaned.parquet"
        
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
