"""Silver layer transformation pipeline."""
import sys
import time
from pathlib import Path
from typing import Any, Dict

import polars as pl

from upstream_home_test.io.parquet_writer import GenericParquetWriter, ParquetWriteError
from upstream_home_test.schemas.silver import map_gear_position
from upstream_home_test.utils.logging_config import log_pipeline_step, setup_logging
from upstream_home_test.utils.timing import elapsed_ms_since
from upstream_home_test.constant import BRONZE_PATH, SILVER_PATH, SILVER_LAYER


def _resolve_paths(bronze_dir: str = None, output_path: str = None) -> tuple[str, str]:
    """Resolve absolute paths for bronze directory and output path.
    
    Args:
        bronze_dir: Bronze directory path (None for default)
        output_path: Output path (None for default)
        
    Returns:
        Tuple of (bronze_dir, output_path) as absolute paths
    """
    from upstream_home_test.utils.logging_config import get_project_root
    
    project_root = get_project_root()
    
    if bronze_dir is None:
        bronze_dir = str(project_root / BRONZE_PATH)
    
    if output_path is None:
        output_path = str(project_root / SILVER_PATH)
    
    return bronze_dir, output_path


def _check_bronze_files_exist(bronze_dir: str, logger) -> bool:
    """Check if Bronze parquet files exist in the directory.
    
    Args:
        bronze_dir: Directory to check for parquet files
        logger: Logger instance for logging
        
    Returns:
        True if parquet files exist, False otherwise
    """
    if not any(Path(bronze_dir).rglob("*.parquet")):
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="No Bronze parquet files found",
            metrics={"bronze_dir": bronze_dir},
            level="WARNING",
        )
        return False
    return True


def _read_bronze_data(bronze_dir: str, logger) -> pl.DataFrame:
    """Read and validate Bronze layer data.
    
    Args:
        bronze_dir: Directory containing Bronze parquet files
        logger: Logger instance for logging
        
    Returns:
        Polars DataFrame with Bronze data
        
    Raises:
        ValueError: If no data found to transform
    """
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
        raise ValueError("No Bronze data found to transform")
    
    input_rows = len(df)
    log_pipeline_step(
        logger=logger,
        step=SILVER_LAYER,
        event=f"Read {input_rows} rows from Bronze layer",
        metrics={"input_rows": input_rows}
    )
    
    return df


def _filter_null_vins(df: pl.DataFrame, logger) -> tuple[pl.DataFrame, int]:
    """Filter out rows with null VIN values.
    
    Args:
        df: Input DataFrame
        logger: Logger instance for logging
        
    Returns:
        Tuple of (filtered_dataframe, filtered_row_count)
    """
    input_rows = len(df)
    df_filtered = df.filter(pl.col("vin").is_not_null())
    filtered_rows = input_rows - len(df_filtered)
    
    log_pipeline_step(
        logger=logger,
        step=SILVER_LAYER,
        event=f"Filtered {filtered_rows} rows with null VIN",
        metrics={"filtered_rows": filtered_rows, "remaining_rows": len(df_filtered)}
    )
    
    return df_filtered, filtered_rows


def _apply_data_transformations(df: pl.DataFrame, logger) -> pl.DataFrame:
    """Apply all data transformations to the DataFrame.
    
    Args:
        df: Input DataFrame
        logger: Logger instance for logging
        
    Returns:
        Transformed DataFrame
    """
    log_pipeline_step(
        logger=logger,
        step=SILVER_LAYER,
        event="Applying data transformations",
        metrics={"input_rows": len(df)}
    )
    
    # Create derived columns and drop specified raw columns
    df_with_gear = df.with_columns([
        # Map gear positions to integers before dropping the raw column
        pl.col("gearPosition").map_elements(
            map_gear_position,
            return_dtype=pl.Int64,
        ).alias("gear_position"),
    ])

    # Apply remaining transformations
    df_cleaned = df_with_gear.with_columns([
        pl.col("frontLeftDoorState").alias("front_left_door_state"),
        pl.col("wipersState").alias("wipers_state"),
        pl.col("driverSeatbeltState").alias("driver_seatbelt_state"),
        # Clean manufacturer field in place (remove trailing spaces)
        pl.col("manufacturer").str.strip_chars().alias("manufacturer"),
        # Ensure timestamp is in UTC timezone
        pl.col("timestamp").dt.replace_time_zone("UTC").alias("timestamp"),
    ])

    # Drop unused columns
    df_final = df_cleaned.drop(["gearPosition", "frontLeftDoorState", "driverSeatbeltState", "wipersState"])
    
    return df_final


def _write_silver_data(df: pl.DataFrame, output_path: str, logger) -> Dict[str, Any]:
    """Write transformed data to Silver layer.
    
    Args:
        df: Transformed DataFrame to write
        output_path: Output directory path
        logger: Logger instance for logging
        
    Returns:
        Write statistics dictionary
    """
    log_pipeline_step(
        logger=logger,
        step=SILVER_LAYER,
        event="Writing cleaned data to Silver layer",
        metrics={"output_rows": len(df)}
    )
    
    # Create generic parquet writer for Silver layer with partitioning (same as Bronze)
    writer = GenericParquetWriter(
        output_dir=output_path,
        partitioning_enabled=True,
        compression="zstd",
        logger=logger
    )
    
    return writer.write(df)


def _create_empty_result() -> Dict[str, Any]:
    """Create an empty result dictionary for cases with no data.
    
    Returns:
        Empty result dictionary
    """
    return {
        "status": "completed",
        "input_rows": 0,
        "output_rows": 0,
        "filtered_rows": 0,
        "duration_ms": 0,
    }


def run_silver_transform(
    bronze_dir: str = None,
    output_path: str = None
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
    # Resolve absolute paths
    bronze_dir, output_path = _resolve_paths(bronze_dir, output_path)
    
    # Set up logging (don't clear log file to preserve bronze logs)
    logger = setup_logging(clear_log_file=False)
    
    pipeline_start = time.time()
    
    try:
        # Step 1: Check if Bronze files exist
        log_pipeline_step(
            logger=logger,
            step="silver_transform",
            event="Starting Silver layer transformation",
            metrics={"bronze_dir": bronze_dir, "output_path": output_path}
        )
        
        if not _check_bronze_files_exist(bronze_dir, logger):
            return _create_empty_result()

        # Step 2: Read Bronze data
        try:
            df = _read_bronze_data(bronze_dir, logger)
        except ValueError as e:
            if "No Bronze data found to transform" in str(e):
                return _create_empty_result()
            raise
        
        # Step 3: Filter null VINs
        df_filtered, filtered_rows = _filter_null_vins(df, logger)
        
        # Step 4: Apply transformations
        df_cleaned = _apply_data_transformations(df_filtered, logger)
        
        # Step 5: Write to Silver layer
        write_stats = _write_silver_data(df_cleaned, output_path, logger)
        
        # Calculate total duration
        total_duration_ms = elapsed_ms_since(pipeline_start)
        
        # Log completion
        log_pipeline_step(
            logger=logger,
            step=SILVER_LAYER,
            event="Silver layer transformation completed successfully",
            metrics={
                "input_rows": len(df),
                "filtered_rows": filtered_rows,
                "output_rows": len(df_cleaned),
                "total_duration_ms": round(total_duration_ms, 2)
            }
        )
        
        return {
            "status": "completed",
            "input_rows": len(df),
            "filtered_rows": filtered_rows,
            "output_rows": len(df_cleaned),
            "duration_ms": round(total_duration_ms, 2)
        }
        
    except ParquetWriteError as e:
        duration_ms = elapsed_ms_since(pipeline_start)
        error_msg = f"Parquet write error during Silver transformation: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step=SILVER_LAYER,
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise
        
    except Exception as e:
        duration_ms = elapsed_ms_since(pipeline_start)
        error_msg = f"Unexpected error during Silver transformation: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step=SILVER_LAYER,
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise RuntimeError(error_msg) from e


def main():    
    try:
        # Parse command line arguments
        bronze_dir = BRONZE_PATH
        output_path = SILVER_PATH
        
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
