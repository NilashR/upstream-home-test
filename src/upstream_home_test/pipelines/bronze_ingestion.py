"""Bronze layer ingestion pipeline."""

import time
from pathlib import Path
from typing import Any, Dict

from upstream_home_test.io.api_client import APIError, fetch_vehicle_messages
from upstream_home_test.io.parquet_writer import ParquetWriteError, write_bronze_parquet
from upstream_home_test.utils.logging_config import get_project_root, log_pipeline_step, setup_logging

BRONZE_LAYER = 'bronze_ingestion'


def run_bronze_ingestion(amount: int = 10000, output_dir: str | None = None) -> Dict[str, Any]:
    """Run the complete Bronze layer ingestion pipeline.
    
    This pipeline:
    1. Fetches vehicle messages from the API
    2. Validates messages against Bronze schema
    3. Partitions messages by date/hour
    4. Splits large partitions (>125MB) into multiple files
    5. Writes partitioned Parquet files with compression
    
    Args:
        amount: Number of messages to fetch from API
        output_dir: Output directory for Bronze layer files
        
    Returns:
        Dictionary with pipeline execution statistics
        
    Raises:
        APIError: If API fetch fails
        ParquetWriteError: If Parquet writing fails
    """
    # Set up logging
    logger = setup_logging()
    
    # Use absolute path for output directory
    if output_dir is None:
        project_root = get_project_root()
        output_dir = str(project_root / "data" / "bronze")
    else:
        # Convert relative path to absolute if needed
        if not Path(output_dir).is_absolute():
            project_root = get_project_root()
            output_dir = str(project_root / output_dir)
    
    pipeline_start = time.time()
    
    try:
        # Step 1: Fetch messages from API
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event="Starting Bronze layer ingestion",
            metrics={"amount": amount, "output_dir": output_dir}
        )
        
        messages = fetch_vehicle_messages(amount)
        
        if not messages:
            log_pipeline_step(
                logger=logger,
                step=BRONZE_LAYER,
                event="No messages received from API",
                metrics={"amount_requested": amount, "amount_received": 0},
                level="WARNING"
            )
            return {
                "status": "completed",
                "messages_fetched": 0,
                "files_written": 0,
                "partitions": 0,
                "duration_ms": 0
            }
        
        # Step 2: Write to Bronze layer
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event="Writing messages to Bronze layer",
            metrics={"messages": len(messages)}
        )
        
        write_stats = write_bronze_parquet(messages, output_dir, logger)
        
        # Calculate total duration
        total_duration_ms = (time.time() - pipeline_start) * 1000
        
        # Log completion
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event="Bronze layer ingestion completed successfully",
            metrics={
                "messages_fetched": len(messages),
                "files_written": write_stats["files_written"],
                "partitions": write_stats["partitions"],
                "total_duration_ms": round(total_duration_ms, 2)
            }
        )
        
        return {
            "status": "completed",
            "messages_fetched": len(messages),
            "files_written": write_stats["files_written"],
            "partitions": write_stats["partitions"],
            "duration_ms": round(total_duration_ms, 2)
        }
        
    except APIError as e:
        duration_ms = (time.time() - pipeline_start) * 1000
        error_msg = f"API error during Bronze ingestion: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise
        
    except ParquetWriteError as e:
        duration_ms = (time.time() - pipeline_start) * 1000
        error_msg = f"Parquet write error during Bronze ingestion: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise
        
    except Exception as e:
        duration_ms = (time.time() - pipeline_start) * 1000
        error_msg = f"Unexpected error during Bronze ingestion: {str(e)}"
        
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event=error_msg,
            metrics={"error": str(e), "duration_ms": round(duration_ms, 2)},
            level="ERROR"
        )
        
        raise RuntimeError(error_msg) from e


def main():
    """CLI entry point for Bronze ingestion."""
    import sys
    
    try:
        # Default values
        amount = 10000
        output_dir = "data/bronze"

        # Parse command line arguments
        if len(sys.argv) > 1:
            try:
                amount = int(sys.argv[1])
                print(f"Using amount from command line: {amount}")
            except ValueError:
                print(f"Invalid amount argument: {sys.argv[1]}. Using default: {amount}")
        else:
            print(f"No amount specified, using default: {amount}")
            
        # Parse output directory if provided as second argument
        if len(sys.argv) > 2:
            output_dir = sys.argv[2]
            print(f"Using output directory from command line: {output_dir}")
        else:
            print(f"No output directory specified, using default: {output_dir}")
        
        # Run pipeline
        result = run_bronze_ingestion(amount, output_dir)
        
        # Print results
        print(f"Bronze ingestion completed successfully!")
        print(f"Messages fetched: {result['messages_fetched']}")
        print(f"Files written: {result['files_written']}")
        print(f"Partitions created: {result['partitions']}")
        print(f"Duration: {result['duration_ms']:.2f}ms")
        
    except Exception as e:
        print(f"Bronze ingestion failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
