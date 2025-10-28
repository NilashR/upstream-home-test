"""Bronze layer ingestion pipeline."""
import sys
import time
from pathlib import Path
from typing import Any
import argparse

from upstream_home_test.constant import BRONZE_LAYER, BRONZE_PATH
from upstream_home_test.io.api_client import APIError, fetch_vehicle_messages
from upstream_home_test.io.parquet_writer import ParquetWriteError, write_parquet
from upstream_home_test.schemas.bronze import VehicleMessageRaw
from upstream_home_test.utils.logging_config import (
    get_project_root,
    log_pipeline_step,
    setup_logging,
)
from upstream_home_test.utils.timing import elapsed_ms_since


def _resolve_output_directory(output_dir: str | None = None) -> str:
    """Resolve absolute path for output directory.

    Args:
        output_dir: Output directory path (None for default)

    Returns:
        Absolute path to output directory
    """
    if output_dir is None:
        project_root = get_project_root()
        return str(project_root / BRONZE_PATH)

    # Convert relative path to absolute if needed
    if not Path(output_dir).is_absolute():
        project_root = get_project_root()
        return str(project_root / output_dir)

    return output_dir


def _fetch_messages_from_api(amount: int, logger) -> list[dict[str, Any]]:
    """Fetch vehicle messages from the API.

    Args:
        amount: Number of messages to fetch
        logger: Logger instance for logging

    Returns:
        List of vehicle message dictionaries

    Raises:
        APIError: If API fetch fails
    """
    log_pipeline_step(
        logger=logger,
        step=BRONZE_LAYER,
        event="Starting Bronze layer ingestion",
        metrics={"amount": amount},
    )

    messages = fetch_vehicle_messages(amount)

    if not messages:
        log_pipeline_step(
            logger=logger,
            step=BRONZE_LAYER,
            event="No messages received from API",
            metrics={"amount_requested": amount, "amount_received": 0},
            level="WARNING",
        )
        raise ValueError("No messages received from API")

    return messages


def _write_messages_to_bronze(
    messages: list[dict[str, Any]], output_dir: str, logger
) -> dict[str, Any]:
    """Write messages to Bronze layer parquet files.

    Args:
        messages: List of vehicle message dictionaries
        output_dir: Output directory path
        logger: Logger instance for logging

    Returns:
        Write statistics dictionary

    Raises:
        ParquetWriteError: If Parquet writing fails
    """
    log_pipeline_step(
        logger=logger,
        step=BRONZE_LAYER,
        event="Writing messages to Bronze layer",
        metrics={"messages": len(messages)},
    )

    return write_parquet(
        messages,
        output_dir=output_dir,
        partitioning_enabled=True,
        validator_model=VehicleMessageRaw,
        logger=logger,
    )


def _create_empty_bronze_result() -> dict[str, Any]:
    """Create an empty result dictionary for cases with no messages.

    Returns:
        Empty result dictionary
    """
    return {
        "status": "completed",
        "messages_fetched": 0,
        "files_written": 0,
        "partitions": 0,
        "duration_ms": 0,
    }


def _log_bronze_completion(
    messages: list[dict[str, Any]],
    write_stats: dict[str, Any],
    total_duration_ms: float,
    logger,
) -> None:
    """Log Bronze ingestion completion.

    Args:
        messages: List of fetched messages
        write_stats: Write statistics from parquet writer
        total_duration_ms: Total pipeline duration in milliseconds
        logger: Logger instance for logging
    """
    log_pipeline_step(
        logger=logger,
        step=BRONZE_LAYER,
        event="Bronze layer ingestion completed successfully",
        metrics={
            "messages_fetched": len(messages),
            "files_written": write_stats["files_written"],
            "partitions": write_stats["partitions"],
            "total_duration_ms": round(total_duration_ms, 2),
        },
    )


def _handle_bronze_error(
    error: Exception, error_type: str, duration_ms: float, logger
) -> None:
    """Handle and log Bronze ingestion errors.

    Args:
        error: The exception that occurred
        error_type: Type of error (API, Parquet, etc.)
        duration_ms: Duration before error occurred
        logger: Logger instance for logging
    """
    error_msg = f"{error_type} error during Bronze ingestion: {error!s}"

    log_pipeline_step(
        logger=logger,
        step=BRONZE_LAYER,
        event=error_msg,
        metrics={"error": str(error), "duration_ms": round(duration_ms, 2)},
        level="ERROR",
    )


def run_bronze_ingestion(
    amount: int = 10000, output_dir: str | None = None
) -> dict[str, Any]:
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

    # Resolve output directory
    output_dir = _resolve_output_directory(output_dir)

    pipeline_start = time.time()

    try:
        # Step 1: Fetch messages from API
        messages = _fetch_messages_from_api(amount, logger)

        # Step 2: Write to Bronze layer
        write_stats = _write_messages_to_bronze(messages, output_dir, logger)

        # Calculate total duration
        total_duration_ms = elapsed_ms_since(pipeline_start)

        # Log completion
        _log_bronze_completion(messages, write_stats, total_duration_ms, logger)

        return {
            "status": "completed",
            "messages_fetched": len(messages),
            "files_written": write_stats["files_written"],
            "partitions": write_stats["partitions"],
            "duration_ms": round(total_duration_ms, 2),
        }

    except ValueError as e:
        # Handle case where no messages were fetched
        if "No messages received from API" in str(e):
            return _create_empty_bronze_result()
        raise

    except APIError as e:
        duration_ms = elapsed_ms_since(pipeline_start)
        _handle_bronze_error(e, "API", duration_ms, logger)
        raise

    except ParquetWriteError as e:
        duration_ms = elapsed_ms_since(pipeline_start)
        _handle_bronze_error(e, "Parquet write", duration_ms, logger)
        raise

    except Exception as e:
        duration_ms = elapsed_ms_since(pipeline_start)
        _handle_bronze_error(e, "Unexpected", duration_ms, logger)
        raise RuntimeError(f"Unexpected error during Bronze ingestion: {e!s}") from e


def main():
    try:
        parser = argparse.ArgumentParser(description="Run Bronze layer ingestion")
        parser.add_argument(
            "--amount",
            type=int,
            default=10000,
            help="Number of messages to fetch from API",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default=BRONZE_PATH,
            help="Output directory for Bronze parquet files",
        )
        args = parser.parse_args()

        # Run pipeline
        result = run_bronze_ingestion(args.amount, args.output_dir)

        # Print results
        print("Bronze ingestion completed successfully!")
        print(f"Messages fetched: {result['messages_fetched']}")
        print(f"Files written: {result['files_written']}")
        print(f"Partitions created: {result['partitions']}")
        print(f"Duration: {result['duration_ms']:.2f}ms")

    except Exception as e:
        print(f"{BRONZE_LAYER} failed: {e!s}")
        sys.exit(1)


if __name__ == "__main__":
    main()
