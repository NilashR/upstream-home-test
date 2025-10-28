"""Parquet writer for Bronze layer with partitioning and compression."""

import time
from pathlib import Path
from typing import Any, Literal, Optional

import polars as pl
from pydantic import BaseModel, ValidationError

from upstream_home_test.utils.logging_config import log_pipeline_step
from upstream_home_test.utils.timing import elapsed_ms_since


class ParquetWriteError(Exception):
    """Custom exception for Parquet writing errors."""



class GenericParquetWriter:
    """Generic Parquet writer with partitioning and compression support."""

    def __init__(
        self,
        output_dir: str = "data/output",
        max_file_size_mb: float = 125.0,
        compression: Literal["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"] = "zstd",
        partitioning_enabled: bool = True,
        partition_columns: Optional[list[str]] = None,
        logger: Optional[Any] = None,
        validator_model: Optional[type[BaseModel]] = None,
    ):
        """Initialize the Generic Parquet writer.

        Args:
            output_dir: Base output directory for parquet files
            max_file_size_mb: Maximum file size in MB before splitting
            compression: Compression algorithm for parquet files
            partitioning_enabled: Whether to enable partitioning by date/hour
            partition_columns: Custom partition columns (overrides date/hour partitioning)
            logger: Logger instance for structured logging
            validator_model: Optional Pydantic model used to validate list[dict] inputs
        """
        self.output_dir = output_dir
        self.max_file_size_mb = max_file_size_mb
        self.compression = compression
        self.partitioning_enabled = partitioning_enabled
        self.partition_columns = partition_columns or ["date", "hour"]
        self.logger = logger
        self.validator_model = validator_model

        # Statistics tracking
        self.files_written = 0
        self.total_rows = 0
        self.partitions_created = 0

    def validate_messages(self, messages: list[dict[str, Any]]) -> list[BaseModel]:
        """Validate messages against provided schema model.

        Args:
            messages: List of raw message dictionaries

        Returns:
            List of validated model objects

        Raises:
            ParquetWriteError: If validation fails
        """
        if self.validator_model is None:
            raise ParquetWriteError(
                "validator_model must be provided when writing list[dict] inputs"
            )

        validated_messages = []
        validation_errors = []

        for i, message in enumerate(messages):
            try:
                validated_msg = self.validator_model.model_validate(message)
                validated_messages.append(validated_msg)
            except ValidationError as e:
                validation_errors.append(f"Message {i}: {e}")

        if validation_errors:
            error_msg = (
                f"Validation failed for {len(validation_errors)} messages:\n"
                + "\n".join(validation_errors[:5])
            )
            if len(validation_errors) > 5:
                error_msg += f"\n... and {len(validation_errors) - 5} more errors"
            raise ParquetWriteError(error_msg)

        return validated_messages

    def _prepare_dataframe(
        self, data: list[dict[str, Any]] | pl.DataFrame
    ) -> pl.DataFrame:
        """Prepare data as a Polars DataFrame.

        Args:
            data: List of dictionaries or Polars DataFrame

        Returns:
            Prepared Polars DataFrame

        Raises:
            ValueError: If data is empty
        """
        if isinstance(data, pl.DataFrame):
            if data.is_empty():
                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event="No data to write",
                    metrics={"rows": 0, "files_written": 0},
                )
                raise ValueError("No data to write")
            return data
        else:
            if not data:
                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event="No messages to write",
                    metrics={"messages": 0, "files_written": 0},
                )
                raise ValueError("No messages to write")

            # Validate messages if it's a list of dicts
            validated_messages = self.validate_messages(data)
            message_dicts = [msg.model_dump() for msg in validated_messages]
            return pl.DataFrame(message_dicts)

    def _add_partitioning_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add partitioning columns to DataFrame if partitioning is enabled.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with partitioning columns added
        """
        if self.partitioning_enabled and "timestamp" in df.columns:
            return df.with_columns(
                [
                    pl.col("timestamp").dt.date().alias("date"),
                    pl.col("timestamp").dt.hour().alias("hour"),
                ]
            )
        return df

    def _write_partitioned_data(self, df: pl.DataFrame) -> int:
        """Write data with partitioning enabled.

        Args:
            df: DataFrame to write

        Returns:
            Number of unique partitions created
        """
        # Group by partition columns
        partitions = df.group_by(self.partition_columns, maintain_order=True)

        # Write each partition
        for group_key, partition_df in partitions:
            self._write_partition(group_key, partition_df)

        # Count unique partitions
        return df.select(self.partition_columns).unique().height

    def _write_unpartitioned_data(self, df: pl.DataFrame) -> int:
        """Write data without partitioning.

        Args:
            df: DataFrame to write

        Returns:
            Always returns 1 for single file
        """
        self._write_single_file(df)
        return 1

    def _log_write_success(self, unique_partitions: int, start_time: float) -> None:
        """Log successful write operation.

        Args:
            unique_partitions: Number of partitions created
            start_time: Start time of write operation
        """
        duration_ms = elapsed_ms_since(start_time)

        log_pipeline_step(
            logger=self.logger,
            step="parquet_write",
            event=f"Successfully wrote {self.total_rows} rows to {self.files_written} files",
            metrics={
                "total_rows": self.total_rows,
                "files_written": self.files_written,
                "partitions": unique_partitions,
                "duration_ms": round(duration_ms, 2),
                "output_dir": self.output_dir,
                "partitioning_enabled": self.partitioning_enabled,
            },
        )

    def _log_write_error(
        self, error: Exception, df: pl.DataFrame, start_time: float
    ) -> None:
        """Log write operation error.

        Args:
            error: Exception that occurred
            df: DataFrame that failed to write
            start_time: Start time of write operation
        """
        duration_ms = elapsed_ms_since(start_time)
        error_msg = f"Failed to write Parquet files: {error!s}"

        log_pipeline_step(
            logger=self.logger,
            step="parquet_write",
            event=error_msg,
            metrics={
                "error": str(error),
                "duration_ms": round(duration_ms, 2),
                "rows_attempted": len(df),
            },
            level="ERROR",
        )

    def write(self, data: list[dict[str, Any]] | pl.DataFrame) -> dict[str, Any]:
        """Write data to Parquet files with optional partitioning.

        Args:
            data: List of message dictionaries or Polars DataFrame

        Returns:
            Dictionary with write statistics

        Raises:
            ParquetWriteError: If writing fails
        """
        start_time = time.time()

        try:
            # Prepare DataFrame
            df = self._prepare_dataframe(data)

            # Reset statistics
            self.files_written = 0
            self.total_rows = 0
            self.partitions_created = 0

            # Add partitioning columns if needed
            df = self._add_partitioning_columns(df)

            # Write data with or without partitioning
            if self.partitioning_enabled and "timestamp" in df.columns:
                unique_partitions = self._write_partitioned_data(df)
            else:
                unique_partitions = self._write_unpartitioned_data(df)

            # Log success
            self._log_write_success(unique_partitions, start_time)

            return {
                "rows": self.total_rows,
                "files_written": self.files_written,
                "partitions": unique_partitions,
                "duration_ms": round(elapsed_ms_since(start_time), 2),
            }

        except ValueError:
            # Handle empty data cases
            return {"rows": 0, "files_written": 0, "partitions": 0, "duration_ms": 0}

        except Exception as e:
            # Log error and re-raise as ParquetWriteError
            self._log_write_error(e, df, start_time)
            raise ParquetWriteError(f"Failed to write Parquet files: {e!s}") from e

    def _write_single_file(self, df: pl.DataFrame) -> None:
        """Write DataFrame as a single parquet file.

        Args:
            df: Polars DataFrame to write
        """
        # Create output directory
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Estimate size and split if needed
        estimated_size_mb = len(df) * 0.001  # Rough estimate: 1KB per row

        if estimated_size_mb <= self.max_file_size_mb:
            # Single file
            filename = "data.parquet"
            file_path = Path(self.output_dir) / filename

            df.write_parquet(file_path, compression=self.compression, use_pyarrow=True)

            self.files_written += 1
            self.total_rows += len(df)

            log_pipeline_step(
                logger=self.logger,
                step="parquet_write",
                event="Wrote single file",
                metrics={"rows": len(df), "file_path": str(file_path)},
            )
        else:
            # Split into multiple files
            chunk_size = int(len(df) * self.max_file_size_mb / estimated_size_mb)
            chunks = [df[i : i + chunk_size] for i in range(0, len(df), chunk_size)]

            for chunk_idx, chunk_df in enumerate(chunks):
                filename = f"part_{chunk_idx:03d}.parquet"
                file_path = Path(self.output_dir) / filename

                chunk_df.write_parquet(
                    file_path, compression=self.compression, use_pyarrow=True
                )

                self.files_written += 1
                self.total_rows += len(chunk_df)

                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event=f"Wrote chunk {chunk_idx + 1}/{len(chunks)}",
                    metrics={
                        "chunk": chunk_idx + 1,
                        "total_chunks": len(chunks),
                        "rows": len(chunk_df),
                        "file_path": str(file_path),
                    },
                )

    def _write_partition(self, group_key: tuple, partition_df: pl.DataFrame) -> None:
        """Write a single partition to parquet files.

        Args:
            group_key: Tuple of partition values (e.g., date, hour)
            partition_df: Polars DataFrame containing the partition data
        """
        # Create partition path based on partition columns
        partition_parts = []
        for i, col in enumerate(self.partition_columns):
            value = group_key[i]
            if hasattr(value, "strftime"):  # Date object
                partition_parts.append(f"{col}={value.strftime('%Y-%m-%d')}")
            else:
                partition_parts.append(f"{col}={str(value).zfill(2)}")

        partition_path = Path(self.output_dir) / "/".join(partition_parts)
        partition_path.mkdir(parents=True, exist_ok=True)

        # Remove the partitioning columns before writing
        write_df = partition_df.drop(self.partition_columns)

        # Estimate size and split if needed
        estimated_size_mb = len(write_df) * 0.001  # Rough estimate: 1KB per row

        if estimated_size_mb <= self.max_file_size_mb:
            # Single file
            filename = "data.parquet"
            file_path = partition_path / filename

            write_df.write_parquet(
                file_path, compression=self.compression, use_pyarrow=True
            )

            self.files_written += 1
            self.total_rows += len(write_df)

            partition_name = "_".join(
                [str(group_key[i]) for i in range(len(self.partition_columns))]
            )
            log_pipeline_step(
                logger=self.logger,
                step="parquet_write",
                event=f"Wrote partition {partition_name}",
                metrics={
                    "partition": partition_name,
                    "rows": len(write_df),
                    "file_path": str(file_path),
                },
            )
        else:
            # Split into multiple files
            chunk_size = int(len(write_df) * self.max_file_size_mb / estimated_size_mb)
            chunks = [
                write_df[i : i + chunk_size]
                for i in range(0, len(write_df), chunk_size)
            ]

            for chunk_idx, chunk_df in enumerate(chunks):
                filename = f"part_{chunk_idx:03d}.parquet"
                file_path = partition_path / filename

                chunk_df.write_parquet(
                    file_path, compression=self.compression, use_pyarrow=True
                )

                self.files_written += 1
                self.total_rows += len(chunk_df)

                partition_name = "_".join(
                    [str(group_key[i]) for i in range(len(self.partition_columns))]
                )
                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event=f"Wrote partition {partition_name} chunk {chunk_idx + 1}/{len(chunks)}",
                    metrics={
                        "partition": partition_name,
                        "chunk": chunk_idx + 1,
                        "total_chunks": len(chunks),
                        "rows": len(chunk_df),
                        "file_path": str(file_path),
                    },
                )


def write_parquet(
    data: list[dict[str, Any]] | pl.DataFrame,
    *,
    output_dir: Optional[str] = None,
    output_path: Optional[str] = None,
    partitioning_enabled: Optional[bool] = None,
    partition_columns: Optional[list[str]] = None,
    validator_model: Optional[type[BaseModel]] = None,
    logger: Optional[Any] = None,
) -> dict[str, Any]:
    """Unified Parquet writer for all layers.

    Supports both partitioned directory outputs (Bronze/Silver partitioned) and
    single-file outputs (typical for non-partitioned Gold).

    Args:
        data: List of dictionaries (validated via validator_model) or a Polars DataFrame
        output_dir: Base output directory for partitioned writes
        output_path: Full file path for single-file writes
        partitioning_enabled: Whether to enable partitioning; inferred if not provided
        partition_columns: Custom partition columns when partitioning is enabled
        validator_model: Pydantic model for validating list[dict] inputs
        logger: Logger instance for structured logging

    Returns:
        Write statistics dictionary.

    Raises:
        ParquetWriteError: If validation or write fails, or when configuration is invalid
    """
    # Determine mode
    if output_dir is None and output_path is None:
        raise ParquetWriteError(
            "Either output_dir (partitioned) or output_path (single file) must be provided"
        )

    if output_path is not None and output_dir is not None:
        raise ParquetWriteError(
            "Provide only one of output_dir or output_path, not both"
        )

    if output_path is not None:
        # Single-file mode
        output_path_obj = Path(output_path)
        resolved_output_dir = str(output_path_obj.parent)
        filename = output_path_obj.name

        writer = GenericParquetWriter(
            output_dir=resolved_output_dir,
            partitioning_enabled=False
            if partitioning_enabled is None
            else partitioning_enabled,
            partition_columns=partition_columns,
            logger=logger,
            validator_model=validator_model,
        )

        result = writer.write(data)

        # Rename if a specific filename was requested
        if filename != "data.parquet":
            current_file = Path(resolved_output_dir) / "data.parquet"
            target_file = Path(resolved_output_dir) / filename
            if current_file.exists():
                current_file.rename(target_file)
        return result

    # Partitioned directory mode
    writer = GenericParquetWriter(
        output_dir=output_dir,  # type: ignore[arg-type]
        partitioning_enabled=True
        if partitioning_enabled is None
        else partitioning_enabled,
        partition_columns=partition_columns,
        logger=logger,
        validator_model=validator_model,
    )
    return writer.write(data)
