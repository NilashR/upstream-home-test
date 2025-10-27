"""Parquet writer for Bronze layer with partitioning and compression."""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
from pydantic import ValidationError

from upstream_home_test.schemas.bronze import VehicleMessageRaw
from upstream_home_test.utils.logging_config import log_pipeline_step
from upstream_home_test.utils.partitioning import create_partition_filename, create_partition_path


class ParquetWriteError(Exception):
    """Custom exception for Parquet writing errors."""
    pass


class GenericParquetWriter:
    """Generic Parquet writer with partitioning and compression support."""
    
    def __init__(
        self, 
        output_dir: str = "data/output",
        max_file_size_mb: float = 125.0,
        compression: str = "zstd",
        partitioning_enabled: bool = True,
        partition_columns: Optional[List[str]] = None,
        logger: Optional[Any] = None
    ):
        """Initialize the Generic Parquet writer.
        
        Args:
            output_dir: Base output directory for parquet files
            max_file_size_mb: Maximum file size in MB before splitting
            compression: Compression algorithm for parquet files
            partitioning_enabled: Whether to enable partitioning by date/hour
            partition_columns: Custom partition columns (overrides date/hour partitioning)
            logger: Logger instance for structured logging
        """
        self.output_dir = output_dir
        self.max_file_size_mb = max_file_size_mb
        self.compression = compression
        self.partitioning_enabled = partitioning_enabled
        self.partition_columns = partition_columns or ["date", "hour"]
        self.logger = logger
        
        # Statistics tracking
        self.files_written = 0
        self.total_rows = 0
        self.partitions_created = 0

    def validate_messages(self, messages: List[Dict[str, Any]]) -> List[VehicleMessageRaw]:
        """Validate messages against Bronze schema.
        
        Args:
            messages: List of raw message dictionaries
            
        Returns:
            List of validated VehicleMessageRaw objects
            
        Raises:
            ParquetWriteError: If validation fails
        """
        validated_messages = []
        validation_errors = []
        
        for i, message in enumerate(messages):
            try:
                validated_msg = VehicleMessageRaw.model_validate(message)
                validated_messages.append(validated_msg)
            except ValidationError as e:
                validation_errors.append(f"Message {i}: {e}")
        
        if validation_errors:
            error_msg = f"Validation failed for {len(validation_errors)} messages:\n" + "\n".join(validation_errors[:5])
            if len(validation_errors) > 5:
                error_msg += f"\n... and {len(validation_errors) - 5} more errors"
            raise ParquetWriteError(error_msg)
        
        return validated_messages

    def write(self, data: List[Dict[str, Any]] | pl.DataFrame) -> Dict[str, Any]:
        """Write data to Parquet files with optional partitioning.
        
        Args:
            data: List of message dictionaries or Polars DataFrame
            
        Returns:
            Dictionary with write statistics
            
        Raises:
            ParquetWriteError: If writing fails
        """
        if isinstance(data, pl.DataFrame):
            if data.is_empty():
                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event="No data to write",
                    metrics={"rows": 0, "files_written": 0}
                )
                return {"rows": 0, "files_written": 0, "partitions": 0}
            df = data
        else:
            if not data:
                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event="No messages to write",
                    metrics={"messages": 0, "files_written": 0}
                )
                return {"messages": 0, "files_written": 0, "partitions": 0}
            
            # Validate messages if it's a list of dicts
            validated_messages = self.validate_messages(data)
            message_dicts = [msg.model_dump() for msg in validated_messages]
            df = pl.DataFrame(message_dicts)
        
        start_time = time.time()
        
        try:
            # Reset statistics
            self.files_written = 0
            self.total_rows = 0
            self.partitions_created = 0
            
            if self.partitioning_enabled and "timestamp" in df.columns:
                # Add partitioning columns if timestamp exists
                df = df.with_columns([
                    pl.col("timestamp").dt.date().alias("date"),
                    pl.col("timestamp").dt.hour().alias("hour")
                ])
                
                # Group by partition columns
                partitions = df.group_by(self.partition_columns, maintain_order=True)
                
                # Write each partition
                for group_key, partition_df in partitions:
                    self._write_partition(group_key, partition_df)
                
                # Count unique partitions
                unique_partitions = df.select(self.partition_columns).unique().height
            else:
                # Write as single file without partitioning
                self._write_single_file(df)
                unique_partitions = 1
            
            duration_ms = (time.time() - start_time) * 1000
            
            # Log summary
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
                    "partitioning_enabled": self.partitioning_enabled
                }
            )
            
            return {
                "rows": self.total_rows,
                "files_written": self.files_written,
                "partitions": unique_partitions,
                "duration_ms": round(duration_ms, 2)
            }
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = f"Failed to write Parquet files: {str(e)}"
            
            log_pipeline_step(
                logger=self.logger,
                step="parquet_write",
                event=error_msg,
                metrics={
                    "error": str(e),
                    "duration_ms": round(duration_ms, 2),
                    "rows_attempted": len(df)
                },
                level="ERROR"
            )
            
            raise ParquetWriteError(error_msg) from e

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
            
            df.write_parquet(
                file_path,
                compression=self.compression,
                use_pyarrow=True
            )
            
            self.files_written += 1
            self.total_rows += len(df)
            
            log_pipeline_step(
                logger=self.logger,
                step="parquet_write",
                event=f"Wrote single file",
                metrics={
                    "rows": len(df),
                    "file_path": str(file_path)
                }
            )
        else:
            # Split into multiple files
            chunk_size = int(len(df) * self.max_file_size_mb / estimated_size_mb)
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            
            for chunk_idx, chunk_df in enumerate(chunks):
                filename = f"part_{chunk_idx:03d}.parquet"
                file_path = Path(self.output_dir) / filename
                
                chunk_df.write_parquet(
                    file_path,
                    compression=self.compression,
                    use_pyarrow=True
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
                        "file_path": str(file_path)
                    }
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
            if hasattr(value, 'strftime'):  # Date object
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
                file_path,
                compression=self.compression,
                use_pyarrow=True
            )
            
            self.files_written += 1
            self.total_rows += len(write_df)
            
            partition_name = "_".join([str(group_key[i]) for i in range(len(self.partition_columns))])
            log_pipeline_step(
                logger=self.logger,
                step="parquet_write",
                event=f"Wrote partition {partition_name}",
                metrics={
                    "partition": partition_name,
                    "rows": len(write_df),
                    "file_path": str(file_path)
                }
            )
        else:
            # Split into multiple files
            chunk_size = int(len(write_df) * self.max_file_size_mb / estimated_size_mb)
            chunks = [write_df[i:i + chunk_size] for i in range(0, len(write_df), chunk_size)]
            
            for chunk_idx, chunk_df in enumerate(chunks):
                filename = f"part_{chunk_idx:03d}.parquet"
                file_path = partition_path / filename
                
                chunk_df.write_parquet(
                    file_path,
                    compression=self.compression,
                    use_pyarrow=True
                )
                
                self.files_written += 1
                self.total_rows += len(chunk_df)
                
                partition_name = "_".join([str(group_key[i]) for i in range(len(self.partition_columns))])
                log_pipeline_step(
                    logger=self.logger,
                    step="parquet_write",
                    event=f"Wrote partition {partition_name} chunk {chunk_idx + 1}/{len(chunks)}",
                    metrics={
                        "partition": partition_name,
                        "chunk": chunk_idx + 1,
                        "total_chunks": len(chunks),
                        "rows": len(chunk_df),
                        "file_path": str(file_path)
                    }
                )


def write_bronze_parquet(
    messages: List[Dict[str, Any]], 
    output_dir: str = "data/bronze",
    logger = None
) -> Dict[str, Any]:
    """Write messages to Bronze layer Parquet files with partitioning.
    
    This is a convenience function that creates a GenericParquetWriter instance
    configured for Bronze layer with partitioning enabled.
    
    Args:
        messages: List of message dictionaries
        output_dir: Base output directory for Bronze layer
        logger: Logger instance for structured logging
        
    Returns:
        Dictionary with write statistics
        
    Raises:
        ParquetWriteError: If writing fails
    """
    writer = GenericParquetWriter(
        output_dir=output_dir,
        partitioning_enabled=True,
        logger=logger
    )
    return writer.write(messages)


def write_silver_parquet(
    df: pl.DataFrame,
    output_path: str = "data/silver/vehicle_messages_cleaned.parquet",
    logger = None
) -> Dict[str, Any]:
    """Write cleaned DataFrame to Silver layer Parquet file.
    
    This is a convenience function that creates a GenericParquetWriter instance
    configured for Silver layer without partitioning.
    
    Args:
        df: Polars DataFrame with cleaned data
        output_path: Output file path
        logger: Logger instance for structured logging
        
    Returns:
        Dictionary with write statistics
        
    Raises:
        ParquetWriteError: If writing fails
    """
    # Extract directory and filename from output_path
    output_path_obj = Path(output_path)
    output_dir = str(output_path_obj.parent)
    filename = output_path_obj.name
    
    writer = GenericParquetWriter(
        output_dir=output_dir,
        partitioning_enabled=False,
        logger=logger
    )
    
    # Write the DataFrame
    result = writer.write(df)
    
    # Rename the output file if it's not already named correctly
    if filename != "data.parquet":
        current_file = Path(output_dir) / "data.parquet"
        target_file = Path(output_dir) / filename
        if current_file.exists():
            current_file.rename(target_file)
    
    return result


