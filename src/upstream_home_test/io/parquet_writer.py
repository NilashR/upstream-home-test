"""Parquet writer for Bronze layer with partitioning and compression."""

import time
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
from pydantic import ValidationError

from ..schemas.bronze import VehicleMessageRaw
from ..utils.logging_config import log_pipeline_step
from ..utils.partitioning import create_partition_filename, create_partition_path, split_by_size


class ParquetWriteError(Exception):
    """Custom exception for Parquet writing errors."""
    pass


def validate_messages(messages: List[Dict[str, Any]]) -> List[VehicleMessageRaw]:
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


def write_bronze_parquet(
    messages: List[Dict[str, Any]], 
    output_dir: str = "data/bronze"
) -> Dict[str, Any]:
    """Write messages to Bronze layer Parquet files with partitioning.
    
    Args:
        messages: List of message dictionaries
        output_dir: Base output directory for Bronze layer
        
    Returns:
        Dictionary with write statistics
        
    Raises:
        ParquetWriteError: If writing fails
    """
    if not messages:
        log_pipeline_step(
            logger=None,
            step="parquet_write",
            event="No messages to write",
            metrics={"messages": 0, "files_written": 0}
        )
        return {"messages": 0, "files_written": 0, "partitions": 0}
    
    start_time = time.time()
    
    try:
        # Validate messages
        validated_messages = validate_messages(messages)
        
        # Convert to list of dicts for Polars
        message_dicts = [msg.model_dump() for msg in validated_messages]
        
        # Create Polars DataFrame
        df = pl.DataFrame(message_dicts)
        
        # Partition by date and hour
        from ..utils.partitioning import partition_by_datetime
        partitions = partition_by_datetime(message_dicts)
        
        files_written = 0
        total_rows = 0
        
        # Write each partition
        for (date, hour), partition_messages in partitions.items():
            partition_path = create_partition_path(date, hour, output_dir)
            Path(partition_path).mkdir(parents=True, exist_ok=True)
            
            # Split partition if too large
            chunks = split_by_size(partition_messages, max_size_mb=125.0)
            
            # Write each chunk
            for chunk_idx, chunk in enumerate(chunks):
                filename = create_partition_filename(chunk_idx, len(chunks))
                file_path = Path(partition_path) / filename
                
                # Create DataFrame for this chunk
                chunk_df = pl.DataFrame(chunk)
                
                # Write Parquet with compression
                chunk_df.write_parquet(
                    file_path,
                    compression="zstd",
                    use_pyarrow=True
                )
                
                files_written += 1
                total_rows += len(chunk)
                
                log_pipeline_step(
                    logger=None,
                    step="parquet_write",
                    event=f"Wrote partition {date}/{hour} chunk {chunk_idx + 1}/{len(chunks)}",
                    metrics={
                        "partition": f"{date}_{hour}",
                        "chunk": chunk_idx + 1,
                        "total_chunks": len(chunks),
                        "rows": len(chunk),
                        "file_path": str(file_path)
                    }
                )
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Log summary
        log_pipeline_step(
            logger=None,
            step="parquet_write",
            event=f"Successfully wrote {total_rows} messages to {files_written} files",
            metrics={
                "total_messages": total_rows,
                "files_written": files_written,
                "partitions": len(partitions),
                "duration_ms": round(duration_ms, 2),
                "output_dir": output_dir
            }
        )
        
        return {
            "messages": total_rows,
            "files_written": files_written,
            "partitions": len(partitions),
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        error_msg = f"Failed to write Parquet files: {str(e)}"
        
        log_pipeline_step(
            logger=None,
            step="parquet_write",
            event=error_msg,
            metrics={
                "error": str(e),
                "duration_ms": round(duration_ms, 2),
                "messages_attempted": len(messages)
            },
            level="ERROR"
        )
        
        raise ParquetWriteError(error_msg) from e


def write_silver_parquet(
    df: pl.DataFrame,
    output_path: str = "data/silver/vehicle_messages_cleaned.parquet"
) -> Dict[str, Any]:
    """Write cleaned DataFrame to Silver layer Parquet file.
    
    Args:
        df: Polars DataFrame with cleaned data
        output_path: Output file path
        
    Returns:
        Dictionary with write statistics
        
    Raises:
        ParquetWriteError: If writing fails
    """
    start_time = time.time()
    
    try:
        # Create output directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Write Parquet with compression
        df.write_parquet(
            output_path,
            compression="zstd",
            use_pyarrow=True
        )
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Log success
        log_pipeline_step(
            logger=None,
            step="silver_write",
            event=f"Successfully wrote {len(df)} rows to Silver layer",
            metrics={
                "rows": len(df),
                "output_path": output_path,
                "duration_ms": round(duration_ms, 2)
            }
        )
        
        return {
            "rows": len(df),
            "output_path": output_path,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        error_msg = f"Failed to write Silver Parquet file: {str(e)}"
        
        log_pipeline_step(
            logger=None,
            step="silver_write",
            event=error_msg,
            metrics={
                "error": str(e),
                "duration_ms": round(duration_ms, 2),
                "rows_attempted": len(df)
            },
            level="ERROR"
        )
        
        raise ParquetWriteError(error_msg) from e
