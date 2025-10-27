"""Partitioning utilities for Bronze layer data organization."""

from datetime import datetime
from typing import Any, Dict, List, Tuple

from upstream_home_test.utils.logging_config import log_pipeline_step


def partition_by_datetime(messages: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """Partition messages by date and hour from timestamp field.
    
    Args:
        messages: List of message dictionaries with timestamp field
        
    Returns:
        Dictionary mapping (date, hour) tuples to lists of messages
        
    Raises:
        ValueError: If timestamp field is missing or invalid
    """
    partitions: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    
    for message in messages:
        # Extract timestamp
        timestamp_str = message.get("timestamp")
        if not timestamp_str:
            raise ValueError("Message missing timestamp field")
        
        # Parse timestamp
        try:
            if isinstance(timestamp_str, str):
                # Try parsing ISO format
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            elif isinstance(timestamp_str, datetime):
                dt = timestamp_str
            else:
                raise ValueError(f"Invalid timestamp type: {type(timestamp_str)}")
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid timestamp format: {timestamp_str}") from e
        
        # Ensure timezone-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
        
        # Convert to UTC
        dt_utc = dt.astimezone(datetime.now().astimezone().tzinfo.utc)
        
        # Create partition key
        date_str = dt_utc.strftime("%Y-%m-%d")
        hour_str = dt_utc.strftime("%H")
        partition_key = (date_str, hour_str)
        
        # Add to partition
        if partition_key not in partitions:
            partitions[partition_key] = []
        partitions[partition_key].append(message)
    
    # Log partitioning results
    log_pipeline_step(
        logger=None,  # Will be set by caller
        step="partitioning",
        event=f"Partitioned {len(messages)} messages into {len(partitions)} partitions",
        metrics={
            "total_messages": len(messages),
            "total_partitions": len(partitions),
            "partition_sizes": {f"{date}_{hour}": len(msgs) for (date, hour), msgs in partitions.items()}
        }
    )
    
    return partitions


def estimate_parquet_size_mb(messages: List[Dict[str, Any]]) -> float:
    """Estimate the size of messages when written as Parquet.
    
    This is a rough estimation based on JSON serialization size.
    Actual Parquet size will be smaller due to compression.
    
    Args:
        messages: List of message dictionaries
        
    Returns:
        Estimated size in MB
    """
    if not messages:
        return 0.0
    
    # Rough estimation: JSON size * 0.3 (typical Parquet compression ratio)
    import json
    json_size_bytes = len(json.dumps(messages, default=str))
    estimated_parquet_bytes = json_size_bytes * 0.3
    
    return estimated_parquet_bytes / (1024 * 1024)  # Convert to MB


def split_by_size(
    messages: List[Dict[str, Any]], 
    max_size_mb: float = 125.0
) -> List[List[Dict[str, Any]]]:
    """Split messages into chunks based on estimated Parquet size.
    
    Args:
        messages: List of message dictionaries
        max_size_mb: Maximum size per chunk in MB
        
    Returns:
        List of message chunks, each under the size limit
    """
    if not messages:
        return []
    
    # Estimate total size
    total_size_mb = estimate_parquet_size_mb(messages)
    
    if total_size_mb <= max_size_mb:
        # No splitting needed
        log_pipeline_step(
            logger=None,
            step="size_splitting",
            event=f"No splitting needed, estimated size {total_size_mb:.2f}MB <= {max_size_mb}MB",
            metrics={
                "total_messages": len(messages),
                "estimated_size_mb": round(total_size_mb, 2),
                "max_size_mb": max_size_mb,
                "chunks": 1
            }
        )
        return [messages]
    
    # Calculate number of chunks needed
    num_chunks = int(total_size_mb / max_size_mb) + 1
    chunk_size = len(messages) // num_chunks
    
    # Split messages into chunks
    chunks = []
    for i in range(0, len(messages), chunk_size):
        chunk = messages[i:i + chunk_size]
        if chunk:  # Only add non-empty chunks
            chunks.append(chunk)
    
    # Log splitting results
    chunk_sizes = [estimate_parquet_size_mb(chunk) for chunk in chunks]
    log_pipeline_step(
        logger=None,
        step="size_splitting",
        event=f"Split {len(messages)} messages into {len(chunks)} chunks",
        metrics={
            "total_messages": len(messages),
            "estimated_total_size_mb": round(total_size_mb, 2),
            "max_size_mb": max_size_mb,
            "chunks": len(chunks),
            "chunk_sizes_mb": [round(size, 2) for size in chunk_sizes]
        }
    )
    
    return chunks


def create_partition_path(date: str, hour: str, base_dir: str = "data/bronze") -> str:
    """Create partition directory path for Bronze layer.
    
    Args:
        date: Date string in YYYY-MM-DD format
        hour: Hour string in HH format
        base_dir: Base directory for Bronze layer
        
    Returns:
        Partition directory path
    """
    return f"{base_dir}/date={date}/hour={hour}"


def create_partition_filename(partition_index: int = 0, total_partitions: int = 1) -> str:
    """Create partition filename for Bronze layer.
    
    Args:
        partition_index: Index of this partition (0-based)
        total_partitions: Total number of partitions for this date/hour
        
    Returns:
        Partition filename
    """
    if total_partitions == 1:
        return "data.parquet"
    else:
        return f"part_{partition_index:03d}.parquet"
