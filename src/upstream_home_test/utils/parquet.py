from typing import Any

from upstream_home_test.utils.logging_config import log_pipeline_step


def estimate_parquet_size_mb(messages: list[dict[str, Any]]) -> float:
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
    messages: list[dict[str, Any]], max_size_mb: float = 125.0
) -> list[list[dict[str, Any]]]:
    """Split messages into chunks based on estimated Parquet size.

    Args:
        messages: List of message dictionaries
        max_size_mb: Maximum size per chunk in MB

    Returns:
        List of message chunks, each under the size limit
    """
    if not messages:
        return []

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
                "chunks": 1,
            },
        )
        return [messages]

    # Calculate number of chunks needed
    num_chunks = int(total_size_mb / max_size_mb) + 1
    chunk_size = len(messages) // num_chunks

    # Split messages into chunks
    chunks = []
    for i in range(0, len(messages), chunk_size):
        chunk = messages[i : i + chunk_size]
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
            "chunk_sizes_mb": [round(size, 2) for size in chunk_sizes],
        },
    )

    return chunks
