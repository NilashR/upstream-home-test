"""timing helper."""

import time


def elapsed_ms_since(start_time: float) -> float:
    """Milliseconds elapsed since the given start_time (time.time())."""
    return (time.time() - start_time) * 1000
