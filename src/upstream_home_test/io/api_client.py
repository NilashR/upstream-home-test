"""API client for fetching vehicle messages from the upstream endpoint."""

import time
from typing import Any

import httpx
from httpx import HTTPError, TimeoutException

from upstream_home_test.utils.logging_config import log_pipeline_step
from upstream_home_test.utils.timing import elapsed_ms_since


class APIError(Exception):
    """Custom exception for API-related errors."""



class APIClient:
    """Client for fetching vehicle messages from the API endpoint."""

    def __init__(self, base_url: str = "http://localhost:9900", timeout: float = 30.0):
        """Initialize API client.

        Args:
            base_url: Base URL for the API
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = 3
        self.retry_delay = 1.0  # seconds

    def fetch_vehicle_messages(self, amount: int = 10000) -> list[dict[str, Any]]:
        """Fetch vehicle messages from the API endpoint.

        Args:
            amount: Number of messages to fetch

        Returns:
            List of vehicle message dictionaries

        Raises:
            APIError: If API request fails after retries
        """
        url = f"{self.base_url}/upstream/vehicle_messages"
        params = {"amount": amount}

        for attempt in range(self.max_retries + 1):
            try:
                start_time = time.time()

                with httpx.Client(timeout=self.timeout) as client:
                    response = client.get(url, params=params)
                    response.raise_for_status()

                duration_ms = elapsed_ms_since(start_time)

                # Parse JSON response
                data = response.json()

                # Ensure we have a list of messages
                if not isinstance(data, list):
                    raise APIError(f"Expected list response, got {type(data)}")

                # Log successful fetch
                log_pipeline_step(
                    logger=None,  # Will be set by caller
                    step="api_fetch",
                    event=f"Successfully fetched {len(data)} vehicle messages",
                    metrics={
                        "url": url,
                        "amount_requested": amount,
                        "amount_received": len(data),
                        "duration_ms": round(duration_ms, 2),
                        "attempt": attempt + 1,
                    },
                )

                return data

            except TimeoutException as e:
                error_msg = (
                    f"API request timeout after {self.timeout}s (attempt {attempt + 1})"
                )
                if attempt < self.max_retries:
                    log_pipeline_step(
                        logger=None,
                        step="api_fetch",
                        event=f"Timeout, retrying in {self.retry_delay}s",
                        metrics={"error": str(e), "attempt": attempt + 1},
                        level="WARNING",
                    )
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise APIError(error_msg) from e

            except HTTPError as e:
                error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
                if attempt < self.max_retries and e.response.status_code >= 500:
                    # Retry on server errors
                    log_pipeline_step(
                        logger=None,
                        step="api_fetch",
                        event=f"Server error, retrying in {self.retry_delay}s",
                        metrics={"error": error_msg, "attempt": attempt + 1},
                        level="WARNING",
                    )
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise APIError(error_msg) from e

            except Exception as e:
                error_msg = f"Unexpected error: {e!s}"
                if attempt < self.max_retries:
                    log_pipeline_step(
                        logger=None,
                        step="api_fetch",
                        event=f"Unexpected error, retrying in {self.retry_delay}s",
                        metrics={"error": error_msg, "attempt": attempt + 1},
                        level="WARNING",
                    )
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise APIError(error_msg) from e

        # This should never be reached, but just in case
        raise APIError("Max retries exceeded")


def fetch_vehicle_messages(amount: int = 10000) -> list[dict[str, Any]]:
    """Convenience function to fetch vehicle messages.

    Args:
        amount: Number of messages to fetch

    Returns:
        List of vehicle message dictionaries

    Raises:
        APIError: If API request fails
    """
    client = APIClient()
    return client.fetch_vehicle_messages(amount)
