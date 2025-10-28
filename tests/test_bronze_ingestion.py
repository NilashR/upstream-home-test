"""Tests for Bronze layer ingestion pipeline."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from upstream_home_test.io.api_client import APIError, APIClient
from upstream_home_test.pipelines.bronze_ingestion import run_bronze_ingestion
from upstream_home_test.schemas.bronze import VehicleMessageRaw
from upstream_home_test.utils.parquet import split_by_size


class TestAPIClient:
    """Test API client functionality."""
    
    def test_fetch_vehicle_messages_success(self):
        """Test successful API fetch."""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "vin": "1HGBH41JXMN109186",
                "manufacturer": "Honda ",
                "gear": "D",
                "timestamp": "2025-01-27T10:30:00Z",
                "speed": 65.5
            }
        ]
        mock_response.raise_for_status.return_value = None
        
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            
            client = APIClient()
            result = client.fetch_vehicle_messages(1)
            
            assert len(result) == 1
            assert result[0]["vin"] == "1HGBH41JXMN109186"
    
    def test_fetch_vehicle_messages_http_error(self):
        """Test API fetch with HTTP error."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            mock_client.return_value.__enter__.return_value.get.side_effect = Exception("HTTP 500")
            
            client = APIClient()
            
            with pytest.raises(APIError):
                client.fetch_vehicle_messages(1)


class TestBronzeSchema:
    """Test Bronze layer schema validation."""
    
    def test_valid_message(self):
        """Test valid message validation."""
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": "Honda",
            "gearPosition": "D",
            "timestamp": "2025-01-27T10:30:00Z",
            "velocity": 65
        }
        
        validated = VehicleMessageRaw.model_validate(message)
        assert validated.vin == "1HGBH41JXMN109186"
        assert validated.manufacturer == "Honda"
        assert validated.gearPosition == "D"
        assert validated.velocity == 65
    
    def test_timezone_aware_timestamp(self):
        """Test timestamp timezone handling."""
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": "Honda",
            "gearPosition": "D",
            "timestamp": "2025-01-27T10:30:00"  # Naive datetime
        }
        
        validated = VehicleMessageRaw.model_validate(message)
        assert validated.timestamp.tzinfo is not None
    
    def test_invalid_vin(self):
        """Test invalid VIN validation."""
        message = {
            "vin": "INVALID",  # Too short
            "manufacturer": "Honda",
            "gearPosition": "D",
            "timestamp": "2025-01-27T10:30:00Z"
        }
        
        with pytest.raises(ValueError, match="VIN must be 17 characters"):
            VehicleMessageRaw.model_validate(message)


class TestPartitioning:
    """Test partitioning logic."""
    
    def test_split_by_size(self):
        """Test size-based splitting."""
        # Create a large message list
        messages = [
            {
                "vin": f"1HGBH41JXMN{i:06d}",
                "manufacturer": "Honda",
                "gear": "D",
                "timestamp": "2025-01-27T10:30:00Z",
                "speed": 65.5,
                "rpm": 2500,
                "fuel_level": 0.75,
                "engine_temp": 190.0
            }
            for i in range(1000)  # Large number of messages
        ]
        
        chunks = split_by_size(messages, max_size_mb=0.001)  # Very small size limit
        
        assert len(chunks) > 1
        assert all(len(chunk) > 0 for chunk in chunks)
        assert sum(len(chunk) for chunk in chunks) == len(messages)


class TestBronzeIngestion:
    """Test Bronze ingestion pipeline."""
    
    @patch("upstream_home_test.pipelines.bronze_ingestion.fetch_vehicle_messages")
    @patch("upstream_home_test.pipelines.bronze_ingestion.write_parquet")
    def test_run_bronze_ingestion_success(self, mock_write, mock_fetch):
        """Test successful Bronze ingestion."""
        # Mock API response
        mock_messages = [
            {
                "vin": "1HGBH41JXMN109186",
                "manufacturer": "Honda",
                "gear": "D",
                "timestamp": "2025-01-27T10:30:00Z"
            }
        ]
        mock_fetch.return_value = mock_messages
        
        # Mock write response
        mock_write.return_value = {
            "messages": 1,
            "files_written": 1,
            "partitions": 1
        }
        
        result = run_bronze_ingestion(amount=1)
        
        assert result["status"] == "completed"
        assert result["messages_fetched"] == 1
        assert result["files_written"] == 1
        assert result["partitions"] == 1
        
        mock_fetch.assert_called_once_with(1)
        mock_write.assert_called_once()
    
    @patch("upstream_home_test.pipelines.bronze_ingestion.fetch_vehicle_messages")
    def test_run_bronze_ingestion_api_error(self, mock_fetch):
        """Test Bronze ingestion with API error."""
        mock_fetch.side_effect = APIError("API unavailable")
        
        with pytest.raises(APIError):
            run_bronze_ingestion(amount=1)
    
    @patch("upstream_home_test.pipelines.bronze_ingestion.fetch_vehicle_messages")
    def test_run_bronze_ingestion_empty_response(self, mock_fetch):
        """Test Bronze ingestion with empty API response."""
        mock_fetch.return_value = []
        
        result = run_bronze_ingestion(amount=1)
        
        assert result["status"] == "completed"
        assert result["messages_fetched"] == 0
        assert result["files_written"] == 0
