"""Tests for Silver layer transformation pipeline."""

import polars as pl
import pytest
from datetime import datetime
from pydantic import ValidationError
from unittest.mock import patch

from upstream_home_test.pipelines.silver_transform import run_silver_transform
from upstream_home_test.schemas.silver import VehicleMessageCleaned, map_gear_position


class TestGearPositionMapping:
    """Test gear position mapping functionality."""
    
    def test_map_gear_position_valid(self):
        """Test valid gear position mappings."""
        assert map_gear_position("P") == 0
        assert map_gear_position("R") == 1
        assert map_gear_position("N") == 2
        assert map_gear_position("D") == 3
        assert map_gear_position("L") == 4
    
    def test_map_gear_position_case_insensitive(self):
        """Test gear position mapping is case insensitive."""
        assert map_gear_position("p") == 0
        assert map_gear_position("d") == 3
        assert map_gear_position("l") == 4
    
    def test_map_gear_position_whitespace(self):
        """Test gear position mapping handles whitespace."""
        assert map_gear_position(" P ") == 0
        assert map_gear_position("  D  ") == 3
    
    def test_map_gear_position_invalid(self):
        """Test invalid gear position mappings."""
        assert map_gear_position("X") == -1
        assert map_gear_position("INVALID") == -1
        assert map_gear_position("") == -1
        assert map_gear_position(None) is None


class TestSilverSchema:
    """Test Silver layer schema validation."""
    
    def test_valid_cleaned_message(self):
        """Test valid cleaned message validation."""
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": "Honda",
            "gear_position": 3,
            "timestamp": "2025-01-27T10:30:00Z"
        }
        
        validated = VehicleMessageCleaned.model_validate(message)
        assert validated.vin == "1HGBH41JXMN109186"
        assert validated.manufacturer == "Honda"
        assert validated.gear_position == 3
    
    def test_gear_position_validation(self):
        """Test gear position validation."""
        # Valid gear positions
        for gear in [0, 1, 2, 3, 4]:
            message = {
                "vin": "1HGBH41JXMN109186",
                "manufacturer": "Honda",
                "gear_position": gear,
                "timestamp": "2025-01-27T10:30:00Z"
            }
            validated = VehicleMessageCleaned.model_validate(message)
            assert validated.gear_position == gear
        
        # Invalid gear position
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": "Honda",
            "gear_position": 7,  # Invalid
            "timestamp": "2025-01-27T10:30:00Z"
        }
        
        with pytest.raises(ValidationError, match="Gear position must be -1 to 6"):
            VehicleMessageCleaned.model_validate(message)
    
    def test_vin_validation(self):
        """Test VIN validation."""
        # Valid VIN
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": "Honda",
            "gear_position": 3,
            "timestamp": "2025-01-27T10:30:00Z"
        }
        validated = VehicleMessageCleaned.model_validate(message)
        assert validated.vin == "1HGBH41JXMN109186"
        
    
    def test_manufacturer_consistency(self):
        """Test manufacturer field validation."""
        # Valid manufacturer
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": "Honda",
            "gear_position": 3,
            "timestamp": "2025-01-27T10:30:00Z"
        }
        validated = VehicleMessageCleaned.model_validate(message)
        assert validated.manufacturer == "Honda"
        
        # Manufacturer with whitespace (should be stripped)
        message = {
            "vin": "1HGBH41JXMN109186",
            "manufacturer": " Honda ",
            "gear_position": 3,
            "timestamp": "2025-01-27T10:30:00Z"
        }
        validated = VehicleMessageCleaned.model_validate(message)
        assert validated.manufacturer == "Honda"


class TestSilverTransform:
    """Test Silver transformation pipeline."""
    
    # def test_validate_silver_sample_valid(self):
    #     """Test validation of valid Silver sample."""
    #     df = pl.DataFrame({
    #         "vin": ["1HGBH41JXMN109186", "1HGBH41JXMN109187"],
    #         "manufacturer_original": ["Honda ", "Toyota"],
    #         "manufacturer_cleaned": ["Honda", "Toyota"],
    #         "gear_position": [3, 0],
    #         "timestamp": ["2025-01-27T10:30:00Z", "2025-01-27T10:31:00Z"]
    #     })
    #     
    #     errors = validate_silver_sample(df)
    #     assert len(errors) == 0
    
    # def test_validate_silver_sample_invalid(self):
    #     """Test validation of invalid Silver sample."""
    #     df = pl.DataFrame({
    #         "vin": [None, "1HGBH41JXMN109187"],  # Null VIN
    #         "manufacturer_original": ["Honda ", "Toyota"],
    #         "manufacturer_cleaned": ["Honda", "Toyota"],
    #         "gear_position": [5, 0],  # Invalid gear position
    #         "timestamp": ["2025-01-27T10:30:00Z", "2025-01-27T10:31:00Z"]
    #     })
    #     
    #     errors = validate_silver_sample(df)
    #     assert len(errors) > 0
    #     assert any("null VIN" in error for error in errors)
    #     assert any("invalid gear positions" in error for error in errors)
    
    @patch("upstream_home_test.pipelines.silver_transform.pl.scan_parquet")
    @patch("upstream_home_test.pipelines.silver_transform.GenericParquetWriter")
    def test_run_silver_transform_success(self, mock_write, mock_scan):
        """Test successful Silver transformation."""
        # Mock Bronze data
        mock_df = pl.DataFrame({
            "vin": ["1HGBH41JXMN109186", "1HGBH41JXMN109187", None],  # One null VIN
            "manufacturer": ["Honda ", "Toyota", "Ford"],
            "gearPosition": ["D", "P", "R"],
            "frontLeftDoorState": ["closed", "open", "closed"],
            "wipersState": [True, False, True],
            "driverSeatbeltState": ["buckled", "unbuckled", "buckled"],
            "timestamp": [
                datetime(2025, 1, 27, 10, 30, 0),
                datetime(2025, 1, 27, 10, 31, 0),
                datetime(2025, 1, 27, 10, 32, 0)
            ]
        })
        mock_scan.return_value.collect.return_value = mock_df
        
        # Mock write response
        mock_writer_instance = mock_write.return_value
        mock_writer_instance.write.return_value = {
            "rows": 2,
            "output_path": "data/silver/vehicle_messages_cleaned.parquet",
            "duration_ms": 100.0
        }
        
        result = run_silver_transform()
        
        assert result["status"] == "completed"
        assert result["input_rows"] == 3
        assert result["filtered_rows"] == 1  # One null VIN filtered
        assert result["output_rows"] == 2
        
        mock_scan.assert_called_once()
        mock_write.assert_called_once()
        mock_writer_instance.write.assert_called_once()
    
    @patch("upstream_home_test.pipelines.silver_transform.pl.scan_parquet")
    def test_run_silver_transform_empty_bronze(self, mock_scan):
        """Test Silver transformation with empty Bronze data."""
        # Mock empty Bronze data
        mock_df = pl.DataFrame()
        mock_scan.return_value.collect.return_value = mock_df
        
        result = run_silver_transform()
        
        assert result["status"] == "completed"
        assert result["input_rows"] == 0
        assert result["output_rows"] == 0


class TestDataTransformation:
    """Test data transformation logic."""
    
    def test_manufacturer_cleaning(self):
        """Test manufacturer field cleaning."""
        df = pl.DataFrame({
            "manufacturer": ["Honda ", " Toyota", "Ford", "  BMW  "]
        })
        
        result = df.with_columns([
            pl.col("manufacturer").alias("manufacturer_original"),
            pl.col("manufacturer").str.strip_chars().alias("manufacturer_cleaned")
        ])
        
        assert result["manufacturer_original"].to_list() == ["Honda ", " Toyota", "Ford", "  BMW  "]
        assert result["manufacturer_cleaned"].to_list() == ["Honda", "Toyota", "Ford", "BMW"]
    
    def test_gear_position_mapping(self):
        """Test gear position mapping in DataFrame."""
        df = pl.DataFrame({
            "gear": ["P", "R", "N", "D", "L", "X", None]
        })
        
        result = df.with_columns([
            pl.col("gear").map_elements(
                map_gear_position,
                return_dtype=pl.Int64
            ).alias("gear_position")
        ])
        
        expected = [0, 1, 2, 3, 4, -1, None]
        assert result["gear_position"].to_list() == expected
    
    def test_vin_filtering(self):
        """Test VIN null filtering."""
        df = pl.DataFrame({
            "vin": ["1HGBH41JXMN109186", None, "1HGBH41JXMN109187", None, "1HGBH41JXMN109188"]
        })
        
        result = df.filter(pl.col("vin").is_not_null())
        
        assert len(result) == 3
        assert result["vin"].to_list() == ["1HGBH41JXMN109186", "1HGBH41JXMN109187", "1HGBH41JXMN109188"]
