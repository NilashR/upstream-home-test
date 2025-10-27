"""Pydantic schemas for Silver layer (cleaned and standardized data)."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class VehicleMessageCleaned(BaseModel):
    """Cleaned vehicle message schema for Silver layer.
    
    This schema represents the cleaned and standardized data after
    processing the Bronze layer raw data.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    # Core vehicle identification (cleaned)
    vin: str  # Non-null after filtering
    manufacturer_original: str | None  # Original manufacturer value
    manufacturer_cleaned: str | None  # Cleaned manufacturer (trailing spaces removed)
    
    # Vehicle state (standardized)
    gear_position: int | None  # Standardized gear position as integer
    
    # Timestamp (timezone-aware UTC)
    timestamp: datetime
    
    # Additional cleaned fields
    speed: float | None = None
    rpm: int | None = None
    fuel_level: float | None = None
    engine_temp: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude: float | None = None
    
    @field_validator("gear_position")
    @classmethod
    def validate_gear_position(cls, v: int | None) -> int | None:
        """Validate gear position is a valid integer.
        
        Args:
            v: Gear position integer or None
            
        Returns:
            Gear position integer or None
            
        Raises:
            ValueError: If gear position is invalid
        """
        if v is not None and v not in {0, 1, 2, 3, 4}:
            raise ValueError(f"Gear position must be 0-4, got {v}")
        return v
    
    @field_validator("vin")
    @classmethod
    def validate_vin_not_empty(cls, v: str) -> str:
        """Validate VIN is not empty.
        
        Args:
            v: VIN string
            
        Returns:
            VIN string
            
        Raises:
            ValueError: If VIN is empty
        """
        if not v or not v.strip():
            raise ValueError("VIN cannot be empty")
        return v.strip().upper()
    
    @field_validator("manufacturer_original")
    @classmethod
    def validate_manufacturer_original(cls, v: str | None) -> str | None:
        """Validate manufacturer_original field (preserve whitespace).
        
        Args:
            v: Manufacturer string or None
            
        Returns:
            Manufacturer string or None
        """
        if v is None:
            return v
        return v  # Keep original whitespace
    
    @field_validator("manufacturer_cleaned")
    @classmethod
    def validate_manufacturer_cleaned(cls, v: str | None) -> str | None:
        """Validate manufacturer_cleaned field (strip whitespace).
        
        Args:
            v: Manufacturer string or None
            
        Returns:
            Manufacturer string or None
        """
        if v is None:
            return v
        return v.strip() if v.strip() else None
    
    @field_validator("speed", "fuel_level", "engine_temp")
    @classmethod
    def validate_positive_float(cls, v: float | None) -> float | None:
        """Validate that float fields are non-negative.
        
        Args:
            v: Float value or None
            
        Returns:
            Float value or None
            
        Raises:
            ValueError: If value is negative
        """
        if v is not None and v < 0:
            raise ValueError(f"Value must be non-negative, got {v}")
        return v
    
    @field_validator("rpm")
    @classmethod
    def validate_positive_int(cls, v: int | None) -> int | None:
        """Validate that integer fields are non-negative.
        
        Args:
            v: Integer value or None
            
        Returns:
            Integer value or None
            
        Raises:
            ValueError: If value is negative
        """
        if v is not None and v < 0:
            raise ValueError(f"Value must be non-negative, got {v}")
        return v
    
    @model_validator(mode="after")
    def validate_manufacturer_consistency(self) -> "VehicleMessageCleaned":
        """Validate manufacturer field consistency.
        
        Returns:
            Self after validation
            
        Raises:
            ValueError: If manufacturer fields are inconsistent
        """
        # If both manufacturer fields are present, cleaned should be original stripped
        if (self.manufacturer_original is not None and 
            self.manufacturer_cleaned is not None):
            expected_cleaned = self.manufacturer_original.strip()
            if self.manufacturer_cleaned != expected_cleaned:
                raise ValueError(
                    f"manufacturer_cleaned should be manufacturer_original stripped. "
                    f"Expected '{expected_cleaned}', got '{self.manufacturer_cleaned}'"
                )
        
        return self


# Gear position mapping constants
GEAR_POSITION_MAPPING = {
    "P": 0,  # Park
    "R": 1,  # Reverse
    "N": 2,  # Neutral
    "D": 3,  # Drive
    "L": 4,  # Low
}


def map_gear_position(gear_str: str | None) -> int | None:
    """Map gear position string to integer.
    
    Args:
        gear_str: Gear position string (P, R, N, D, L) or None
        
    Returns:
        Mapped integer (0-4) or None if invalid/None
    """
    if gear_str is None:
        return None
    
    gear_str = gear_str.strip().upper()
    return GEAR_POSITION_MAPPING.get(gear_str)
