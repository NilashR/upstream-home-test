"""Pydantic schemas for Silver layer (cleaned and standardized data)."""

from datetime import datetime
from enum import IntEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class VehicleMessageCleaned(BaseModel):
    """Cleaned vehicle message schema for Silver layer.
    
    This schema represents the cleaned and standardized data after
    processing the Bronze layer raw data.
    """
    
    model_config = ConfigDict(extra="forbid")

    vin: str
    manufacturer: str | None
    year: int | None = None
    model: str | None = None
    gear_position: int | None  # Standardized gear position as integer
    velocity: int | None = None
    front_left_door_state: str | None = None
    wipers_state: bool | None = None
    driver_seatbelt_state: str | None = None
    timestamp: datetime
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
        if v is not None and v not in {-1, 0, 1, 2, 3, 4, 5, 6, None}:
            raise ValueError(f"Gear position must be -1 to 6,null got {v}")
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
            raise ValueError("VIN cannot be null")
        return v.strip().upper()

    
    @field_validator("manufacturer")
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


# Gear position enum with all available options
class GearPosition(IntEnum):
    """Standardized gear position enum with all available options."""
    UNKNOWN = -1
    PARK = 0
    REVERSE = 1
    NEUTRAL = 2
    DRIVE = 3
    LOW = 4
    GEAR_5 = 5
    GEAR_6 = 6


# Gear position mapping constants
GEAR_POSITION_MAPPING = {
    # Standard gear positions
    "P": GearPosition.PARK,
    "R": GearPosition.REVERSE,
    "N": GearPosition.NEUTRAL,
    "D": GearPosition.DRIVE,
    "L": GearPosition.LOW,
    
    # Numeric gear positions
    "-1": GearPosition.UNKNOWN,
    "0": GearPosition.PARK,
    "1": GearPosition.REVERSE,
    "2": GearPosition.NEUTRAL,
    "3": GearPosition.DRIVE,
    "4": GearPosition.LOW,
    "5": GearPosition.GEAR_5,
    "6": GearPosition.GEAR_6,
    
    # String representations
    "PARK": GearPosition.PARK,
    "REVERSE": GearPosition.REVERSE,
    "NEUTRAL": GearPosition.NEUTRAL,
    "DRIVE": GearPosition.DRIVE,
    "LOW": GearPosition.LOW,
    
    # Special cases
    "": GearPosition.UNKNOWN,
    "UNKNOWN": GearPosition.UNKNOWN,
    "NULL": GearPosition.UNKNOWN,
}


def map_gear_position(gear_str: str | None) -> int | None:
    """Map gear position string to standardized integer.
    
    Args:
        gear_str: Gear position string or None
        
    Returns:
        Mapped integer (-1, 0-6) or None if invalid/None
        -1: Unknown/Invalid
        0: Park
        1: Reverse  
        2: Neutral
        3: Drive
        4: Low
        5: Gear 2/5
        6: Gear 3/6
    """
    if gear_str is None:
        return None
    
    gear_str = gear_str.strip().upper()
    mapped_value = GEAR_POSITION_MAPPING.get(gear_str)
    
    if mapped_value is not None:
        return mapped_value.value
    
    # If not found in mapping, return unknown
    return GearPosition.UNKNOWN.value
