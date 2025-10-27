"""Pydantic schemas for Bronze layer (raw data from API)."""

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class VehicleMessageRaw(BaseModel):
    """Raw vehicle message schema for Bronze layer validation.
    
    This schema validates the raw data structure from the API endpoint
    before writing to the Bronze layer parquet files.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    # Core vehicle identification
    vin: str | None
    manufacturer: str | None
    year: int | None = None
    model: str | None = None
    
    # Vehicle state
    gearPosition: str | None = None  # Raw gear position as string
    velocity: int | None = None  # Speed in km/h
    
    # Vehicle systems
    frontLeftDoorState: str | None = None
    wipersState: bool | None = None
    driverSeatbeltState: str | None = None
    
    # Timestamp
    timestamp: datetime
    
    # Additional fields that might be present in API response
    # These are optional to handle varying API response structures
    speed: float | None = None
    rpm: int | None = None
    fuel_level: float | None = None
    engine_temp: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude: float | None = None
    
    @field_validator("timestamp")
    @classmethod
    def ensure_timezone_aware(cls, v: datetime) -> datetime:
        """Ensure timestamp is timezone-aware UTC.
        
        Args:
            v: Input datetime value
            
        Returns:
            Timezone-aware datetime in UTC
            
        Raises:
            ValueError: If timestamp cannot be converted to UTC
        """
        if v.tzinfo is None:
            # Assume naive datetime is UTC
            return v.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC if timezone-aware
            return v.astimezone(timezone.utc)
    
    @field_validator("vin")
    @classmethod
    def validate_vin(cls, v: str | None) -> str | None:
        """Validate VIN format if present.
        
        Args:
            v: VIN string or None
            
        Returns:
            VIN string or None
            
        Raises:
            ValueError: If VIN format is invalid
        """
        if v is None:
            return v
            
        # Basic VIN validation (17 characters, alphanumeric)
        v = v.strip().upper()
        if len(v) != 17:
            raise ValueError(f"VIN must be 17 characters, got {len(v)}")
        
        if not v.replace("I", "1").replace("O", "0").replace("Q", "0").isalnum():
            raise ValueError("VIN must contain only alphanumeric characters")
            
        return v
    
    @field_validator("manufacturer")
    @classmethod
    def validate_manufacturer(cls, v: str | None) -> str | None:
        """Validate manufacturer field.
        
        Args:
            v: Manufacturer string or None
            
        Returns:
            Manufacturer string or None
        """
        if v is None:
            return v
        return v.strip()
    
    @field_validator("gearPosition")
    @classmethod
    def validate_gear_position(cls, v: str | None) -> str | None:
        """Validate gear position field.
        
        Args:
            v: Gear position string or None
            
        Returns:
            Gear position string or None
        """
        if v is None:
            return v
        return v.strip().upper()
    
    @field_validator("velocity")
    @classmethod
    def validate_velocity(cls, v: int | None) -> int | None:
        """Validate velocity field.
        
        Args:
            v: Velocity integer or None
            
        Returns:
            Velocity integer or None
            
        Raises:
            ValueError: If value is negative
        """
        if v is not None and v < 0:
            raise ValueError(f"Velocity must be non-negative, got {v}")
        return v
    
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
