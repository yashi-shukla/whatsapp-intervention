#!/usr/bin/env python3

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError
import pandas as pd
import numpy as np


# Status constants (simplified from enums)
STATUS_TYPES = ["sent", "delivered", "read", "failed", "deleted"]
MESSAGE_TYPES = ["text", "interactive", "button", "image", "document", "audio", "hsm", "template", "video", "voice", "sticker", "contacts", "reaction"]
AUTHOR_TYPES = ["OWNER", "USER", "STACK", "AUTOMATOR", "OPERATOR", "SYSTEM"]
DIRECTIONS = ["inbound", "outbound"]


class RawMessage(BaseModel):
    """Raw message data model"""

    id: int
    message_type: str
    masked_addressees: Optional[str]
    masked_author: Optional[str]
    content: Optional[str]
    author_type: str
    direction: str
    external_id: Optional[str]
    external_timestamp: Optional[str]
    masked_from_addr: Optional[str]
    is_deleted: Optional[str]
    last_status: Optional[str]
    last_status_timestamp: Optional[str]
    rendered_content: Optional[str]
    source_type: Optional[str]
    uuid: str
    inserted_at: str
    updated_at: str

    # Convert string timestamps to datetime objects
    @field_validator(
        "inserted_at", "updated_at", "external_timestamp", "last_status_timestamp"
    )
    @classmethod
    def validate_datetime(cls, v):
        if v and v != "":
            try:
                # Handle different datetime formats
                if isinstance(v, str):
                    return datetime.fromisoformat(v.replace(" ", "T"))
                elif isinstance(v, datetime):
                    return v
                else:
                    return str(v)  # Keep as string if can't parse
            except:
                return v
        return v


class RawStatus(BaseModel):
    """Raw status data model"""

    id: int
    status: str
    timestamp: str
    uuid: str
    message_uuid: Optional[str]
    message_id: Optional[int]
    number_id: Optional[int]
    inserted_at: str
    updated_at: str

    # Convert string timestamps to datetime objects (uses the more comprehensive validator above)


class UnifiedMessage(BaseModel):
    """Unified message with status information"""

    # Core message fields
    id: Optional[int] = None
    uuid: Optional[str] = None
    message_type: Optional[str] = None
    content: Optional[str] = None
    author_type: Optional[str] = None
    direction: Optional[str] = None
    external_id: Optional[str] = None
    external_timestamp: Optional[str] = None
    inserted_at: Optional[str] = None
    updated_at: Optional[str] = None

    # Status count fields
    sent: Optional[int] = None
    delivered: Optional[int] = None
    read: Optional[int] = None
    failed: Optional[int] = None
    deleted: Optional[int] = None

    # Status metadata (simplified - one set per message)
    status_uuid: Optional[str] = None
    status_timestamp: Optional[str] = None
    status_inserted_at: Optional[str] = None
    status_updated_at: Optional[str] = None
    status_id: Optional[int] = None
    status_message_id: Optional[int] = None
    status_number_id: Optional[int] = None

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, v):
        if v and v not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}")
        return v

    @field_validator("message_type")
    @classmethod
    def validate_message_type(cls, v):
        if v and v not in MESSAGE_TYPES:
            raise ValueError(f"message_type must be one of {MESSAGE_TYPES}")
        return v

    @field_validator("author_type")
    @classmethod
    def validate_author_type(cls, v):
        if v and v not in AUTHOR_TYPES:
            raise ValueError(f"author_type must be one of {AUTHOR_TYPES}")
        return v

    class Config:
        validate_assignment = True
        validate_by_name = True

    @classmethod
    def from_pandas_row(cls, row, strict: bool = False):
        """Create UnifiedMessage from pandas DataFrame row"""
        data = {}

        for field_name, field_info in cls.model_fields.items():
            alias = field_info.alias or field_name
            value = row.get(alias)

            # Handle NaN values and type conversions with better error handling
            if pd.isna(value) or value is None:
                data[field_name] = None
            elif isinstance(value, np.integer):
                data[field_name] = int(value)
            elif isinstance(value, np.floating):
                if np.isnan(value):
                    data[field_name] = None
                else:
                    data[field_name] = str(value)
            elif isinstance(value, (int, float)):
                if np.isnan(value):
                    data[field_name] = None
                else:
                    data[field_name] = value
            elif isinstance(value, str):
                data[field_name] = value.strip() if value.strip() else None
            else:
                data[field_name] = str(value) if value is not None else None

        try:
            return cls(**data)
        except ValidationError as e:
            if strict:
                raise e
            else:
                # Try to create with minimal valid data
                minimal_data = {k: v for k, v in data.items() if v is not None}
                try:
                    return cls(**minimal_data)
                except ValidationError:
                    # Last resort: create with just required fields as None
                    safe_data = {}
                    for field_name in cls.model_fields:
                        if field_name in data:
                            safe_data[field_name] = data[field_name]
                        else:
                            safe_data[field_name] = None
                    return cls(**safe_data)


class DuplicateRecord(BaseModel):
    """Duplicate record data model"""

    id: int
    content: Optional[str] = None
    inserted_at: str  # Keep as string, convert when needed
    uuid: str
    direction: str
    duplicate_group: Optional[str] = None

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, v):
        if v and v not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}")
        return v

    @field_validator("inserted_at")
    @classmethod
    def validate_inserted_at(cls, v):
        """Convert datetime to string if needed"""
        if v is None:
            return v
        if isinstance(v, datetime):
            return str(v)
        if hasattr(v, 'isoformat'):  # pandas Timestamp
            return str(v)
        if isinstance(v, str):
            return v
        return str(v)

    @classmethod
    def from_pandas_row(cls, row, strict: bool = False):
        """Create DuplicateRecord from pandas DataFrame row"""
        data = {}

        for field_name in cls.model_fields.keys():
            value = row.get(field_name)

            # Handle NaN values and type conversions
            if pd.isna(value) or value is None:
                # Provide defaults for required fields
                if field_name == "id":
                    data[field_name] = 0
                elif field_name == "uuid":
                    data[field_name] = "unknown-uuid"
                elif field_name == "direction":
                    data[field_name] = "unknown"
                elif field_name == "inserted_at":
                    data[field_name] = str(datetime.now())
                else:
                    data[field_name] = None
            elif field_name == "inserted_at":
                # Special handling for timestamp conversion
                if hasattr(value, 'isoformat'):  # pandas Timestamp
                    data[field_name] = str(value)
                elif isinstance(value, datetime):
                    data[field_name] = str(value)
                else:
                    data[field_name] = str(value)
            elif isinstance(value, np.integer):
                data[field_name] = int(value)
            elif isinstance(value, str):
                data[field_name] = value.strip() if value.strip() else None
            else:
                data[field_name] = value

        return cls(**data)


class QualityCheckResult(BaseModel):
    """Data quality check result"""

    check_name: str
    check_type: str = "count"  # 'count', 'flag', 'percentage', 'boolean'
    value: int = 0
    description: str = ""
    severity: str = "info"  # 'info', 'warning', 'error', 'critical'


class DataQualityReport(BaseModel):
    """Data quality report"""

    total_messages: int = 0
    total_statuses: int = 0
    messages_without_status: int = 0
    invalid_statuses: int = 0
    future_timestamps: int = 0
    missing_required_fields: int = 0
    duplicates_found: int = 0
    checks: List[QualityCheckResult] = []
    report_generated_at: datetime = Field(default_factory=datetime.now)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the data quality report"""
        error_count = sum(1 for check in self.checks if check.severity == "error")
        warning_count = sum(1 for check in self.checks if check.severity == "warning")

        return {
            "total_messages": self.total_messages,
            "total_statuses": self.total_statuses,
            "data_quality_score": max(
                0, 100 - (error_count * 20) - (warning_count * 5)
            ),
            "errors_found": error_count,
            "warnings_found": warning_count,
            "report_generated_at": self.report_generated_at.isoformat(),
        }





# Utility functions for JSONL serialization


def save_jsonl(data, filepath: str) -> None:
    """Save list of models or DataFrame to JSONL file"""
    import pandas as pd
    import json
    import logging

    logger = logging.getLogger(__name__)

    with open(filepath, "w", encoding="utf-8") as f:
        if isinstance(data, pd.DataFrame):
            # Save DataFrame data
            for i, (_, row) in enumerate(data.iterrows()):
                # Convert row to dict and handle NaT values
                record = {}
                for col, val in row.items():
                    if pd.isna(val):
                        record[col] = None
                    else:
                        record[col] = val
                f.write(json.dumps(record, default=str) + "\n")

                if i == 0:  # Log first record structure
                    logger.info(f"First JSON record columns: {list(record.keys())}")
        elif isinstance(data, list) and data and hasattr(data[0], "model_dump_json"):
            # Handle list of BaseModel objects
            for item in data:
                f.write(item.model_dump_json(exclude_unset=True, by_alias=True) + "\n")
        else:
            # Handle list of dictionaries or other iterables
            for item in data:
                if hasattr(item, "model_dump_json"):
                    f.write(
                        item.model_dump_json(exclude_unset=True, by_alias=True) + "\n"
                    )
                else:
                    f.write(json.dumps(item, default=str) + "\n")






