from datetime import datetime

REQUIRED_FIELDS = ["id", "name", "value", "timestamp"]

def transform_record(record: dict) -> dict:
    """Validate and transform a raw data record."""
    validate_record(record)
    return {
        "id": str(record["id"]),
        "name": record["name"].strip().lower(),
        "value": float(record["value"]),
        "timestamp": normalize_timestamp(record["timestamp"]),
        "processed_at": datetime.utcnow().isoformat()
    }

def validate_record(record: dict):
    """Validate required fields are present."""
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

def normalize_timestamp(ts: str) -> str:
    """Normalize timestamp to ISO 8601 format."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.isoformat()
    except ValueError:
        raise ValueError(f"Invalid timestamp format: {ts}")
