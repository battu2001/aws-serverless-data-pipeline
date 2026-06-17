import pytest
from functions.processor.transformer import transform_record, validate_record, normalize_timestamp

def test_transform_record_valid():
    record = {"id": 1, "name": "  Test Record  ", "value": "42.5", "timestamp": "2024-01-15T10:30:00Z"}
    result = transform_record(record)
    assert result["id"] == "1"
    assert result["name"] == "test record"
    assert result["value"] == 42.5
    assert "processed_at" in result

def test_validate_record_missing_fields():
    with pytest.raises(ValueError, match="Missing required fields"):
        validate_record({"id": 1, "name": "test"})

def test_normalize_timestamp_valid():
    ts = normalize_timestamp("2024-01-15T10:30:00Z")
    assert "2024-01-15" in ts

def test_normalize_timestamp_invalid():
    with pytest.raises(ValueError, match="Invalid timestamp format"):
        normalize_timestamp("not-a-timestamp")

def test_transform_strips_whitespace():
    record = {"id": 2, "name": "   spaces   ", "value": "10", "timestamp": "2024-01-15T10:30:00Z"}
    result = transform_record(record)
    assert result["name"] == "spaces"
