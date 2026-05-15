from pathlib import Path

from sensorops.quality.validate_bronze_source import (
    DEFAULT_BRONZE_SOURCE_DIR,
    read_jsonl,
    validate_bronze_source,
)


def test_bronze_source_directory_exists() -> None:
    assert DEFAULT_BRONZE_SOURCE_DIR.exists()


def test_ps1_source_file_has_records() -> None:
    ps1_file = DEFAULT_BRONZE_SOURCE_DIR / "sensor_raw_cycles" / "ps1_raw_cycles.jsonl"

    records = read_jsonl(ps1_file)

    assert len(records) > 0


def test_ps1_record_has_valid_reading_count() -> None:
    ps1_file = DEFAULT_BRONZE_SOURCE_DIR / "sensor_raw_cycles" / "ps1_raw_cycles.jsonl"

    first_record = read_jsonl(ps1_file)[0]

    assert first_record["sensor_id"] == "PS1"
    assert first_record["expected_reading_count"] == 6000
    assert first_record["actual_reading_count"] == 6000
    assert len(first_record["readings"]) == 6000
    assert first_record["structure_status"] == "OK"


def test_profile_source_file_has_records() -> None:
    profile_file = DEFAULT_BRONZE_SOURCE_DIR / "profile_raw" / "profile_raw.jsonl"

    records = read_jsonl(profile_file)

    assert len(records) > 0


def test_bronze_source_validation_passes() -> None:
    errors = validate_bronze_source(Path(DEFAULT_BRONZE_SOURCE_DIR))

    assert errors == []
