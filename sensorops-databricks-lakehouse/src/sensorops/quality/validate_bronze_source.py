import argparse
import json
from pathlib import Path

from sensorops.data_acquisition.hydraulic_metadata import (
    PROFILE_COLUMNS,
    SENSOR_METADATA,
)


DEFAULT_BRONZE_SOURCE_DIR = Path("data/landing/bronze_source")

SENSOR_REQUIRED_FIELDS = {
    "cycle_id",
    "asset_id",
    "source_system",
    "sensor_id",
    "physical_quantity",
    "unit",
    "sampling_rate_hz",
    "expected_reading_count",
    "actual_reading_count",
    "readings",
    "source_file_name",
    "source_file_path",
    "structure_status",
}

PROFILE_REQUIRED_FIELDS = {
    "cycle_id",
    "asset_id",
    "source_system",
    "source_file_name",
    "source_file_path",
    *PROFILE_COLUMNS,
}


def read_jsonl(file_path: Path) -> list[dict]:
    records = []

    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {file_path} at line {line_number}: {error}"
                ) from error

    return records


def validate_required_fields(
    record: dict,
    required_fields: set[str],
    file_path: Path,
    record_number: int,
) -> list[str]:
    errors = []

    missing_fields = sorted(required_fields - set(record.keys()))

    if missing_fields:
        errors.append(
            f"{file_path} record {record_number}: missing fields {missing_fields}"
        )

    return errors


def validate_sensor_record(
    record: dict,
    file_path: Path,
    record_number: int,
) -> list[str]:
    errors = []

    errors.extend(
        validate_required_fields(
            record=record,
            required_fields=SENSOR_REQUIRED_FIELDS,
            file_path=file_path,
            record_number=record_number,
        )
    )

    if errors:
        return errors

    readings = record["readings"]
    expected_count = record["expected_reading_count"]
    actual_count = record["actual_reading_count"]

    if not isinstance(readings, list):
        errors.append(f"{file_path} record {record_number}: readings is not a list")
        return errors

    if len(readings) != actual_count:
        errors.append(
            f"{file_path} record {record_number}: readings length {len(readings)} "
            f"does not match actual_reading_count {actual_count}"
        )

    if actual_count != expected_count:
        errors.append(
            f"{file_path} record {record_number}: actual_reading_count {actual_count} "
            f"does not match expected_reading_count {expected_count}"
        )

    if record["structure_status"] != "OK":
        errors.append(
            f"{file_path} record {record_number}: structure_status is "
            f"{record['structure_status']}, expected OK"
        )

    if not isinstance(record["cycle_id"], int):
        errors.append(f"{file_path} record {record_number}: cycle_id is not an integer")

    return errors


def validate_profile_record(
    record: dict,
    file_path: Path,
    record_number: int,
) -> list[str]:
    errors = []

    errors.extend(
        validate_required_fields(
            record=record,
            required_fields=PROFILE_REQUIRED_FIELDS,
            file_path=file_path,
            record_number=record_number,
        )
    )

    if errors:
        return errors

    if not isinstance(record["cycle_id"], int):
        errors.append(f"{file_path} record {record_number}: cycle_id is not an integer")

    for column in PROFILE_COLUMNS:
        if not isinstance(record[column], int):
            errors.append(
                f"{file_path} record {record_number}: {column} is not an integer"
            )

    return errors


def validate_sensor_files(bronze_source_dir: Path) -> tuple[list[str], dict[str, set[int]]]:
    errors = []
    sensor_cycle_ids = {}

    sensor_dir = bronze_source_dir / "sensor_raw_cycles"

    if not sensor_dir.exists():
        return [f"Missing sensor directory: {sensor_dir}"], sensor_cycle_ids

    for sensor in SENSOR_METADATA:
        sensor_id = sensor["sensor_id"]
        file_path = sensor_dir / f"{sensor_id.lower()}_raw_cycles.jsonl"

        if not file_path.exists():
            errors.append(f"Missing expected sensor file: {file_path}")
            continue

        records = read_jsonl(file_path)

        if not records:
            errors.append(f"Sensor file is empty: {file_path}")
            continue

        cycle_ids = set()

        for record_number, record in enumerate(records, start=1):
            errors.extend(
                validate_sensor_record(
                    record=record,
                    file_path=file_path,
                    record_number=record_number,
                )
            )

            if "cycle_id" in record:
                cycle_ids.add(record["cycle_id"])

            if record.get("sensor_id") != sensor_id:
                errors.append(
                    f"{file_path} record {record_number}: sensor_id "
                    f"{record.get('sensor_id')} does not match expected {sensor_id}"
                )

        sensor_cycle_ids[sensor_id] = cycle_ids

    return errors, sensor_cycle_ids


def validate_profile_file(bronze_source_dir: Path) -> tuple[list[str], set[int]]:
    errors = []
    profile_cycle_ids = set()

    profile_file = bronze_source_dir / "profile_raw" / "profile_raw.jsonl"

    if not profile_file.exists():
        return [f"Missing profile file: {profile_file}"], profile_cycle_ids

    records = read_jsonl(profile_file)

    if not records:
        return [f"Profile file is empty: {profile_file}"], profile_cycle_ids

    for record_number, record in enumerate(records, start=1):
        errors.extend(
            validate_profile_record(
                record=record,
                file_path=profile_file,
                record_number=record_number,
            )
        )

        if "cycle_id" in record:
            profile_cycle_ids.add(record["cycle_id"])

    return errors, profile_cycle_ids


def validate_cycle_alignment(
    sensor_cycle_ids: dict[str, set[int]],
    profile_cycle_ids: set[int],
) -> list[str]:
    errors = []

    if not profile_cycle_ids:
        return ["Profile cycle IDs are empty. Cannot validate cycle alignment."]

    for sensor_id, cycle_ids in sensor_cycle_ids.items():
        missing_in_profile = cycle_ids - profile_cycle_ids
        missing_in_sensor = profile_cycle_ids - cycle_ids

        if missing_in_profile:
            errors.append(
                f"{sensor_id}: {len(missing_in_profile)} sensor cycles missing in profile"
            )

        if missing_in_sensor:
            errors.append(
                f"{sensor_id}: {len(missing_in_sensor)} profile cycles missing in sensor"
            )

    return errors


def validate_bronze_source(bronze_source_dir: Path) -> list[str]:
    errors = []

    if not bronze_source_dir.exists():
        return [f"Bronze source directory does not exist: {bronze_source_dir}"]

    sensor_errors, sensor_cycle_ids = validate_sensor_files(bronze_source_dir)
    profile_errors, profile_cycle_ids = validate_profile_file(bronze_source_dir)

    errors.extend(sensor_errors)
    errors.extend(profile_errors)
    errors.extend(validate_cycle_alignment(sensor_cycle_ids, profile_cycle_ids))

    return errors


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate ingestion-ready Bronze source JSONL files."
    )

    parser.add_argument(
        "--bronze-source-dir",
        type=Path,
        default=DEFAULT_BRONZE_SOURCE_DIR,
        help="Directory containing prepared Bronze source JSONL files.",
    )

    args = parser.parse_args()

    print(f"Validating Bronze source directory: {args.bronze_source_dir}")

    errors = validate_bronze_source(args.bronze_source_dir)

    if errors:
        print("\nValidation failed.")
        print(f"Error count: {len(errors)}")

        for error in errors[:50]:
            print(f"- {error}")

        if len(errors) > 50:
            print(f"... and {len(errors) - 50} more errors")

        raise SystemExit(1)

    print("\nValidation successful.")
    print("Bronze source files are ready for Databricks ingestion.")


if __name__ == "__main__":
    main()
