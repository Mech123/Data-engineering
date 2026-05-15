import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from sensorops.data_acquisition.hydraulic_metadata import (
    PROFILE_COLUMNS,
    SENSOR_METADATA,
)


DEFAULT_DATASET_DIR = Path("data/raw/hydraulic_systems/extracted")
DEFAULT_OUTPUT_DIR = Path("data/landing/bronze_source")
ASSET_ID = "HYDRAULIC_TEST_RIG_001"
SOURCE_SYSTEM = "UCI_HYDRAULIC_TEST_RIG"


def parse_sensor_readings(line: str) -> list[float]:
    return [float(value) for value in line.split()]


def parse_profile_values(line: str) -> dict:
    values = [int(value) for value in line.split()]

    if len(values) != len(PROFILE_COLUMNS):
        raise ValueError(
            f"Expected {len(PROFILE_COLUMNS)} profile columns, got {len(values)}"
        )

    return dict(zip(PROFILE_COLUMNS, values, strict=True))


def write_sensor_raw_cycles(
    dataset_dir: Path,
    output_dir: Path,
    max_cycles: int | None,
) -> list[dict]:
    sensor_output_dir = output_dir / "sensor_raw_cycles"
    sensor_output_dir.mkdir(parents=True, exist_ok=True)

    summaries = []

    for sensor in SENSOR_METADATA:
        input_file = dataset_dir / sensor["file_name"]
        output_file = sensor_output_dir / f"{sensor['sensor_id'].lower()}_raw_cycles.jsonl"

        expected_reading_count = sensor["sampling_rate_hz"] * 60
        written_count = 0
        invalid_structure_count = 0

        with input_file.open("r", encoding="utf-8", errors="ignore") as reader, output_file.open(
            "w", encoding="utf-8"
        ) as writer:
            for cycle_index, line in enumerate(reader, start=1):
                if max_cycles is not None and cycle_index > max_cycles:
                    break

                readings = parse_sensor_readings(line)
                actual_reading_count = len(readings)
                structure_status = (
                    "OK"
                    if actual_reading_count == expected_reading_count
                    else "CHECK"
                )

                if structure_status != "OK":
                    invalid_structure_count += 1

                record = {
                    "cycle_id": cycle_index,
                    "asset_id": ASSET_ID,
                    "source_system": SOURCE_SYSTEM,
                    "sensor_id": sensor["sensor_id"],
                    "physical_quantity": sensor["physical_quantity"],
                    "unit": sensor["unit"],
                    "sampling_rate_hz": sensor["sampling_rate_hz"],
                    "expected_reading_count": expected_reading_count,
                    "actual_reading_count": actual_reading_count,
                    "readings": readings,
                    "source_file_name": sensor["file_name"],
                    "source_file_path": str(input_file.as_posix()),
                    "structure_status": structure_status,
                }

                writer.write(json.dumps(record) + "\n")
                written_count += 1

        summaries.append(
            {
                "source_file": sensor["file_name"],
                "output_file": str(output_file.as_posix()),
                "sensor_id": sensor["sensor_id"],
                "records_written": written_count,
                "invalid_structure_count": invalid_structure_count,
            }
        )

    return summaries


def write_profile_raw(
    dataset_dir: Path,
    output_dir: Path,
    max_cycles: int | None,
) -> dict:
    profile_output_dir = output_dir / "profile_raw"
    profile_output_dir.mkdir(parents=True, exist_ok=True)

    input_file = dataset_dir / "profile.txt"
    output_file = profile_output_dir / "profile_raw.jsonl"

    written_count = 0

    with input_file.open("r", encoding="utf-8", errors="ignore") as reader, output_file.open(
        "w", encoding="utf-8"
    ) as writer:
        for cycle_index, line in enumerate(reader, start=1):
            if max_cycles is not None and cycle_index > max_cycles:
                break

            profile_values = parse_profile_values(line)

            record = {
                "cycle_id": cycle_index,
                "asset_id": ASSET_ID,
                "source_system": SOURCE_SYSTEM,
                **profile_values,
                "source_file_name": "profile.txt",
                "source_file_path": str(input_file.as_posix()),
            }

            writer.write(json.dumps(record) + "\n")
            written_count += 1

    return {
        "source_file": "profile.txt",
        "output_file": str(output_file.as_posix()),
        "records_written": written_count,
    }


def write_manifest(
    output_dir: Path,
    max_cycles: int | None,
    sensor_summaries: list[dict],
    profile_summary: dict,
) -> None:
    manifest = {
        "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
        "asset_id": ASSET_ID,
        "source_system": SOURCE_SYSTEM,
        "max_cycles": max_cycles,
        "sensor_file_count": len(sensor_summaries),
        "sensor_summaries": sensor_summaries,
        "profile_summary": profile_summary,
    }

    manifest_path = output_dir / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare ingestion-ready Bronze source JSONL files from hydraulic raw data."
    )

    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=DEFAULT_DATASET_DIR,
        help="Directory containing extracted hydraulic dataset txt files.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for generated Bronze source files.",
    )

    parser.add_argument(
        "--max-cycles",
        type=int,
        default=25,
        help="Maximum number of cycles to prepare. Use 0 for full dataset.",
    )

    args = parser.parse_args()

    if not args.dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {args.dataset_dir}. "
            "Run download_hydraulic_dataset.py first."
        )

    max_cycles = None if args.max_cycles == 0 else args.max_cycles

    print("Preparing Bronze source files...")
    print(f"Dataset directory: {args.dataset_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Max cycles: {'FULL DATASET' if max_cycles is None else max_cycles}")

    sensor_summaries = write_sensor_raw_cycles(
        dataset_dir=args.dataset_dir,
        output_dir=args.output_dir,
        max_cycles=max_cycles,
    )

    profile_summary = write_profile_raw(
        dataset_dir=args.dataset_dir,
        output_dir=args.output_dir,
        max_cycles=max_cycles,
    )

    write_manifest(
        output_dir=args.output_dir,
        max_cycles=max_cycles,
        sensor_summaries=sensor_summaries,
        profile_summary=profile_summary,
    )

    print("\nBronze source preparation completed.")
    print("\nSensor files:")

    for summary in sensor_summaries:
        print(
            f"- {summary['sensor_id']}: "
            f"{summary['records_written']} records | "
            f"invalid structures: {summary['invalid_structure_count']}"
        )

    print(
        f"\nProfile records: {profile_summary['records_written']}"
    )
    print(f"Manifest: {args.output_dir / '_manifest.json'}")


if __name__ == "__main__":
    main()
