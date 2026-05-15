from pathlib import Path

import pandas as pd

from sensorops.data_acquisition.hydraulic_metadata import (
    PROFILE_COLUMNS,
    PROFILE_VALUE_MEANINGS,
    SENSOR_METADATA,
)


DATASET_DIR = Path("data/raw/hydraulic_systems/extracted")
OUTPUT_DOC = Path("docs/dataset_understanding.md")


def count_rows(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8", errors="ignore") as file:
        return sum(1 for _ in file)


def count_columns_in_first_row(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8", errors="ignore") as file:
        first_line = file.readline()
    return len(first_line.split())


def build_sensor_structure_summary() -> list[dict]:
    summary = []

    for sensor in SENSOR_METADATA:
        file_path = DATASET_DIR / sensor["file_name"]

        if not file_path.exists():
            raise FileNotFoundError(f"Missing expected sensor file: {file_path}")

        actual_rows = count_rows(file_path)
        actual_columns = count_columns_in_first_row(file_path)
        expected_columns = sensor["sampling_rate_hz"] * 60

        summary.append(
            {
                "file_name": sensor["file_name"],
                "sensor_id": sensor["sensor_id"],
                "physical_quantity": sensor["physical_quantity"],
                "unit": sensor["unit"],
                "sampling_rate_hz": sensor["sampling_rate_hz"],
                "expected_columns": expected_columns,
                "actual_columns": actual_columns,
                "rows_cycles": actual_rows,
                "structure_status": "OK"
                if expected_columns == actual_columns
                else "CHECK",
            }
        )

    return summary


def read_profile() -> pd.DataFrame:
    profile_path = DATASET_DIR / "profile.txt"

    if not profile_path.exists():
        raise FileNotFoundError(f"Missing profile file: {profile_path}")

    return pd.read_csv(
        profile_path,
        sep=r"\s+",
        header=None,
        names=PROFILE_COLUMNS,
        engine="python",
    )


def print_sensor_summary(summary: list[dict]) -> None:
    print("\nRaw sensor file structure:\n")

    for row in summary:
        print(
            f"{row['file_name']:8} | "
            f"{row['physical_quantity']:20} | "
            f"{row['sampling_rate_hz']:3} Hz | "
            f"expected_cols={row['expected_columns']:5} | "
            f"actual_cols={row['actual_columns']:5} | "
            f"rows={row['rows_cycles']:4} | "
            f"{row['structure_status']}"
        )


def print_profile_distribution(profile_df: pd.DataFrame) -> None:
    print("\nProfile target distributions:\n")

    for column in PROFILE_COLUMNS:
        print(f"{column}:")
        counts = profile_df[column].value_counts().sort_index()

        for value, count in counts.items():
            meaning = PROFILE_VALUE_MEANINGS[column].get(value, "unknown")
            print(f"  {value}: {count} cycles | {meaning}")

        print()


def write_dataset_doc(summary: list[dict], profile_df: pd.DataFrame) -> None:
    OUTPUT_DOC.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Dataset Understanding",
        "",
        "## Dataset",
        "",
        "Condition Monitoring of Hydraulic Systems",
        "",
        "The dataset contains raw industrial sensor matrices from a hydraulic test rig.",
        "Each sensor file contains one row per 60-second load cycle.",
        "Each column inside a sensor file represents a measurement point within that cycle.",
        "",
        "## Raw Sensor Structure",
        "",
        "| File | Sensor | Quantity | Unit | Sampling Rate | Expected Columns | Actual Columns | Rows/Cycles | Status |",
        "|---|---|---|---|---:|---:|---:|---:|---|",
    ]

    for row in summary:
        lines.append(
            f"| {row['file_name']} "
            f"| {row['sensor_id']} "
            f"| {row['physical_quantity']} "
            f"| {row['unit']} "
            f"| {row['sampling_rate_hz']} Hz "
            f"| {row['expected_columns']} "
            f"| {row['actual_columns']} "
            f"| {row['rows_cycles']} "
            f"| {row['structure_status']} |"
        )

    lines.extend(
        [
            "",
            "## Profile Target Columns",
            "",
            "| Column | Meaning |",
            "|---|---|",
            "| cooler_condition_pct | Cooler condition percentage |",
            "| valve_condition_pct | Valve condition percentage |",
            "| internal_pump_leakage | Internal pump leakage severity |",
            "| hydraulic_accumulator_bar | Hydraulic accumulator pressure condition |",
            "| stable_flag | Whether stable conditions were reached |",
            "",
            "## Profile Target Distributions",
            "",
        ]
    )

    for column in PROFILE_COLUMNS:
        lines.append(f"### {column}")
        lines.append("")
        lines.append("| Value | Cycle Count | Meaning |")
        lines.append("|---:|---:|---|")

        counts = profile_df[column].value_counts().sort_index()

        for value, count in counts.items():
            meaning = PROFILE_VALUE_MEANINGS[column].get(value, "unknown")
            lines.append(f"| {value} | {count} | {meaning} |")

        lines.append("")

    lines.extend(
        [
            "## Initial Lakehouse Design Decision",
            "",
            "For Bronze, we will preserve raw readings as arrays at the grain of one row per cycle per sensor.",
            "This keeps the raw sensor sequence without exploding the dataset too early.",
            "",
            "For Silver, we will create cleaned cycle-level and sensor-level feature tables.",
            "This is where we calculate statistics such as mean, min, max, standard deviation, and anomaly flags.",
            "",
            "For Gold, we will create business-ready machine health, component condition, and maintenance-risk tables.",
            "",
        ]
    )

    OUTPUT_DOC.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nDocumentation written to: {OUTPUT_DOC}")


def main() -> None:
    if not DATASET_DIR.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {DATASET_DIR}. "
            "Run download_hydraulic_dataset.py first."
        )

    summary = build_sensor_structure_summary()
    profile_df = read_profile()

    print_sensor_summary(summary)
    print_profile_distribution(profile_df)
    write_dataset_doc(summary, profile_df)

    failed_files = [row for row in summary if row["structure_status"] != "OK"]

    if failed_files:
        raise ValueError(f"Some files have unexpected column counts: {failed_files}")

    print("\nDataset structure profiling completed successfully.")


if __name__ == "__main__":
    main()
