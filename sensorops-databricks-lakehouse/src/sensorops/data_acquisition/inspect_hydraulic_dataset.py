from pathlib import Path


EXPECTED_FILES = [
    "PS1.txt",
    "PS2.txt",
    "PS3.txt",
    "PS4.txt",
    "PS5.txt",
    "PS6.txt",
    "EPS1.txt",
    "FS1.txt",
    "FS2.txt",
    "TS1.txt",
    "TS2.txt",
    "TS3.txt",
    "TS4.txt",
    "VS1.txt",
    "CE.txt",
    "CP.txt",
    "SE.txt",
    "profile.txt",
]


def count_lines(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8", errors="ignore") as file:
        return sum(1 for _ in file)


def preview_file(file_path: Path, lines: int = 2) -> None:
    print(f"\nPreview: {file_path.name}")

    with file_path.open("r", encoding="utf-8", errors="ignore") as file:
        for index, line in enumerate(file):
            if index >= lines:
                break
            print(line.strip()[:250])


def main() -> None:
    dataset_dir = Path("data/raw/hydraulic_systems/extracted")

    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset folder not found: {dataset_dir}. "
            "Run download_hydraulic_dataset.py first."
        )

    print("Checking expected dataset files...\n")

    missing_files = []

    for file_name in EXPECTED_FILES:
        file_path = dataset_dir / file_name

        if not file_path.exists():
            missing_files.append(file_name)
            print(f"Missing: {file_name}")
        else:
            line_count = count_lines(file_path)
            print(f"Found: {file_name} | rows: {line_count}")

    if missing_files:
        raise FileNotFoundError(f"Missing files: {missing_files}")

    print("\nAll expected files are available.")

    preview_file(dataset_dir / "PS1.txt")
    preview_file(dataset_dir / "TS1.txt")
    preview_file(dataset_dir / "VS1.txt")
    preview_file(dataset_dir / "profile.txt")


if __name__ == "__main__":
    main()
