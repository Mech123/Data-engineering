from pathlib import Path
from urllib.request import urlretrieve
from zipfile import ZipFile


DATASET_URL = "https://zenodo.org/records/1323611/files/data.zip?download=1"


def download_file(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"File already exists: {output_path}")
        return

    print(f"Downloading dataset to: {output_path}")
    urlretrieve(url, output_path)
    print("Download completed.")


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)

    print(f"Extracting {zip_path} to {extract_dir}")

    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    print("Extraction completed.")


def main() -> None:
    raw_dir = Path("data/raw/hydraulic_systems")
    zip_path = raw_dir / "hydraulic_systems.zip"
    extract_dir = raw_dir / "extracted"

    download_file(DATASET_URL, zip_path)
    extract_zip(zip_path, extract_dir)

    print("\nDataset is ready.")
    print(f"Raw dataset folder: {extract_dir}")


if __name__ == "__main__":
    main()
