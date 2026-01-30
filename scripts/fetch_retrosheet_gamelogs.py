import requests
import zipfile
from pathlib import Path

BASE_URL = "https://www.retrosheet.org/gamelogs" #this link should not change
DATA_DIR = Path("/mnt/c/Users/david/Projects/MLB-DB-Analysis/data/retrosheet/raw")

def fetch_gamelogs(season: int) -> Path:
    url = f"{BASE_URL}/gl{season}.zip"
    out_dir = Path(f"{DATA_DIR}/{str(season)}")
    out_dir.mkdir(parents=True, exist_ok=True)

    zip_path = out_dir / f"gl{season}.zip"

    print(f"Downloading Retrosheet gamelogs for {season}...")
    resp = requests.get(url)
    resp.raise_for_status()

    zip_path.write_bytes(resp.content)

    print("Extracting...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)

    return out_dir

def main():
    for season in range(1871, 2026):
        fetch_gamelogs(season)

if __name__ == "__main__":
    main()
