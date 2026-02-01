import requests
import zipfile
from pathlib import Path
import argparse

BASE_URL = "https://www.retrosheet.org/gamelogs" #this link should not change
DATA_DIR = Path("/mnt/c/Users/david/Projects/MLB-DB-Analysis/data/retrosheet/raw")

def fetch_gamelogs(season: int) -> Path:
    """
    Docstring for fetch_gamelogs
    
    :param season: Season to retrieve gamelogs for
    :type season: int
    :return: Path to the directory where gamelogs were extracted
    :rtype: Path
    """
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
    parser=argparse.ArgumentParser()
    parser.add_argument("--season", type=str, required=False, default=None, help="List of Seasons to fetch data for - separate seasons with commas (e.g. 2021,2022,2023)")
    args=parser.parse_args()
    if args.season:
        seasons = [int(s.strip()) for s in args.season.split(",")]
        for season in seasons:
            fetch_gamelogs(season)
    else:
        for season in range(1871, 2026): #default to fetch all available data if no season is given
            fetch_gamelogs(season)

if __name__ == "__main__":
    main()
