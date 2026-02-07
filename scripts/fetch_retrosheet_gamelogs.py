import requests
import zipfile
from pathlib import Path
import argparse

GAMELOG_URL = "https://www.retrosheet.org/gamelogs" #this link should not change
EVENTLOG_URL = "https://www.retrosheet.org/events" #this link should not change
DATA_DIR = Path("/mnt/c/Users/david/Projects/MLB-DB-Analysis/data/retrosheet")

def fetch_retrosheet_logs(season: int, BASE_URL: str) -> Path:
    """
    Docstring for fetch_retrosheet_logs
    
    :param season: Season to retrieve gamelogs for
    :type season: int
    :return: Path to the directory where gamelogs were extracted
    :rtype: Path
    """
    #Determine file path based on input URL and season
    if "gamelogs" in BASE_URL:
        url = f"{BASE_URL}/gl{season}.zip"
        #set outdir
        out_dir = Path(f"{DATA_DIR}/raw_gamelogs/{str(season)}")
        out_dir.mkdir(parents=True, exist_ok=True)

        zip_path = out_dir / f"gl{season}.zip"

        print(f"Downloading Retrosheet gamelogs for {season}...")

    elif "events" in BASE_URL:
        url = f"{BASE_URL}/{season}eve.zip"
        #set outdir
        out_dir = Path(f"{DATA_DIR}/raw_eventlogs/{str(season)}")
        out_dir.mkdir(parents=True, exist_ok=True)

        zip_path = out_dir / f"{season}eve.zip"

        print(f"Downloading Retrosheet event logs for {season}...")

    resp = requests.get(url)

    try:
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching data for season {season}: {e}")
        return None

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
            fetch_retrosheet_logs(season, GAMELOG_URL)
            fetch_retrosheet_logs(season, EVENTLOG_URL)
    else:
        for season in range(1871, 2026): #default to fetch all available data if no season is given
            fetch_retrosheet_logs(season, GAMELOG_URL)
            fetch_retrosheet_logs(season, EVENTLOG_URL)

if __name__ == "__main__":
    main()
