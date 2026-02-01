import requests
from pathlib import Path
from requests.exceptions import HTTPError, RequestException
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://statsapi.mlb.com/api/v1"
BASE_GAME_URL = "https://statsapi.mlb.com/api/v1.1"
DATA_DIR = Path("/mnt/c/Users/david/Projects/MLB-DB-Analysis/data/mlb_api/raw")

def get_schedule(season: int) -> dict:
    """
    Docstring for get_schedule
    
    :param season: Baseball season to retrieve data for
    :type season: int
    :return: JSON response containing the schedule data
    :rtype: dict

    Notes: This function interacts with the MLB API to 
    retrieve the schedule for a given baseball season. This 
    utilizes the BASE_URL (v1) endpoint. Note that a different
    URL must be used to fetch individual game data.
    """
    url = f"{BASE_URL}/schedule"
    params = {
        "sportId": 1,
        "season": season
    }

    try:
        resp = requests.get(url, params=params,timeout=30)
        resp.raise_for_status()
        return resp.json()

    except HTTPError as e:
        print(f"HTTP error fetching schedule for {season}: {e}")
        raise  # this *should* fail the run

    except RequestException as e:
        print(f"Network error fetching schedule for {season}: {e}")
        raise

def fetch_game_pk(season: int) -> list[int]:
    """
    Docstring for fetch_game_pk
    
    :param season: Baseball season to retrieve data for
    :type season: int
    :return: List of game primary keys (gamePk) for the season
    :rtype: list 
    """

    try:
        schedule = get_schedule(season)
    except Exception:
        print("Aborting run — could not retrieve schedule")
        return

    game_pk_list = []

    for date in schedule.get("dates", []):
        for game in date.get("games", []):

            game_pk = game.get("gamePk")
            if not game_pk:
                continue
            game_pk_list.append(game_pk)
    return game_pk_list


def fetch_single_game(game_pk: int, out_dir: Path) -> dict:
    """
    Docstring for fetch_single_game
    
    :param game_pk: Game primary key to retrieve data for
    :type game_pk: int
    :param out_dir: Output directory to save game data
    :type out_dir: Path
    :return: Dictionary from JSON response of game data
    :rtype: dict

    Note: This function fetches individual game data based on 
    a given game key, and uses a different endpoint from fetch schedule - 
    BASE_GAME_URL (v1.1) - to do so.
    """
    game_url = f"{BASE_GAME_URL}/game/{game_pk}/feed/live"

    try:
        resp = requests.get(game_url, timeout=30)

        if resp.status_code == 404:
            print(f"{game_pk} not available")
            return False

        resp.raise_for_status()

        out_file = out_dir / f"{game_pk}.json"
        out_file.write_text(resp.text)

        print(f"{game_pk}")
        return True

    except (HTTPError, RequestException) as e:
        print(f"{game_pk} failed: {e}")
        return False

def fetch_games_threaded(game_pks: list[int], out_dir: Path, max_workers: int = 8):
    """
    Docstring for fetch_games_threaded
    
    :param game_pks: List of game primary keys to retrieve data for
    :type game_pks: list[int]
    :param out_dir: Output directory to save game data
    :type out_dir: Path
    :param max_workers: Maximum number of concurrent threads to use
    :type max_workers: int

    Note: This function uses threading to fetch multiple games concrurently
    by calling fetch_single_game for each game_pk in the provided list.
    The MLB api will timeout if too many requests are made in a short period
    (roughly 25 requests per second). 
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_single_game, pk, out_dir)
            for pk in game_pks
        ]

        for future in as_completed(futures):
            if future.result():
                successes += 1
            else:
                failures += 1

    print(f"Done. Success: {successes}, Failures: {failures}")

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--season", type=str, required=False, default=None, help="List of Seasons to fetch data for - separate seasons with commas (e.g. 2021,2022,2023)")
    args=parser.parse_args()
    if args.season:
        seasons = [int(s.strip()) for s in args.season.split(",")]
        for season in seasons:
            game_pks = fetch_game_pk(season)
            fetch_games_threaded(game_pks, DATA_DIR / str(season), max_workers=10)
    else:
        for season in range(2021, 2027): #default to fetching all available data if no season given
            game_pks = fetch_game_pk(season)
            fetch_games_threaded(game_pks, DATA_DIR / str(season), max_workers=10)

if __name__ == "__main__":
    main()
