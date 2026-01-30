import requests
from pathlib import Path
from requests.exceptions import HTTPError, RequestException

BASE_URL = "https://statsapi.mlb.com/api/v1"
BASE_GAME_URL = "https://statsapi.mlb.com/api/v1.1"
DATA_DIR = Path("/mnt/c/Users/david/Projects/MLB-DB-Analysis/data/mlb_api/raw")

def get_schedule(season: int) -> dict:
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

def fetch_games(season: int, limit: int | None = None) -> None:
    out_dir = DATA_DIR / str(season)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        schedule = get_schedule(season)
    except Exception:
        print("Aborting run — could not retrieve schedule")
        return

    games_fetched = 0
    games_skipped = 0

    for date in schedule.get("dates", []):
        for game in date.get("games", []):

            game_pk = game.get("gamePk")
            if not game_pk:
                continue

            game_url = f"{BASE_GAME_URL}/game/{game_pk}/feed/live"

            try:
                resp = requests.get(game_url, timeout=30)

                if resp.status_code == 404:
                    print(game_url)
                    print(f" Game {game_pk} not available yet, skipping")
                    games_skipped += 1
                    continue

                resp.raise_for_status()

                out_file = out_dir / f"{game_pk}.json"
                out_file.write_text(resp.text)

                print(f"Saved game {game_pk}")
                games_fetched += 1

            except HTTPError as e:
                print(f"HTTP error for game {game_pk}: {e}")
                games_skipped += 1
                continue

            except RequestException as e:
                print(f"Network error for game {game_pk}: {e}")
                games_skipped += 1
                continue

            if limit and games_fetched >= limit:
                print("Fetch limit reached")
                print(f"Fetched: {games_fetched}, Skipped: {games_skipped}")
                return

    print(f"Done. Fetched: {games_fetched}, Skipped: {games_skipped}")

if __name__ == "__main__":
    fetch_games(2024, limit=5)
