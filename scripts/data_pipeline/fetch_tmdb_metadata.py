import os
import json
import time
from pathlib import Path
from collections import defaultdict

import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

if not TMDB_API_KEY:
    raise RuntimeError(
        "Missing TMDB_API_KEY. Add it to your environment or `.env` file."
    )

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "tmdb_clean_parts"
RAW_DIR.mkdir(parents=True, exist_ok=True)

base = pd.read_pickle("data/movie_base.pkl")

session = requests.Session()

# fields we want
FIELDS = [
    "genres",
    "origin_country",
    "overview",
    "popularity",
    "poster_path",
    "spoken_languages",
    "release_date",
    "keywords",
]


def fetch_movie(tmdb_id: int):

    url = f"{BASE_URL}/movie/{tmdb_id}"

    params = {
        "api_key": TMDB_API_KEY,
        "append_to_response": "credits,keywords",
    }

    max_retries = 6
    backoff_s = 0.5

    for attempt in range(max_retries):
        r = session.get(url, params=params, timeout=30)

        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    sleep_s = backoff_s
            else:
                sleep_s = backoff_s
            time.sleep(sleep_s)
            backoff_s = min(backoff_s * 2, 20)
            continue

        if 500 <= r.status_code < 600:
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 2, 20)
            continue

        return r

    return r


def clean_movie(data):

    cast = data.get("credits", {}).get("cast", [])

    actors = []

    crew = data.get("credits", {}).get("crew", [])

    directors = [
        {
            "director_id": c.get("id"),
            "name": c.get("name")
        }
        for c in crew
        if c.get("job") == "Director"
    ]

    for c in cast[:20]:

        actors.append({
            "actor_id": c.get("id"),
            "name": c.get("name"),
            "character": c.get("character"),
            "order": c.get("order"),
            "popularity": c.get("popularity"),
        })



    return {

        "tmdbId": data.get("id"),

        "genres": [g["name"] for g in data.get("genres", [])],

        "origin_country": data.get("origin_country"),

        "overview": data.get("overview"),

        "popularity": data.get("popularity"),

        "poster_path": data.get("poster_path"),

        "spoken_languages": [l["english_name"] for l in data.get("spoken_languages", [])],

        "release_date": data.get("release_date"),

        "actors": actors,

        "directors": directors,

        "keywords": [k["name"] for k in data.get("keywords", {}).get("keywords", [])]
    }


def update_missing_stats(stats, movie):

    for field in FIELDS:

        if not movie.get(field):
            stats[field] += 1


def fetch_full(save_every=100):

    rows = base[["movieId", "tmdbId"]].dropna()

    rows["tmdbId"] = rows["tmdbId"].astype(int)

    records = []

    stats_missing = defaultdict(int)

    total = 0
    success = 0
    failed = 0
    failed_by_status = defaultdict(int)
    failed_tmdb_ids = []

    for row in tqdm(rows.itertuples(index=False), total=len(rows)):

        tmdb_id = int(row.tmdbId)

        try:

            r = fetch_movie(tmdb_id)

            if r.status_code != 200:

                failed += 1
                failed_by_status[r.status_code] += 1
                failed_tmdb_ids.append(tmdb_id)
                continue

            data = r.json()

            movie = clean_movie(data)

            movie["movieId"] = int(row.movieId)

            update_missing_stats(stats_missing, movie)

            records.append(movie)

            success += 1

        except Exception:
            failed += 1
            failed_by_status["exception"] += 1
            failed_tmdb_ids.append(tmdb_id)
            continue

        total += 1

        if total % save_every == 0:

            chunk_path = RAW_DIR / f"tmdb_part_{total}.json"

            with open(chunk_path, "w") as f:
                json.dump(records, f)

            records = []

            print("\n------ STATS AFTER", total, "MOVIES ------")

            for k in FIELDS:

                missing_pct = stats_missing[k] / total

                print(f"{k}: {missing_pct:.3f} missing")

            print("success:", success)
            print("failed:", failed)
            if failed_by_status:
                top = sorted(
                    failed_by_status.items(),
                    key=lambda kv: kv[1],
                    reverse=True,
                )[:8]
                print("failed_by_status (top):", dict(top))

        time.sleep(0.05)

    if records:

        chunk_path = RAW_DIR / f"tmdb_part_final.json"

        with open(chunk_path, "w") as f:
            json.dump(records, f)

    print("\nFINISHED")
    print("success:", success)
    print("failed:", failed)
    if failed_by_status:
        top = sorted(
            failed_by_status.items(),
            key=lambda kv: kv[1],
            reverse=True,
        )[:12]
        print("failed_by_status (top):", dict(top))
    if failed_tmdb_ids:
        print("example_failed_tmdb_ids:", failed_tmdb_ids[:20])


if __name__ == "__main__":

    fetch_full(save_every=1000)