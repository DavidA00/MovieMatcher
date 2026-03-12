import json
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd


DATA_DIR = Path("data")
TMDB_PARTS_DIR = DATA_DIR / "tmdb_clean_parts"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Config
# ----------------------------

TMDB_GENRE_MAP = {
    "Sci-Fi": "Science Fiction",
}

MOVIELENS_GENRE_MAP = {
    "Sci-Fi": "Science Fiction",
    "Film-Noir": "Film Noir",
    "(no genres listed)": None,
}

TOP_CAST_LIMIT = 10

# change to an integer like 1000 for faster testing
MAX_TMDB_PARTS = None


# ----------------------------
# Helpers
# ----------------------------

def normalize_text(value):
    if value is None:
        return None
    if pd.isna(value):
        return None
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)
    return value if value else None


def normalize_text_for_match(value):
    value = normalize_text(value)
    if value is None:
        return None
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return value.casefold()


def normalize_genre_name(name, source="tmdb"):
    name = normalize_text(name)
    if name is None:
        return None

    if source == "tmdb":
        name = TMDB_GENRE_MAP.get(name, name)
    else:
        name = MOVIELENS_GENRE_MAP.get(name, name)

    return name


def parse_year_from_release_date(release_date):
    release_date = normalize_text(release_date)
    if not release_date:
        return np.nan
    m = re.match(r"^(\d{4})", release_date)
    return int(m.group(1)) if m else np.nan


def safe_list(x):
    if isinstance(x, list):
        return x
    return []


def quality_bucket(score):
    if score >= 0.85:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


# ----------------------------
# Load MovieLens base
# ----------------------------

print("Loading movie_base.pkl ...")
base = pd.read_pickle(DATA_DIR / "movie_base.pkl").copy()

base["movieId"] = base["movieId"].astype(int)
base["tmdbId"] = pd.to_numeric(base["tmdbId"], errors="coerce").astype("Int64")
base["imdbId"] = pd.to_numeric(base["imdbId"], errors="coerce").astype("Int64")

# keep everything with tmdbId; that was your decision
base = base[base["tmdbId"].notna()].copy()

base["title_clean"] = base["title_clean"].apply(normalize_text)
base["title_year"] = base["title_year"].apply(normalize_text)

print(f"Movie base rows kept: {len(base)}")


# ----------------------------
# Load TMDb partial dumps
# ----------------------------

print("Loading TMDb part files ...")
part_files = sorted(TMDB_PARTS_DIR.glob("tmdb_part_*.json"))
if MAX_TMDB_PARTS is not None:
    part_files = part_files[:MAX_TMDB_PARTS]

print(f"TMDb part files found: {len(part_files)}")

tmdb_records = []
for fp in part_files:
    with open(fp, "r", encoding="utf-8") as f:
        rows = json.load(f)
        if isinstance(rows, list):
            tmdb_records.extend(rows)

print(f"TMDb records loaded: {len(tmdb_records)}")


# ----------------------------
# Normalize TMDb records into tables
# ----------------------------

movie_rows = []
genre_rows = []
actor_rows = []
director_rows = []
language_rows = []
country_rows = []
keyword_rows = []
fetch_rows = []

for rec in tmdb_records:
    movie_id = rec.get("movieId")
    tmdb_id = rec.get("tmdbId")

    if movie_id is None or tmdb_id is None:
        continue

    movie_id = int(movie_id)
    tmdb_id = int(tmdb_id)

    genres = safe_list(rec.get("genres"))
    countries = safe_list(rec.get("origin_country"))
    languages = safe_list(rec.get("spoken_languages"))
    actors = safe_list(rec.get("actors"))
    directors = safe_list(rec.get("directors"))
    keywords = safe_list(rec.get("keywords"))

    overview = normalize_text(rec.get("overview"))
    poster_path = normalize_text(rec.get("poster_path"))
    popularity = rec.get("popularity")
    release_date = normalize_text(rec.get("release_date"))

    movie_rows.append({
        "movieId": movie_id,
        "tmdbId": tmdb_id,
        "overview_tmdb": overview,
        "poster_path": poster_path,
        "popularity_tmdb": popularity,
        "release_date": release_date,
        "origin_country_raw": countries,
        "spoken_languages_raw": languages,
        "genres_tmdb_raw": genres,
        "keywords_tmdb_raw": keywords,
        "actors_raw_count": len(actors),
        "directors_raw_count": len(directors),
        "keywords_raw_count": len(keywords),
        "has_tmdb_overview": overview is not None,
        "has_tmdb_poster": poster_path is not None,
        "has_tmdb_genres": len(genres) > 0,
        "has_tmdb_languages": len(languages) > 0,
        "has_tmdb_countries": len(countries) > 0,
        "has_tmdb_cast": len(actors) > 0,
        "has_tmdb_director": len(directors) > 0,
        "has_tmdb_keywords": len(keywords) > 0,
        "tmdb_fetch_status": "success",
    })

    fetch_rows.append({
        "movieId": movie_id,
        "tmdbId": tmdb_id,
        "tmdb_fetch_status": "success",
    })

    for g in genres:
        g_norm = normalize_genre_name(g, source="tmdb")
        if g_norm is not None:
            genre_rows.append({
                "movieId": movie_id,
                "tmdbId": tmdb_id,
                "genre_name": g_norm,
                "genre_name_norm": normalize_text_for_match(g_norm),
                "genre_source": "tmdb",
            })

    for country_code in countries:
        country_code = normalize_text(country_code)
        if country_code:
            country_rows.append({
                "movieId": movie_id,
                "tmdbId": tmdb_id,
                "country_code": country_code,
                "country_code_norm": normalize_text_for_match(country_code),
            })

    for lang in languages:
        lang = normalize_text(lang)
        if lang:
            language_rows.append({
                "movieId": movie_id,
                "tmdbId": tmdb_id,
                "language_name": lang,
                "language_name_norm": normalize_text_for_match(lang),
            })

    for actor in actors[:TOP_CAST_LIMIT]:
        actor_id = actor.get("actor_id")
        actor_name = normalize_text(actor.get("name"))
        character = normalize_text(actor.get("character"))
        cast_order = actor.get("order")
        actor_popularity = actor.get("popularity")

        if actor_id is None or actor_name is None:
            continue

        actor_rows.append({
            "movieId": movie_id,
            "tmdbId": tmdb_id,
            "actor_id": int(actor_id),
            "actor_name": actor_name,
            "actor_name_norm": normalize_text_for_match(actor_name),
            "character": character,
            "cast_order": cast_order,
            "actor_popularity": actor_popularity,
        })

    for d in directors:
        director_id = d.get("director_id")
        director_name = normalize_text(d.get("name"))

        if director_id is None or director_name is None:
            continue

        director_rows.append({
            "movieId": movie_id,
            "tmdbId": tmdb_id,
            "director_id": int(director_id),
            "director_name": director_name,
            "director_name_norm": normalize_text_for_match(director_name),
        })

    for kw in keywords:
        kw = normalize_text(kw)
        if kw:
            keyword_rows.append({
                "movieId": movie_id,
                "tmdbId": tmdb_id,
                "keyword_name": kw,
                "keyword_name_norm": normalize_text_for_match(kw),
            })

movies_tmdb = pd.DataFrame(movie_rows).drop_duplicates(subset=["movieId", "tmdbId"])
movie_genres_tmdb = pd.DataFrame(genre_rows).drop_duplicates()
movie_countries = pd.DataFrame(country_rows).drop_duplicates()
movie_languages = pd.DataFrame(language_rows).drop_duplicates()
movie_actors = pd.DataFrame(actor_rows).drop_duplicates()
movie_directors = pd.DataFrame(director_rows).drop_duplicates()
movie_keywords = pd.DataFrame(keyword_rows).drop_duplicates()
tmdb_fetch_status = pd.DataFrame(fetch_rows).drop_duplicates()

print(f"TMDb movie rows: {len(movies_tmdb)}")
print(f"TMDb genre edges: {len(movie_genres_tmdb)}")
print(f"TMDb actor edges: {len(movie_actors)}")
print(f"TMDb director edges: {len(movie_directors)}")
print(f"TMDb keyword edges: {len(movie_keywords)}")
print(f"TMDb language edges: {len(movie_languages)}")
print(f"TMDb country edges: {len(movie_countries)}")


# ----------------------------
# Track TMDb failures too
# ----------------------------

success_movie_ids = set(movies_tmdb["movieId"].astype(int).tolist())
all_expected = base[["movieId", "tmdbId"]].copy()
all_expected["movieId"] = all_expected["movieId"].astype(int)
all_expected["tmdbId"] = all_expected["tmdbId"].astype(int)

failed_expected = all_expected[~all_expected["movieId"].isin(success_movie_ids)].copy()
failed_expected["tmdb_fetch_status"] = "missing_or_failed"

tmdb_fetch_status = pd.concat(
    [
        tmdb_fetch_status,
        failed_expected[["movieId", "tmdbId", "tmdb_fetch_status"]]
    ],
    ignore_index=True
).drop_duplicates(subset=["movieId", "tmdbId"], keep="first")

print(f"TMDb success movies: {len(success_movie_ids)}")
print(f"TMDb missing/failed movies: {len(failed_expected)}")


# ----------------------------
# Normalize MovieLens genres
# ----------------------------

ml_genre_rows = []
for row in base[["movieId", "tmdbId", "genres_list"]].itertuples(index=False):
    for g in safe_list(row.genres_list):
        g_norm = normalize_genre_name(g, source="movielens")
        if g_norm is not None:
            ml_genre_rows.append({
                "movieId": int(row.movieId),
                "tmdbId": int(row.tmdbId),
                "genre_name": g_norm,
                "genre_name_norm": normalize_text_for_match(g_norm),
                "genre_source": "movielens",
            })

movie_genres_ml = pd.DataFrame(ml_genre_rows).drop_duplicates()

print(f"MovieLens genre edges: {len(movie_genres_ml)}")


# ----------------------------
# Merge movie-level table
# ----------------------------

movies_final = base.merge(
    movies_tmdb,
    on=["movieId", "tmdbId"],
    how="left"
)

movies_final = movies_final.merge(
    tmdb_fetch_status,
    on=["movieId", "tmdbId"],
    how="left",
    suffixes=("", "_status")
)

movies_final["year_tmdb"] = np.nan
if "release_date" in movies_final.columns:
    movies_final["year_tmdb"] = movies_final["release_date"].apply(parse_year_from_release_date)

# If your current TMDb fetcher does not store release_date yet, keep this resilient
if "release_date" not in movies_final.columns:
    movies_final["release_date"] = None
    movies_final["year_tmdb"] = np.nan

movies_final["year_final"] = movies_final["year_tmdb"]
movies_final.loc[movies_final["year_final"].isna(), "year_final"] = movies_final["year_movielens"]

movies_final["year_mismatch_flag"] = (
    movies_final["year_tmdb"].notna() &
    movies_final["year_movielens"].notna() &
    (movies_final["year_tmdb"] != movies_final["year_movielens"])
)

movies_final["has_any_genre"] = (
    movies_final["has_movielens_genres"].fillna(False) |
    movies_final["has_tmdb_genres"].fillna(False)
)

movies_final["has_overview"] = movies_final["has_tmdb_overview"].fillna(False)
movies_final["has_poster"] = movies_final["has_tmdb_poster"].fillna(False)
movies_final["has_languages"] = movies_final["has_tmdb_languages"].fillna(False)
movies_final["has_countries"] = movies_final["has_tmdb_countries"].fillna(False)
movies_final["has_cast"] = movies_final["has_tmdb_cast"].fillna(False)
movies_final["has_director"] = movies_final.get("has_tmdb_director", False)
movies_final["has_keywords"] = movies_final.get("has_tmdb_keywords", False)

score_components = [
    "has_overview",
    "has_poster",
    "has_any_genre",
    "has_cast",
    "has_director",
    "has_keywords",
    "has_languages",
    "has_countries",
    "has_reliable_rating",
]
movies_final["data_completeness_score"] = movies_final[score_components].astype(float).mean(axis=1)
movies_final["quality_bucket"] = movies_final["data_completeness_score"].apply(quality_bucket)

movies_final["exclude_from_main_candidate_pool"] = (
    ~movies_final["has_overview"] |
    ~movies_final["has_any_genre"] |
    ~movies_final["has_poster"] |
    (movies_final["tmdb_fetch_status"] != "success")
)

movies_final["title_match_norm"] = movies_final["title_clean"].apply(normalize_text_for_match)

# drop bulky raw columns if present
for col in ["origin_country_raw", "spoken_languages_raw", "genres_tmdb_raw"]:
    if col not in movies_final.columns:
        movies_final[col] = None

# keep a clean subset
movie_cols = [
    "movieId",
    "tmdbId",
    "imdbId",
    "title_year",
    "title_clean",
    "year_movielens",
    "release_date",
    "year_tmdb",
    "year_final",
    "year_mismatch_flag",
    "rating_count",
    "avg_rating",
    "avg_rating_raw",
    "has_ratings",
    "has_reliable_rating",
    "unique_tag_count",
    "total_tag_assignments",
    "has_tags",
    "top_tags",
    "has_movielens_genres",
    "has_tmdb_genres",
    "has_any_genre",
    "has_overview",
    "has_poster",
    "has_languages",
    "has_countries",
    "has_cast",
    "has_director",
    "has_keywords",
    "data_completeness_score",
    "quality_bucket",
    "tmdb_fetch_status",
    "exclude_from_main_candidate_pool",
    "overview_tmdb",
    "poster_path",
    "popularity_tmdb",
]
movie_cols = [c for c in movie_cols if c in movies_final.columns]
movies_final = movies_final[movie_cols].copy()


# ----------------------------
# Merge genre edges
# ----------------------------

movie_genres = pd.concat([movie_genres_ml, movie_genres_tmdb], ignore_index=True)
movie_genres = movie_genres.drop_duplicates(subset=["movieId", "genre_name_norm", "genre_source"])

# Also create merged unique movie-genre edges
movie_genres_merged = (
    movie_genres[["movieId", "tmdbId", "genre_name", "genre_name_norm"]]
    .drop_duplicates(subset=["movieId", "genre_name_norm"])
    .copy()
)


# ----------------------------
# Directors placeholder
# ----------------------------

movie_directors = pd.DataFrame(columns=[
    "movieId",
    "tmdbId",
    "director_id",
    "director_name",
    "director_name_norm",
])


# ----------------------------
# Save outputs
# ----------------------------

movies_final.to_csv(PROCESSED_DIR / "movies_final.csv", index=False)
movie_genres.to_csv(PROCESSED_DIR / "movie_genres_with_source.csv", index=False)
movie_genres_merged.to_csv(PROCESSED_DIR / "movie_genres.csv", index=False)
movie_actors.to_csv(PROCESSED_DIR / "movie_actors.csv", index=False)
movie_directors.to_csv(PROCESSED_DIR / "movie_directors.csv", index=False)
movie_languages.to_csv(PROCESSED_DIR / "movie_languages.csv", index=False)
movie_countries.to_csv(PROCESSED_DIR / "movie_countries.csv", index=False)
movie_keywords.to_csv(PROCESSED_DIR / "movie_keywords.csv", index=False)
tmdb_fetch_status.to_csv(PROCESSED_DIR / "tmdb_fetch_status.csv", index=False)

print("\nSaved processed files to data/processed/")
print("movies_final.csv")
print("movie_genres_with_source.csv")
print("movie_genres.csv")
print("movie_actors.csv")
print("movie_directors.csv")
print("movie_languages.csv")
print("movie_countries.csv")
print("movie_keywords.csv")
print("tmdb_fetch_status.csv")