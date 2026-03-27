import pandas as pd
from pathlib import Path

PROCESSED_DIR = Path("data/processed")

movies = pd.read_csv(PROCESSED_DIR / "movies_final.csv")
genres = pd.read_csv(PROCESSED_DIR / "movie_genres.csv")
actors = pd.read_csv(PROCESSED_DIR / "movie_actors.csv")
directors = pd.read_csv(PROCESSED_DIR / "movie_directors.csv")
languages = pd.read_csv(PROCESSED_DIR / "movie_languages.csv")
countries = pd.read_csv(PROCESSED_DIR / "movie_countries.csv")
fetch = pd.read_csv(PROCESSED_DIR / "tmdb_fetch_status.csv")

print("=== MOVIES ===")
print("movies:", len(movies))
print("tmdb success share:", (movies["tmdb_fetch_status"] == "success").mean())
print("has overview:", movies["has_overview"].mean())
print("has poster:", movies["has_poster"].mean())
print("has any genre:", movies["has_any_genre"].mean())
print("has cast:", movies["has_cast"].mean())
print("has reliable rating:", movies["has_reliable_rating"].mean())
print("excluded from main pool:", movies["exclude_from_main_candidate_pool"].mean())

print("\nquality buckets:")
print(movies["quality_bucket"].value_counts(dropna=False))

print("\n=== EDGE TABLE SIZES ===")
print("movie_genres:", len(genres))
print("movie_actors:", len(actors))
print("movie_directors:", len(directors))
print("movie_languages:", len(languages))
print("movie_countries:", len(countries))

print("\n=== DEGREE STATS ===")
if len(actors) > 0:
    actor_deg = actors.groupby("actor_id")["movieId"].nunique()
    print("actors unique:", actor_deg.shape[0])
    print("max actor degree:", actor_deg.max())
    print("median actor degree:", actor_deg.median())

genre_deg = genres.groupby("genre_name_norm")["movieId"].nunique()
print("genres unique:", genre_deg.shape[0])
print("max genre degree:", genre_deg.max())
print("median genre degree:", genre_deg.median())

print("\n=== TMDB FAILURES ===")
print(fetch["tmdb_fetch_status"].value_counts(dropna=False))

failed = fetch[fetch["tmdb_fetch_status"] != "success"].copy()
print("\nSample failed:")
print(failed.head(20))