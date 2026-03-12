import re
import numpy as np
import pandas as pd

movies = pd.read_csv("data/movielens/movies.csv")
ratings = pd.read_csv("data/movielens/ratings.csv")
links = pd.read_csv("data/movielens/links.csv")
tags = pd.read_csv("data/movielens/tags.csv")


def extract_year(title: str):
    if pd.isna(title):
        return np.nan
    m = re.search(r"\((\d{4})\)\s*$", title)
    return int(m.group(1)) if m else np.nan


def remove_year_suffix(title: str):
    if pd.isna(title):
        return title
    return re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()


# ratings summary
ratings_summary = (
    ratings.groupby("movieId")["rating"]
    .agg(rating_count="count", avg_rating_raw="mean")
    .reset_index()
)

ratings_summary["avg_rating"] = ratings_summary["avg_rating_raw"]
ratings_summary.loc[ratings_summary["rating_count"] < 10, "avg_rating"] = np.nan

# tag summary, movie-level only
tags_clean = tags.copy()
tags_clean["tag"] = tags_clean["tag"].astype(str).str.strip().str.lower()
tags_clean = tags_clean[tags_clean["tag"] != ""]

tag_counts = (
    tags_clean.groupby(["movieId", "tag"])
    .size()
    .reset_index(name="tag_count")
)

tag_summary = (
    tag_counts.groupby("movieId")
    .agg(
        unique_tag_count=("tag", "nunique"),
        total_tag_assignments=("tag_count", "sum"),
    )
    .reset_index()
)

top_tags = (
    tag_counts.sort_values(["movieId", "tag_count", "tag"], ascending=[True, False, True])
    .groupby("movieId")
    .head(25)
    .groupby("movieId")
    .apply(lambda df: df[["tag", "tag_count"]].to_dict("records"))
    .rename("top_tags")
    .reset_index()
)

# base join
base = movies.merge(links, on="movieId", how="left")
base = base.merge(ratings_summary, on="movieId", how="left")
base = base.merge(tag_summary, on="movieId", how="left")
base = base.merge(top_tags, on="movieId", how="left")

# keep only movies with tmdbId
base = base[base["tmdbId"].notna()].copy()

# normalize ids
base["tmdbId"] = base["tmdbId"].astype("Int64")
base["imdbId"] = base["imdbId"].astype("Int64")

# title/year fields
base["title_year"] = base["title"]
base["title_clean"] = base["title"].apply(remove_year_suffix)
base["year_movielens"] = base["title"].apply(extract_year)

# genres
base["has_movielens_genres"] = base["genres"].fillna("(no genres listed)") != "(no genres listed)"
base["genres_list"] = base["genres"].fillna("").apply(
    lambda x: [] if x in ["", "(no genres listed)"] else [g.strip() for g in x.split("|")]
)

# data completeness flags
base["has_ratings"] = base["rating_count"].fillna(0) > 0
base["has_reliable_rating"] = base["rating_count"].fillna(0) >= 10
base["has_tags"] = base["unique_tag_count"].fillna(0) > 0

# fill missing numeric summaries
base["rating_count"] = base["rating_count"].fillna(0).astype(int)
base["unique_tag_count"] = base["unique_tag_count"].fillna(0).astype(int)
base["total_tag_assignments"] = base["total_tag_assignments"].fillna(0).astype(int)

# useful ordering
cols = [
    "movieId",
    "tmdbId",
    "imdbId",
    "title_year",
    "title_clean",
    "year_movielens",
    "genres",
    "genres_list",
    "has_movielens_genres",
    "rating_count",
    "avg_rating",
    "avg_rating_raw",
    "has_ratings",
    "has_reliable_rating",
    "unique_tag_count",
    "total_tag_assignments",
    "has_tags",
    "top_tags",
]
base = base[cols]

print("Movies kept (tmdbId exists):", len(base))
print("Movies with reliable avg rating (>=10 ratings):", base["has_reliable_rating"].sum())
print("Movies with no MovieLens genres:", (~base["has_movielens_genres"]).sum())
print("Movies with tags:", base["has_tags"].sum())

print("\nSample rows:")
print(base.head())

base.to_pickle("data/movie_base.pkl")
base.to_csv("data/movie_base.csv", index=False)
