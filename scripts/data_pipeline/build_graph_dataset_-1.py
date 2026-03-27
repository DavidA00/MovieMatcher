from pathlib import Path

import pandas as pd


def _read_if_exists(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def build_graph_dataset(min_actor_frequency: int = 2):
    processed = Path("data/processed")

    movies = pd.read_csv(processed / "movies_final.csv")
    actors = pd.read_csv(processed / "movie_actors.csv")
    genres = pd.read_csv(processed / "movie_genres.csv")
    languages = pd.read_csv(processed / "movie_languages.csv")
    countries = pd.read_csv(processed / "movie_countries.csv")
    keywords = _read_if_exists(processed / "movie_keywords.csv")
    directors = _read_if_exists(processed / "movie_directors.csv")

    # -------------------------
    # select high quality movies
    # -------------------------
    movies = movies[
        (movies["quality_bucket"] == "high")
        & (movies["tmdb_fetch_status"] == "success")
        & (movies["has_any_genre"])
        & (movies["has_overview"])
        & (movies["has_cast"])
    ].copy()

    print("Graph movies:", len(movies))
    movie_ids = set(movies["movieId"])

    # -------------------------
    # filter edges
    # -------------------------
    actors = actors[actors["movieId"].isin(movie_ids)].copy()
    genres = genres[genres["movieId"].isin(movie_ids)].copy()
    languages = languages[languages["movieId"].isin(movie_ids)].copy()
    countries = countries[countries["movieId"].isin(movie_ids)].copy()

    if not keywords.empty and "movieId" in keywords.columns:
        keywords = keywords[keywords["movieId"].isin(movie_ids)].copy()
    if not directors.empty and "movieId" in directors.columns:
        directors = directors[directors["movieId"].isin(movie_ids)].copy()

    print("Actor edges:", len(actors))
    print("Genre edges:", len(genres))

    # -------------------------
    # remove very low frequency actors
    # -------------------------
    actor_counts = actors.groupby("actor_id").size()
    popular_actors = actor_counts[actor_counts >= min_actor_frequency].index
    actors = actors[actors["actor_id"].isin(popular_actors)].copy()
    print("Actors kept:", len(popular_actors))

    # -------------------------
    # save graph tables
    # -------------------------
    out = Path("data/graph")
    out.mkdir(exist_ok=True)

    movies.to_csv(out / "graph_movies.csv", index=False)
    actors.to_csv(out / "graph_movie_actor.csv", index=False)
    genres.to_csv(out / "graph_movie_genre.csv", index=False)
    languages.to_csv(out / "graph_movie_language.csv", index=False)
    countries.to_csv(out / "graph_movie_country.csv", index=False)
    if not keywords.empty:
        keywords.to_csv(out / "graph_movie_keyword.csv", index=False)
    if not directors.empty:
        directors.to_csv(out / "graph_movie_director.csv", index=False)

    print("Graph dataset saved")


def print_graph_summary():
    data = Path("data/graph")

    movies = pd.read_csv(data / "graph_movies.csv")
    actors = pd.read_csv(data / "graph_movie_actor.csv")
    genres = pd.read_csv(data / "graph_movie_genre.csv")
    languages = pd.read_csv(data / "graph_movie_language.csv")
    countries = pd.read_csv(data / "graph_movie_country.csv")
    keywords = _read_if_exists(data / "graph_movie_keyword.csv")
    directors = _read_if_exists(data / "graph_movie_director.csv")

    print("\n===== GRAPH SIZE =====")
    print("Movies:", len(movies))
    print("Actors:", actors["actor_id"].nunique())
    print("Genres:", genres["genre_name"].nunique())
    print("Languages:", languages["language_name"].nunique())
    print("Countries:", countries["country_code"].nunique())
    if not keywords.empty and "keyword_name" in keywords.columns:
        print("Keywords:", keywords["keyword_name"].nunique())
    if not directors.empty and "director_id" in directors.columns:
        print("Directors:", directors["director_id"].nunique())

    print("\n===== EDGE COUNTS =====")
    print("Movie-Actor edges:", len(actors))
    print("Movie-Genre edges:", len(genres))
    print("Movie-Language edges:", len(languages))
    print("Movie-Country edges:", len(countries))
    if not keywords.empty:
        print("Movie-Keyword edges:", len(keywords))
    if not directors.empty:
        print("Movie-Director edges:", len(directors))

    print("\n===== MOVIE DEGREE =====")
    movie_actor_deg = actors.groupby("movieId").size()
    movie_genre_deg = genres.groupby("movieId").size()
    print("Avg actors per movie:", movie_actor_deg.mean())
    print("Avg genres per movie:", movie_genre_deg.mean())

    print("\n===== ACTOR HUBS =====")
    actor_deg = actors.groupby("actor_id").size()
    print("Max actor degree:", actor_deg.max())
    print("\nTop 10 actors by degree:")
    print(actor_deg.sort_values(ascending=False).head(10))

    print("\n===== GENRE HUBS =====")
    genre_deg = genres.groupby("genre_name").size()
    print("Max genre degree:", genre_deg.max())
    print("\nGenres by degree:")
    print(genre_deg.sort_values(ascending=False))

    print("\n===== MOVIE DEGREE DISTRIBUTION =====")
    movie_total_deg = movie_actor_deg.add(movie_genre_deg, fill_value=0)
    print(movie_total_deg.describe())
    print("\nTop 10 highest-degree movies:")
    print(movie_total_deg.sort_values(ascending=False).head(10))


if __name__ == "__main__":
    build_graph_dataset()
    print_graph_summary()

