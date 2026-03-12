import pandas as pd
from pathlib import Path

DATA = Path("data/graph")

movies = pd.read_csv(DATA / "graph_movies.csv")
actors = pd.read_csv(DATA / "graph_movie_actor.csv")
genres = pd.read_csv(DATA / "graph_movie_genre.csv")

movie_titles = dict(zip(movies["movieId"], movies["title_clean"]))

# precompute feature sets

movie_features = {}

for movie_id in movies["movieId"]:

    movie_actors = set(
        actors[actors["movieId"] == movie_id]["actor_id"]
    )

    movie_genres = set(
        genres[genres["movieId"] == movie_id]["genre_name"]
    )

    movie_features[movie_id] = movie_actors | movie_genres


def jaccard(a, b):

    if len(a | b) == 0:
        return 0

    return len(a & b) / len(a | b)


def find_similar(movie_name, top_k=10):

    row = movies[movies["title_clean"].str.contains(movie_name, case=False)]

    if len(row) == 0:
        print("Movie not found")
        return

    movie_id = row.iloc[0]["movieId"]

    target_features = movie_features[movie_id]

    sims = []

    for other_id, features in movie_features.items():

        if other_id == movie_id:
            continue

        s = jaccard(target_features, features)

        sims.append((other_id, s))

    sims.sort(key=lambda x: x[1], reverse=True)

    print("\nSimilar movies to:", movie_titles[movie_id])
    print()

    for mid, score in sims[:top_k]:

        print(f"{movie_titles[mid]}  ({score:.3f})")


if __name__ == "__main__":

    find_similar("Inception")
