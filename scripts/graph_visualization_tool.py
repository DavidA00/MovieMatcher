import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path

DATA = Path("data/graph")

movies = pd.read_csv(DATA / "graph_movies.csv")
actors = pd.read_csv(DATA / "graph_movie_actor.csv")
genres = pd.read_csv(DATA / "graph_movie_genre.csv")

title_lookup = dict(zip(movies["movieId"], movies["title_clean"]))

def visualize_movie(movie_name):

    movie_row = movies[movies["title_clean"].str.contains(movie_name, case=False)]

    if len(movie_row) == 0:
        print("Movie not found")
        return

    movie_id = movie_row.iloc[0]["movieId"]
    movie_title = movie_row.iloc[0]["title_clean"]

    print("Visualizing:", movie_title)

    G = nx.Graph()

    G.add_node(movie_title, type="movie")

    # actors
    movie_actors = actors[actors["movieId"] == movie_id]

    for _, row in movie_actors.iterrows():

        actor = row["actor_name"]

        G.add_node(actor, type="actor")
        G.add_edge(movie_title, actor)

    # genres
    movie_genres = genres[genres["movieId"] == movie_id]

    for _, row in movie_genres.iterrows():

        genre = row["genre_name"]

        G.add_node(genre, type="genre")
        G.add_edge(movie_title, genre)

    pos = nx.spring_layout(G, seed=42)

    colors = []

    for node in G.nodes:

        t = G.nodes[node]["type"]

        if t == "movie":
            colors.append("red")
        elif t == "actor":
            colors.append("lightblue")
        else:
            colors.append("green")

    plt.figure(figsize=(10,8))

    nx.draw(
        G,
        pos,
        with_labels=True,
        node_color=colors,
        node_size=1200,
        font_size=8
    )

    plt.title(movie_title)
    plt.show()


if __name__ == "__main__":

    visualize_movie("Inception")