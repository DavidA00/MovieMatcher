import os
import networkx as nx
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def fetch_neighborhood(movie_title, limit=200):

    query = """
    MATCH (m:Movie {title:$title})

    OPTIONAL MATCH (m)-[:HAS_ACTOR]->(a:Actor)
    OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
    OPTIONAL MATCH (m)-[:SPOKEN_LANGUAGE]->(l:Language)
    OPTIONAL MATCH (m)-[:ORIGIN_COUNTRY]->(c:Country)
    OPTIONAL MATCH (m)-[:HAS_KEYWORD]->(k:Keyword)

    RETURN m,a,g,l,c,k
    LIMIT $limit
    """

    with driver.session() as session:

        result = session.run(query, title=movie_title, limit=limit)

        nodes = []
        edges = []

        for r in result:

            movie = r["m"]["title"]

            nodes.append(("movie", movie))

            if r["a"]:
                actor = r["a"]["name"]
                nodes.append(("actor", actor))
                edges.append((movie, actor))

            if r["g"]:
                genre = r["g"]["name"]
                nodes.append(("genre", genre))
                edges.append((movie, genre))

            if r["l"]:
                lang = r["l"]["name"]
                nodes.append(("language", lang))
                edges.append((movie, lang))

            if r["c"]:
                country = r["c"]["code"]
                nodes.append(("country", country))
                edges.append((movie, country))

            if r["k"]:
                keyword = r["k"]["name"]
                nodes.append(("keyword", keyword))
                edges.append((movie, keyword))

        return nodes, edges


def visualize(movie_title):

    nodes, edges = fetch_neighborhood(movie_title)

    G = nx.Graph()

    for t, n in nodes:
        G.add_node(n, type=t)

    for u, v in edges:
        G.add_edge(u, v)

    pos = nx.spring_layout(G, k=0.6)

    colors = []

    for node in G.nodes:

        t = G.nodes[node]["type"]

        if t == "movie":
            colors.append("red")

        elif t == "actor":
            colors.append("skyblue")

        elif t == "genre":
            colors.append("green")

        elif t == "language":
            colors.append("orange")

        elif t == "country":
            colors.append("purple")

        elif t == "keyword":
            colors.append("yellow")

        else:
            colors.append("grey")

    plt.figure(figsize=(12,10))

    nx.draw(
        G,
        pos,
        node_color=colors,
        with_labels=True,
        node_size=900,
        font_size=8
    )

    plt.title(movie_title)

    plt.show()


if __name__ == "__main__":

    visualize("Inception")