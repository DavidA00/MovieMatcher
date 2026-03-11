#!/usr/bin/env python3
"""
Movie Knowledge Base Demo — Neo4j

Demonstrates creating and querying a movie graph in Neo4j:
- Nodes: Movie, Person (actors/directors), Genre
- Relationships: ACTED_IN, DIRECTED, IN_GENRE

Usage:
  Set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD in environment, or use defaults.
  Run: python movie_knowledge_base.py
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load .env from project directory
load_dotenv(Path(__file__).resolve().parent / ".env")

NEO4J_URI = os.environ.get("NEO4J_URI", "neo4j://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER") or os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def run_demo():
    driver = get_driver()
    try:
        driver.verify_connectivity()
        print("Connected to Neo4j.\n")
    except Exception as e:
        print(f"Cannot connect to Neo4j: {e}")
        print("Ensure Neo4j is running and NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD are set.")
        return

    with driver.session() as session:
        # 1. Clear and create schema
        clear_and_create_schema(session)
        # 2. Insert sample data
        insert_sample_data(session)
        # 3. Run example queries
        run_queries(session)

    driver.close()
    print("\nDemo finished.")


def clear_and_create_schema(session):
    """Remove existing data and create constraints/indexes for the movie graph."""
    print("--- Schema: clearing and creating constraints ---")
    session.run("MATCH (n) DETACH DELETE n")
    session.run("""
        CREATE CONSTRAINT movie_id IF NOT EXISTS
        FOR (m:Movie) REQUIRE m.title IS UNIQUE
    """)
    session.run("""
        CREATE CONSTRAINT person_name IF NOT EXISTS
        FOR (p:Person) REQUIRE p.name IS UNIQUE
    """)
    session.run("""
        CREATE CONSTRAINT genre_name IF NOT EXISTS
        FOR (g:Genre) REQUIRE g.name IS UNIQUE
    """)
    print("Constraints created (Movie.title, Person.name, Genre.name unique).\n")


def insert_sample_data(session):
    """Insert movies, people, genres and relationships."""
    print("--- Inserting sample data ---")

    # Genres
    session.run("""
        UNWIND ['Action', 'Sci-Fi', 'Drama', 'Comedy', 'Thriller', 'Romance'] AS name
        MERGE (g:Genre {name: name})
    """)

    # People and Movies with relationships
    data = [
        {
            "movie": {"title": "The Matrix", "year": 1999},
            "director": "Lana Wachowski",
            "actors": ["Keanu Reeves", "Laurence Fishburne", "Carrie-Anne Moss"],
            "genres": ["Action", "Sci-Fi"],
        },
        {
            "movie": {"title": "Inception", "year": 2010},
            "director": "Christopher Nolan",
            "actors": ["Leonardo DiCaprio", "Tom Hardy", "Elliot Page"],
            "genres": ["Sci-Fi", "Thriller"],
        },
        {
            "movie": {"title": "The Dark Knight", "year": 2008},
            "director": "Christopher Nolan",
            "actors": ["Christian Bale", "Heath Ledger", "Gary Oldman"],
            "genres": ["Action", "Drama", "Thriller"],
        },
        {
            "movie": {"title": "La La Land", "year": 2016},
            "director": "Damien Chazelle",
            "actors": ["Ryan Gosling", "Emma Stone"],
            "genres": ["Drama", "Romance", "Comedy"],
        },
        {
            "movie": {"title": "John Wick", "year": 2014},
            "director": "Chad Stahelski",
            "actors": ["Keanu Reeves", "Willem Dafoe"],
            "genres": ["Action", "Thriller"],
        },
    ]

    for item in data:
        session.run(
            """
            MERGE (m:Movie {title: $title})
            SET m.year = $year
            WITH m
            MERGE (d:Person {name: $director})
            MERGE (d)-[:DIRECTED]->(m)
            WITH m
            UNWIND $actors AS actorName
            MERGE (a:Person {name: actorName})
            MERGE (a)-[:ACTED_IN]->(m)
            WITH m
            UNWIND $genres AS genreName
            MERGE (g:Genre {name: genreName})
            MERGE (m)-[:IN_GENRE]->(g)
            """,
            title=item["movie"]["title"],
            year=item["movie"]["year"],
            director=item["director"],
            actors=item["actors"],
            genres=item["genres"],
        )

    print("Inserted 5 movies with directors, actors, and genres.\n")


def run_queries(session):
    """Run example Cypher queries on the movie graph."""
    print("--- Example queries ---\n")

    # 1. All movies
    print("1. All movies (title, year):")
    result = session.run("MATCH (m:Movie) RETURN m.title AS title, m.year AS year ORDER BY m.year")
    for record in result:
        print(f"   {record['title']} ({record['year']})")

    # 2. Movies by genre
    print("\n2. Sci-Fi movies:")
    result = session.run("""
        MATCH (m:Movie)-[:IN_GENRE]->(g:Genre {name: 'Sci-Fi'})
        RETURN m.title AS title ORDER BY m.title
    """)
    for record in result:
        print(f"   {record['title']}")

    # 3. Movies by actor
    print("\n3. Movies with Keanu Reeves:")
    result = session.run("""
        MATCH (p:Person {name: 'Keanu Reeves'})-[:ACTED_IN]->(m:Movie)
        RETURN m.title AS title, m.year AS year ORDER BY m.year
    """)
    for record in result:
        print(f"   {record['title']} ({record['year']})")

    # 4. Directors and their movies
    print("\n4. Christopher Nolan's films:")
    result = session.run("""
        MATCH (p:Person {name: 'Christopher Nolan'})-[:DIRECTED]->(m:Movie)
        RETURN m.title AS title, m.year AS year ORDER BY m.year
    """)
    for record in result:
        print(f"   {record['title']} ({record['year']})")

    # 5. Co-actors (people who acted in the same movie as Keanu Reeves)
    print("\n5. Co-actors of Keanu Reeves (shared movies):")
    result = session.run("""
        MATCH (keanu:Person {name: 'Keanu Reeves'})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(other:Person)
        WHERE other <> keanu
        RETURN other.name AS co_actor, m.title AS movie
        ORDER BY co_actor, movie
    """)
    for record in result:
        print(f"   {record['co_actor']} — {record['movie']}")

    # 6. Movie with full details (one example)
    print("\n6. Full details for 'Inception':")
    result = session.run("""
        MATCH (m:Movie {title: 'Inception'})
        OPTIONAL MATCH (m)-[:IN_GENRE]->(g:Genre)
        OPTIONAL MATCH (d:Person)-[:DIRECTED]->(m)
        OPTIONAL MATCH (a:Person)-[:ACTED_IN]->(m)
        RETURN m.title AS title, m.year AS year,
               collect(DISTINCT d.name) AS directors,
               collect(DISTINCT a.name) AS actors,
               collect(DISTINCT g.name) AS genres
    """)
    rec = result.single()
    if rec:
        print(f"   Title: {rec['title']} ({rec['year']})")
        print(f"   Directors: {', '.join(rec['directors'])}")
        print(f"   Actors: {', '.join(rec['actors'])}")
        print(f"   Genres: {', '.join(rec['genres'])}")


if __name__ == "__main__":
    run_demo()
