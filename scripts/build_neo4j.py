import os
import sys
import logging
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = os.getenv("MOVIEMATCHER_LOG_FILE", str(LOG_DIR / "neo4j_load.log"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("neo4j_loader")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# Fail fast with a clear error if credentials are wrong
try:
    driver.verify_connectivity()
except Exception as e:
    log.error("Neo4j connection failed. Check NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD in .env")
    log.error("For Aura, username is usually 'neo4j'. Reset password in console.neo4j.io if needed.")
    raise

DATA = "data/graph"

movies = pd.read_csv(f"{DATA}/graph_movies.csv")
actors = pd.read_csv(f"{DATA}/graph_movie_actor.csv")
genres = pd.read_csv(f"{DATA}/graph_movie_genre.csv")
languages = pd.read_csv(f"{DATA}/graph_movie_language.csv")
countries = pd.read_csv(f"{DATA}/graph_movie_country.csv")
keywords_path = Path(DATA) / "graph_movie_keyword.csv"
directors_path = Path(DATA) / "graph_movie_director.csv"
keywords = pd.read_csv(keywords_path) if keywords_path.exists() else pd.DataFrame()
directors = pd.read_csv(directors_path) if directors_path.exists() else pd.DataFrame()

def run_query_in_batches(
    query: str,
    rows: list[dict],
    stage: str,
    batch_size: int = 5000,
    skip_first: int = 0,
):
    if not rows:
        log.info("[%s] 0 rows, skipping", stage)
        return

    if skip_first < 0:
        raise ValueError(f"skip_first must be >= 0 (got {skip_first})")

    if skip_first:
        if skip_first >= len(rows):
            log.info(
                "[%s] skip_first=%s >= total=%s, nothing to load",
                stage,
                skip_first,
                len(rows),
            )
            return
        rows = rows[skip_first:]

    total = len(rows)
    log.info(
        "[%s] loading %s rows (batch_size=%s, skip_first=%s)",
        stage,
        total,
        batch_size,
        skip_first,
    )

    with driver.session() as session:
        for start in range(0, total, batch_size):
            batch = rows[start : start + batch_size]
            session.execute_write(lambda tx: tx.run(query, rows=batch))
            log.info("[%s] %s/%s", stage, min(start + batch_size, total), total)

# -------------------
# Movies (nodes)
# -------------------

movie_cols = [c for c in ["movieId", "title_clean", "year_final", "overview_tmdb", "poster_path", "popularity_tmdb"] if c in movies.columns]
movie_rows = movies[movie_cols].to_dict("records")

query = """
UNWIND $rows AS row
MERGE (m:Movie {movieId: row.movieId})
SET m.title = row.title_clean,
    m.year = row.year_final
FOREACH (_ IN CASE WHEN row.overview_tmdb IS NULL THEN [] ELSE [1] END |
  SET m.overview = row.overview_tmdb
)
FOREACH (_ IN CASE WHEN row.poster_path IS NULL THEN [] ELSE [1] END |
  SET m.poster_path = row.poster_path
)
FOREACH (_ IN CASE WHEN row.popularity_tmdb IS NULL THEN [] ELSE [1] END |
  SET m.popularity = row.popularity_tmdb
)
"""

run_query_in_batches(query, movie_rows, stage="movies")

# -------------------
# Genres (nodes + edges)
# -------------------

# Skip rows with NaN in key fields so we never create invalid links
genre_cols = [c for c in ["movieId", "genre_name"] if c in genres.columns]
genre_rows = genres.dropna(subset=genre_cols).to_dict("records") if genre_cols else []

query = """
UNWIND $rows AS row
MERGE (g:Genre {name: row.genre_name})
WITH row,g
MATCH (m:Movie {movieId: row.movieId})
MERGE (m)-[:HAS_GENRE]->(g)
"""


run_query_in_batches(query, genre_rows, stage="genres")

# -------------------
# Actors (nodes + edges)
# -------------------

# Skip rows with NaN in key fields (e.g. character) so we never create invalid links
actor_cols = [c for c in ["movieId", "actor_id", "actor_name", "character"] if c in actors.columns]
actor_rows = actors.dropna(subset=actor_cols).to_dict("records") if actor_cols else []

query = """
UNWIND $rows AS row
MERGE (a:Actor {actorId: row.actor_id})
SET a.name = row.actor_name
WITH row,a
MATCH (m:Movie {movieId: row.movieId})
MERGE (m)-[:HAS_ACTOR {character: row.character}]->(a)
"""

run_query_in_batches(
    query,
    actor_rows,
    stage="actors",
    skip_first=295000,
)

# -------------------
# Languages (nodes + edges)
# -------------------

lang_cols = [c for c in ["movieId", "language_name"] if c in languages.columns]
lang_rows = languages.dropna(subset=lang_cols).to_dict("records") if lang_cols else []

query = """
UNWIND $rows AS row
MERGE (l:Language {name: row.language_name})
WITH row,l
MATCH (m:Movie {movieId: row.movieId})
MERGE (m)-[:SPOKEN_LANGUAGE]->(l)
"""

run_query_in_batches(query, lang_rows, stage="languages")

# -------------------
# Countries (nodes + edges)
# -------------------

country_cols = [c for c in ["movieId", "country_code"] if c in countries.columns]
country_rows = countries.dropna(subset=country_cols).to_dict("records") if country_cols else []

query = """
UNWIND $rows AS row
MERGE (c:Country {code: row.country_code})
WITH row,c
MATCH (m:Movie {movieId: row.movieId})
MERGE (m)-[:ORIGIN_COUNTRY]->(c)
"""

run_query_in_batches(query, country_rows, stage="countries")

# -------------------
# Keywords (nodes + edges)
# -------------------

if not keywords.empty:
    kw_cols = [c for c in ["movieId", "keyword_name"] if c in keywords.columns]
    keyword_rows = keywords.dropna(subset=kw_cols).to_dict("records") if kw_cols else []
    query = """
    UNWIND $rows AS row
    MERGE (k:Keyword {name: row.keyword_name})
    WITH row,k
    MATCH (m:Movie {movieId: row.movieId})
    MERGE (m)-[:HAS_KEYWORD]->(k)
    """
    run_query_in_batches(query, keyword_rows, stage="keywords")
else:
    log.info("[keywords] no keyword file found, skipping")

# -------------------
# Directors (nodes + edges)
# -------------------

if not directors.empty:
    dir_cols = [c for c in ["movieId", "director_id", "director_name"] if c in directors.columns]
    director_rows = directors.dropna(subset=dir_cols).to_dict("records") if dir_cols else []
    query = """
    UNWIND $rows AS row
    MERGE (d:Director {directorId: row.director_id})
    SET d.name = row.director_name
    WITH row,d
    MATCH (m:Movie {movieId: row.movieId})
    MERGE (m)-[:DIRECTED_BY]->(d)
    """
    run_query_in_batches(query, director_rows, stage="directors")
else:
    log.info("[directors] no director file found, skipping")

driver.close()

log.info("Graph fully loaded")