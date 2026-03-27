"""
enrich_ratings.py — Add IMDB ratings to Neo4j Movie nodes

Sources:
  1. IMDb official dataset: https://datasets.imdb.com/title.ratings.tsv.gz
     Free, no API, covers ALL titles (~1.4M). Cols: tconst, averageRating, numVotes
  2. Your graph_movies.csv: maps movieId → imdbId

Usage:
  python enrich_ratings.py
  python enrich_ratings.py --ratings-file title.ratings.tsv.gz --csv data/graph/graph_movies.csv
"""

import os, sys, time, argparse
import pandas as pd
import requests
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI  = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASS = os.environ["NEO4J_PASSWORD"]
IMDB_URL   = "https://datasets.imdb.com/title.ratings.tsv.gz"
CSV_PATH   = "data/graph/graph_movies.csv"


def download_imdb(dest="title.ratings.tsv.gz"):
    if Path(dest).exists():
        print(f"  Cached: {dest}"); return dest
    print(f"  Downloading {IMDB_URL}...")
    r = requests.get(IMDB_URL, stream=True); r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    dl = 0
    with open(dest, "wb") as f:
        for chunk in r.iter_content(256*1024):
            f.write(chunk); dl += len(chunk)
            if total: print(f"\r  {dl/1e6:.1f}/{total/1e6:.1f} MB", end="", flush=True)
    print(f"\n  Done: {dest}"); return dest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ratings-file", default=None)
    parser.add_argument("--csv", default=CSV_PATH)
    args = parser.parse_args()

    print("=" * 50)
    print("  IMDB Ratings → Neo4j")
    print("=" * 50)

    # 1. Load your movie mapping
    print(f"\n  Loading {args.csv}...")
    movies = pd.read_csv(args.csv, usecols=["movieId", "imdbId", "title_clean"])
    movies["imdbId"] = movies["imdbId"].apply(
        lambda x: f"tt{int(x):07d}" if pd.notna(x) and str(x).replace("tt","").isdigit()
        else (str(x) if pd.notna(x) else None)
    )
    movies = movies.dropna(subset=["imdbId"])
    print(f"  {len(movies)} movies with IMDB IDs")

    # 2. Load IMDB ratings
    path = args.ratings_file or download_imdb()
    print(f"\n  Loading IMDB ratings...")
    ratings = pd.read_csv(path, sep="\t", compression="gzip" if path.endswith(".gz") else None)
    print(f"  {len(ratings)} IMDB ratings loaded")

    # 3. Merge
    merged = movies.merge(ratings, left_on="imdbId", right_on="tconst", how="inner")
    print(f"\n  Matched: {len(merged)} / {len(movies)} ({len(merged)/len(movies)*100:.1f}%)")
    print(f"  Rating stats: mean={merged['averageRating'].mean():.2f}, "
          f"median={merged['averageRating'].median():.1f}, "
          f"range={merged['averageRating'].min()}-{merged['averageRating'].max()}")

    for _, r in merged.nlargest(5, "averageRating").iterrows():
        print(f"    ★{r['averageRating']} {r['title_clean']} ({r['numVotes']:,} votes)")

    # 4. Write to Neo4j
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print(f"\n  Writing to Neo4j...")

    records = merged[["movieId", "averageRating", "numVotes"]].to_dict("records")
    BATCH = 500
    t0 = time.time()
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        with driver.session() as s:
            s.run("""
                UNWIND $batch AS row
                MATCH (m:Movie {movieId: row.movieId})
                SET m.imdb_rating = row.averageRating,
                    m.imdb_votes = row.numVotes
            """, batch=batch)
        print(f"\r  {min(i+BATCH, len(records))}/{len(records)}", end="", flush=True)

    print(f"\n  Done in {time.time()-t0:.1f}s")

    # 5. Verify
    with driver.session() as s:
        v = s.run("""
            MATCH (m:Movie) WHERE m.imdb_rating IS NOT NULL
            RETURN count(m) AS n, avg(m.imdb_rating) AS avg
        """).single()
        print(f"\n  Verified: {v['n']} movies have imdb_rating (avg: {v['avg']:.2f})")

    driver.close()
    print("\n✅ Done! Properties added: imdb_rating (0-10), imdb_votes (int)")


if __name__ == "__main__":
    main()