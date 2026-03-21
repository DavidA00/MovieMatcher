"""
enrich_ratings.py — Add IMDB ratings to Neo4j Movie nodes

STEP 1: Download title.ratings.tsv.gz manually from:
        https://developer.imdb.com/non-commercial-datasets/
        (Free, just need an IMDb account)

STEP 2: Run this script:
        python enrich_ratings.py --ratings-file title.ratings.tsv.gz --csv data/graph/graph_movies.csv

If you don't want to download, the script also tries these fallback URLs:
  - https://datasets.imdb.com/title.ratings.tsv.gz (legacy, may 404)
"""

import os, sys, time, argparse
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI  = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASS = os.environ["NEO4J_PASSWORD"]
CSV_PATH   = "data/graph/graph_movies.csv"

FALLBACK_URLS = [
    "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "https://datasets.imdb.com/title.ratings.tsv.gz",
]


def try_download(dest="title.ratings.tsv.gz"):
    """Try downloading from known URLs."""
    import requests
    for url in FALLBACK_URLS:
        try:
            print(f"  Trying {url}...")
            r = requests.get(url, stream=True, timeout=15)
            if r.status_code == 200:
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(256*1024):
                        f.write(chunk)
                print(f"  Downloaded: {dest}")
                return dest
            else:
                print(f"  Got {r.status_code}, skipping...")
        except Exception as e:
            print(f"  Failed: {e}")
    return None


def main():
    parser = argparse.ArgumentParser(description="Enrich Neo4j movies with IMDB ratings")
    parser.add_argument("--ratings-file", default=None,
                        help="Path to title.ratings.tsv.gz (download from developer.imdb.com)")
    parser.add_argument("--csv", default=CSV_PATH, help="Path to graph_movies.csv")
    args = parser.parse_args()

    print("=" * 50)
    print("  IMDB Ratings → Neo4j")
    print("=" * 50)

    # Find ratings file
    ratings_path = args.ratings_file
    if ratings_path and Path(ratings_path).exists():
        print(f"\n  Using provided file: {ratings_path}")
    elif Path("title.ratings.tsv.gz").exists():
        ratings_path = "title.ratings.tsv.gz"
        print(f"\n  Found cached: {ratings_path}")
    elif Path("title.ratings.tsv").exists():
        ratings_path = "title.ratings.tsv"
        print(f"\n  Found cached (uncompressed): {ratings_path}")
    else:
        print("\n  No local ratings file found. Trying to download...")
        ratings_path = try_download()
        if not ratings_path:
            print("\n  ❌ Could not download automatically.")
            print("  Please download manually from:")
            print("    https://developer.imdb.com/non-commercial-datasets/")
            print("  Look for 'title.ratings.tsv.gz', download it, then run:")
            print(f"    python {sys.argv[0]} --ratings-file title.ratings.tsv.gz")
            sys.exit(1)

    # 1. Load movie mapping
    print(f"\n  Loading {args.csv}...")
    movies = pd.read_csv(args.csv, usecols=["movieId", "imdbId", "title_clean"])
    movies["imdbId"] = movies["imdbId"].apply(
        lambda x: f"tt{int(x):07d}" if pd.notna(x) and str(x).replace("tt","").isdigit()
        else (str(x) if pd.notna(x) else None)
    )
    movies = movies.dropna(subset=["imdbId"])
    print(f"  {len(movies)} movies with IMDB IDs")
    print(f"  Sample IDs from your CSV: {movies['imdbId'].head(5).tolist()}")

    # 2. Load IMDB ratings
    print(f"\n  Loading IMDB ratings from {ratings_path}...")
    comp = "gzip" if ratings_path.endswith(".gz") else None
    ratings = pd.read_csv(ratings_path, sep="\t", compression=comp)
    print(f"  {len(ratings)} IMDB ratings loaded")
    print(f"  Sample IDs from IMDB file: {ratings['tconst'].head(5).tolist()}")

    # 3. Merge
    merged = movies.merge(ratings, left_on="imdbId", right_on="tconst", how="inner")
    print(f"\n  Matched: {len(merged)} / {len(movies)} ({len(merged)/len(movies)*100:.1f}%)")

    if merged.empty:
        print("  ❌ No matches! Check that imdbId format matches (should be tt1234567)")
        sys.exit(1)

    print(f"  Stats: mean={merged['averageRating'].mean():.2f}, "
          f"median={merged['averageRating'].median():.1f}")

    for _, r in merged.nlargest(5, "numVotes").iterrows():
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
        v = s.run("MATCH (m:Movie) WHERE m.imdb_rating IS NOT NULL RETURN count(m) AS n").single()
        print(f"\n  ✅ {v['n']} movies now have imdb_rating")
    driver.close()


if __name__ == "__main__":
    main()