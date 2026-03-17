"""
neo4j_upload_embeddings.py
Run on your Mac after copying embedding files from Colab Drive.

What this script does:
  1. Creates vector indexes on Movie.embedding_semantic and
     Movie.embedding_graph (and entity graph indexes) so Cypher
     ANN queries work natively.
  2. Writes embedding_semantic + embedding_graph to existing Movie nodes.
  3. Writes embedding_graph to existing Genre, Director, Actor nodes.
  4. MERGES new Decade nodes (they don't exist in Neo4j yet) and writes
     their embedding_graph.
  5. Handles discrepancies gracefully:
       - Node in Neo4j but not in embeddings → skip, count logged
       - Node in embeddings but not in Neo4j  → log to skipped CSV, skip
  6. Prints a full reconciliation summary at the end.

Usage:
  python neo4j_upload_embeddings.py

  Or with explicit paths:
  EMBED_DIR=/path/to/embeddings python neo4j_upload_embeddings.py
"""

import os, sys, json, logging
import numpy as np
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ── Config ────────────────────────────────────────────────────
EMBED_DIR = Path("GAT_files")

NEO4J_URI      = os.environ["NEO4J_URI"]
NEO4J_USER     = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

EMB_DIM        = 256
BATCH_SIZE     = 200    # nodes per Cypher write batch

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("neo4j_upload.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("uploader")

# ── Load files ────────────────────────────────────────────────
log.info(f"Loading embeddings from {EMBED_DIR}")

def load_npy(name):
    p = EMBED_DIR / f"{name}.npy"
    if not p.exists():
        log.error(f"Missing: {p}")
        sys.exit(1)
    arr = np.load(p).astype(np.float32)
    log.info(f"  {name}: {arr.shape}")
    return arr

sem_emb      = load_npy("semantic_embeddings")
graph_movie  = load_npy("graph_emb_movie")
graph_genre  = load_npy("graph_emb_genre")
graph_dir    = load_npy("graph_emb_director")
graph_actor  = load_npy("graph_emb_actor")
graph_kw     = load_npy("graph_emb_keyword")
graph_lang   = load_npy("graph_emb_language")
graph_country= load_npy("graph_emb_country")
graph_decade = load_npy("graph_emb_decade")

with open(EMBED_DIR / "graph_meta.json") as f:
    meta = json.load(f)

movie_ids      = [int(x) for x in meta["movie_ids"]]    # movieId is int in Neo4j
genre_vocab    = meta["genre_vocab"]
dir_vocab      = meta["dir_vocab"]    # director_id strings
act_vocab      = meta["act_vocab"]    # actor_id strings
kw_vocab       = meta["kw_vocab"]
lang_vocab     = meta["lang_vocab"]
country_vocab  = meta["country_vocab"]
decade_vocab   = meta["decade_vocab"]

log.info(f"Vocab sizes: movies={len(movie_ids)}, genres={len(genre_vocab)}, "
         f"directors={len(dir_vocab)}, actors={len(act_vocab)}, "
         f"keywords={len(kw_vocab)}, languages={len(lang_vocab)}, "
         f"countries={len(country_vocab)}, decades={len(decade_vocab)}")

# ── Neo4j connection ──────────────────────────────────────────
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
try:
    driver.verify_connectivity()
    log.info("✅ Neo4j connected")
except Exception as e:
    log.error(f"Neo4j connection failed: {e}")
    sys.exit(1)

# ── Reconciliation tracking ───────────────────────────────────
skipped = []   # {"type": ..., "id": ..., "reason": ...}

def record_skip(node_type, node_id, reason):
    skipped.append({"type": node_type, "id": str(node_id), "reason": reason})

# ── Helper: run a query ───────────────────────────────────────
def run(session, cypher, **params):
    return session.run(cypher, **params).data()

# ── Step 1: Create vector indexes ────────────────────────────
# Neo4j 5.x native vector index — enables fast ANN search via
# db.index.vector.queryNodes() without loading embeddings into Python.
#
# DISCREPANCY DECISION for indexes:
#   We always CREATE IF NOT EXISTS — safe to re-run.

log.info("Creating vector indexes…")
INDEX_QUERIES = [
    # Semantic index on Movie (used for vibe/text query search)
    f"""CREATE VECTOR INDEX movie_semantic_idx IF NOT EXISTS
        FOR (m:Movie) ON m.embedding_semantic
        OPTIONS {{indexConfig: {{
            `vector.dimensions`: {EMB_DIM},
            `vector.similarity_function`: 'cosine'
        }}}}""",

    # Graph index on Movie (used for structural similarity + sliders)
    f"""CREATE VECTOR INDEX movie_graph_idx IF NOT EXISTS
        FOR (m:Movie) ON m.embedding_graph
        OPTIONS {{indexConfig: {{
            `vector.dimensions`: {EMB_DIM},
            `vector.similarity_function`: 'cosine'
        }}}}""",

    # Entity indexes (used for slider steering)
    f"""CREATE VECTOR INDEX genre_graph_idx IF NOT EXISTS
        FOR (g:Genre) ON g.embedding_graph
        OPTIONS {{indexConfig: {{
            `vector.dimensions`: {EMB_DIM},
            `vector.similarity_function`: 'cosine'
        }}}}""",

    f"""CREATE VECTOR INDEX director_graph_idx IF NOT EXISTS
        FOR (d:Director) ON d.embedding_graph
        OPTIONS {{indexConfig: {{
            `vector.dimensions`: {EMB_DIM},
            `vector.similarity_function`: 'cosine'
        }}}}""",

    f"""CREATE VECTOR INDEX decade_graph_idx IF NOT EXISTS
        FOR (dc:Decade) ON dc.embedding_graph
        OPTIONS {{indexConfig: {{
            `vector.dimensions`: {EMB_DIM},
            `vector.similarity_function`: 'cosine'
        }}}}""",
]

with driver.session() as s:
    for q in INDEX_QUERIES:
        s.run(q)
log.info("✅ Vector indexes ready")


# ── Step 2: Check what actually exists in Neo4j ───────────────
# We pre-fetch the set of existing IDs for each node type so we
# can categorise every embedding row as matched / unmatched
# before writing anything. This avoids silent MATCH failures.

log.info("Fetching existing IDs from Neo4j…")
with driver.session() as s:
    neo_movie_ids   = {r["id"] for r in run(s, "MATCH (m:Movie)    RETURN m.movieId    AS id")}
    neo_genre_names = {r["id"] for r in run(s, "MATCH (g:Genre)    RETURN g.name       AS id")}
    neo_dir_ids     = {str(r["id"]) for r in run(s, "MATCH (d:Director) RETURN d.directorId AS id")}
    neo_act_ids     = {str(r["id"]) for r in run(s, "MATCH (a:Actor)    RETURN a.actorId    AS id")}
    neo_kw_names    = {r["id"] for r in run(s, "MATCH (k:Keyword)  RETURN k.name       AS id")}
    neo_lang_names  = {r["id"] for r in run(s, "MATCH (l:Language) RETURN l.name       AS id")}
    neo_country_codes={r["id"] for r in run(s, "MATCH (c:Country)  RETURN c.code       AS id")}

log.info(f"  Neo4j: {len(neo_movie_ids)} movies, {len(neo_genre_names)} genres, "
         f"{len(neo_dir_ids)} directors, {len(neo_act_ids)} actors")


# ── Step 3: Write Movie embeddings ───────────────────────────
# DISCREPANCY DECISION for Movies:
#   In embeddings but not Neo4j → log + skip (shouldn't happen for
#   the full run, but might during subset testing).
#   In Neo4j but not in embeddings → untouched (movies outside the
#   training pool keep no embedding; the app simply won't surface them
#   via embedding-based search, which is correct behaviour).

log.info("Writing Movie embeddings (semantic + graph)…")

matched_movies, skipped_movies = 0, 0
batch = []

def flush_movie_batch(session, b):
    session.run("""
        UNWIND $rows AS row
        MATCH (m:Movie {movieId: row.movieId})
        SET m.embedding_semantic = row.sem,
            m.embedding_graph    = row.gph
    """, rows=b)

with driver.session() as s:
    for i, mid in enumerate(tqdm(movie_ids, desc="Movies")):
        if mid not in neo_movie_ids:
            record_skip("Movie", mid, "not_in_neo4j")
            skipped_movies += 1
            continue
        batch.append({
            "movieId": mid,
            "sem": sem_emb[i].tolist(),
            "gph": graph_movie[i].tolist(),
        })
        matched_movies += 1
        if len(batch) >= BATCH_SIZE:
            flush_movie_batch(s, batch)
            batch = []
    if batch:
        flush_movie_batch(s, batch)

log.info(f"  Movies → matched: {matched_movies}, skipped: {skipped_movies}")


# ── Step 4: Generic entity writer ────────────────────────────

def write_entity_embeddings(
    node_label, id_property, vocab, embeddings,
    neo_id_set, match_transform=None
):
    """
    Writes embedding_graph to existing entity nodes.
    match_transform: optional fn applied to vocab value before Neo4j lookup
                     (e.g. int() for director IDs stored as int in Neo4j)
    """
    matched, skipped_count = 0, 0
    batch = []

    cypher = f"""
        UNWIND $rows AS row
        MATCH (n:{node_label} {{{id_property}: row.node_id}})
        SET n.embedding_graph = row.emb
    """

    with driver.session() as s:
        for i, v in enumerate(tqdm(vocab, desc=node_label)):
            lookup_v = match_transform(v) if match_transform else v
            if str(lookup_v) not in {str(x) for x in neo_id_set}:
                record_skip(node_label, v, "not_in_neo4j")
                skipped_count += 1
                continue
            batch.append({"node_id": lookup_v, "emb": embeddings[i].tolist()})
            matched += 1
            if len(batch) >= BATCH_SIZE:
                s.run(cypher, rows=batch)
                batch = []
        if batch:
            s.run(cypher, rows=batch)

    log.info(f"  {node_label} → matched: {matched}, skipped: {skipped_count}")
    return matched, skipped_count


# ── Step 5: Write Genre embeddings ───────────────────────────
log.info("Writing Genre embeddings…")
# write_entity_embeddings(
#     "Genre", "name", genre_vocab, graph_genre, neo_genre_names
# )

# ── Step 6: Write Director embeddings ────────────────────────
log.info("Writing Director embeddings…")
# director_id stored as string in our vocab, check against Neo4j directorId
# write_entity_embeddings(
#     "Director", "directorId", dir_vocab, graph_dir, neo_dir_ids
# )

# ── Step 7: Write Actor embeddings ───────────────────────────
log.info("Writing Actor embeddings…")
write_entity_embeddings(
    "Actor", "actorId", act_vocab, graph_actor, neo_act_ids
)

# ── Step 8: Write Keyword embeddings ─────────────────────────
log.info("Writing Keyword embeddings…")
write_entity_embeddings(
    "Keyword", "name", kw_vocab, graph_kw, neo_kw_names
)

# ── Step 9: Write Language embeddings ────────────────────────
log.info("Writing Language embeddings…")
write_entity_embeddings(
    "Language", "name", lang_vocab, graph_lang, neo_lang_names
)

# ── Step 10: Write Country embeddings ────────────────────────
log.info("Writing Country embeddings…")
write_entity_embeddings(
    "Country", "code", country_vocab, graph_country, neo_country_codes
)

# ── Step 11: MERGE Decade nodes + embeddings ─────────────────
# DISCREPANCY DECISION for Decade:
#   Decade nodes do NOT exist in Neo4j yet. We MERGE (create-or-match)
#   so the script is safe to re-run. Each Decade node gets a label
#   and an embedding_graph. We also add HAS_DECADE edges from Movie
#   to Decade based on the movie's year, derived directly in Cypher
#   from the year already stored on Movie nodes.

log.info("Merging Decade nodes + embeddings…")
with driver.session() as s:
    # First create/update decade nodes with their embeddings
    for i, label in enumerate(tqdm(decade_vocab, desc="Decades")):
        s.run("""
            MERGE (dc:Decade {label: $label})
            SET dc.embedding_graph = $emb
        """, label=label, emb=graph_decade[i].tolist())

    # Now create HAS_DECADE edges for all movies in Neo4j
    # using the year already stored on Movie.year
    # The Cypher expression (m.year / 10) * 10 + 's' builds the label
    log.info("Creating Movie→Decade edges from Movie.year…")
    result = s.run("""
        MATCH (m:Movie)
        WHERE m.year IS NOT NULL
        WITH m,
             toString(toInteger(m.year / 10) * 10) + 's' AS decade_label
        MATCH (dc:Decade {label: decade_label})
        MERGE (m)-[:IN_DECADE]->(dc)
        RETURN count(*) AS edges_created
    """).single()
    edges = result["edges_created"] if result else 0
    log.info(f"  IN_DECADE edges: {edges:,}")

log.info(f"✅ Decade nodes written: {len(decade_vocab)}")


# ── Step 12: Reconciliation summary ──────────────────────────
log.info("\n" + "="*55)
log.info("RECONCILIATION SUMMARY")
log.info("="*55)

with driver.session() as s:
    for label, prop in [
        ("Movie",    "embedding_semantic"),
        ("Movie",    "embedding_graph"),
        ("Genre",    "embedding_graph"),
        ("Director", "embedding_graph"),
        ("Actor",    "embedding_graph"),
        ("Decade",   "embedding_graph"),
    ]:
        r = s.run(f"""
            MATCH (n:{label})
            RETURN
              count(n) AS total,
              count(n.{prop}) AS has_emb
        """).single()
        pct = 100 * r["has_emb"] / r["total"] if r["total"] > 0 else 0
        log.info(f"  {label}.{prop}: {r['has_emb']:,}/{r['total']:,} ({pct:.1f}%)")

if skipped:
    skip_path = Path("neo4j_upload_skipped.csv")
    pd.DataFrame(skipped).to_csv(skip_path, index=False)
    log.info(f"\n  ⚠️  {len(skipped)} nodes skipped → {skip_path}")
    log.info("  These are nodes present in the embedding file but absent")
    log.info("  from Neo4j. Most likely cause: the full-run movie pool")
    log.info("  includes a movie/entity not in your original Neo4j load.")
    log.info("  Action: inspect the CSV; if large, re-run the Neo4j loader")
    log.info("  for the missing entries before re-running this script.")
else:
    log.info("\n  ✅ No skipped nodes — full reconciliation achieved")

log.info("="*55)
log.info("✅ Upload complete")
driver.close()


