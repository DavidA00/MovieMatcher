"""
fix_director_actor_embeddings.py
Run on your Mac. Fixes the two concrete problems from the test output:

PROBLEM 1 — Director/Actor embeddings missing ("has no embedding"):
  The upload script wrote directorId as a string ("525") but Neo4j
  stores it as an integer (525). Every MATCH silently found nothing.
  This script re-uploads just directors and actors with correct int casting.

PROBLEM 2 — avgRating property warning:
  The property is stored as avg_rating (snake_case) not avgRating.
  This is a test-script-only fix — no data change needed.
  The test script already auto-detects the right name, so this is
  just documented here for clarity.

Usage:
  python fix_director_actor_embeddings.py
"""

import os, sys, json, logging
import numpy as np
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

EMBED_DIR      = Path("GAT_files")
NEO4J_URI      = os.environ["NEO4J_URI"]
NEO4J_USER     = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
BATCH_SIZE     = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("fix")

# ── Load embeddings and vocab ─────────────────────────────────
graph_dir = np.load(EMBED_DIR / "graph_emb_director.npy").astype(np.float32)
graph_act = np.load(EMBED_DIR / "graph_emb_actor.npy").astype(np.float32)

with open(EMBED_DIR / "graph_meta.json") as f:
    meta = json.load(f)

# Vocab values are strings in JSON; Neo4j stores them as ints
dir_vocab = [int(v) for v in meta["dir_vocab"]]
act_vocab = [int(v) for v in meta["act_vocab"]]

log.info(f"Directors to upload: {len(dir_vocab)}")
log.info(f"Actors to upload:    {len(act_vocab)}")

# ── Connect ───────────────────────────────────────────────────
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
driver.verify_connectivity()
log.info("Connected to Neo4j")

# ── Verify the ID type actually stored in Neo4j ───────────────
with driver.session() as s:
    sample = s.run("MATCH (d:Director) RETURN d.directorId AS id LIMIT 3").data()
    log.info(f"Sample directorId values in Neo4j: {[r['id'] for r in sample]}")
    log.info(f"Type in Neo4j: {type(sample[0]['id']).__name__ if sample else 'N/A'}")

# ── Upload directors ──────────────────────────────────────────
DIRECTOR_QUERY = """
UNWIND $rows AS row
MATCH (d:Director {directorId: row.did})
SET d.embedding_graph = row.emb
"""

matched_d = 0
with driver.session() as s:
    for i in tqdm(range(0, len(dir_vocab), BATCH_SIZE), desc="Directors"):
        batch = [
            {"did": dir_vocab[i + k], "emb": graph_dir[i + k].tolist()}
            for k in range(min(BATCH_SIZE, len(dir_vocab) - i))
        ]
        summary = s.run(DIRECTOR_QUERY, rows=batch).consume()
        matched_d += summary.counters.properties_set

log.info(f"Director properties set: {matched_d}")

# ── Upload actors ─────────────────────────────────────────────
ACTOR_QUERY = """
UNWIND $rows AS row
MATCH (a:Actor {actorId: row.aid})
SET a.embedding_graph = row.emb
"""

matched_a = 0
with driver.session() as s:
    for i in tqdm(range(0, len(act_vocab), BATCH_SIZE), desc="Actors"):
        batch = [
            {"aid": act_vocab[i + k], "emb": graph_act[i + k].tolist()}
            for k in range(min(BATCH_SIZE, len(act_vocab) - i))
        ]
        summary = s.run(ACTOR_QUERY, rows=batch).consume()
        matched_a += summary.counters.properties_set

log.info(f"Actor properties set: {matched_a}")

# ── Create actor vector index (if not exists) ─────────────────
with driver.session() as s:
    s.run("""
        CREATE VECTOR INDEX actor_graph_idx IF NOT EXISTS
        FOR (a:Actor) ON a.embedding_graph
        OPTIONS {indexConfig: {
            `vector.dimensions`: 256,
            `vector.similarity_function`: 'cosine'
        }}
    """)

# ── Verify ────────────────────────────────────────────────────
log.info("Verification:")
with driver.session() as s:
    for label, prop in [("Director", "embedding_graph"), ("Actor", "embedding_graph")]:
        r = s.run(f"""
            MATCH (n:{label})
            RETURN count(n) AS total, count(n.{prop}) AS has_emb
        """).single()
        pct = 100 * r["has_emb"] / r["total"] if r["total"] else 0
        status = "OK" if pct > 50 else "STILL MISSING"
        log.info(f"  {label}.{prop}: {r['has_emb']:,}/{r['total']:,} ({pct:.1f}%) [{status}]")

    for name in ["Christopher Nolan", "Stanley Kubrick", "Steven Spielberg"]:
        r = s.run("""
            MATCH (d:Director {name: $n})
            RETURN d.directorId AS did, d.embedding_graph IS NOT NULL AS has_emb
        """, n=name).single()
        if r:
            log.info(f"  {name} (id={r['did']}): {'has embedding' if r['has_emb'] else 'STILL MISSING - was not in training subset'}")
        else:
            log.info(f"  {name}: not found in Neo4j")

driver.close()
log.info("Fix complete")
log.info("NOTE: avg_rating is the correct property name (snake_case). No data fix needed.")