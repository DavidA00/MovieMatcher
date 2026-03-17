"""
cypher_search_tests.py
Run on your Mac after neo4j_upload_embeddings.py completes.

Tests every search pattern the app will use:
  1. Semantic search  — encode query → ANN on embedding_semantic
  2. Graph search     — pivot movie's graph embedding → ANN on embedding_graph
  3. Genre slider     — steer query in graph space using genre embedding
  4. Decade slider    — steer query toward a decade
  5. Director slider  — steer toward a director's style
  6. Hybrid           — blend semantic + graph scores 
  7. Filtered         — semantic search + genre/decade constraint in WHERE

All queries use Neo4j's native vector index (db.index.vector.queryNodes)
so no embeddings need to be loaded into Python at query time.
The Jina model IS needed here to encode the text query.
"""

import os, json, sys
import numpy as np
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
import torch

load_dotenv()

EMBED_DIR = Path(os.environ.get("EMBED_DIR", os.path.expanduser("~/moviematcher/embeddings")))
NEO4J_URI      = os.environ["NEO4J_URI"]
NEO4J_USER     = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
EMB_DIM        = 256
TOP_K          = 7

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
driver.verify_connectivity()
print("✅ Neo4j connected\n")

# ── Load Jina for query encoding ─────────────────────────────
# Only needed for text queries. Entity steering uses embeddings
# fetched directly from Neo4j.
print("Loading Jina encoder…")
from transformers import AutoModel
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
jina = AutoModel.from_pretrained(
    "jinaai/jina-embeddings-v5-text-nano",
    trust_remote_code=True, torch_dtype=torch.bfloat16
).to(DEVICE)
jina.eval()

def encode_query(text: str) -> list[float]:
    """Encode a text query → 256-dim L2-normalised float list for Cypher."""
    with torch.no_grad():
        emb = jina.encode(
            texts=[text], task="retrieval",
            prompt_name="query", truncate_dim=EMB_DIM
        )
    if isinstance(emb, torch.Tensor):
        emb = emb.cpu().float().numpy()
    emb = np.array(emb[0], dtype=np.float32)
    emb = emb / (np.linalg.norm(emb) + 1e-8)
    return emb.tolist()

def run(cypher, **params):
    with driver.session() as s:
        return s.run(cypher, **params).data()

def header(title):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"{'='*65}")

def print_results(rows, score_key="score"):
    for r in rows:
        title = r.get("title") or r.get("name") or str(r)
        score = r.get(score_key, 0)
        year  = r.get("year", "")
        print(f"  {title:<48} {year!s:<6}  {score:.4f}")


# ================================================================
# TEST 1 — SEMANTIC SEARCH
# ================================================================
# Encode text query → ANN over Movie.embedding_semantic
# This is the main search bar query.

header("TEST 1 — SEMANTIC SEARCH")
queries = [
    "slow atmospheric sci-fi about communication with aliens",
    "feel-good animated family movie with humor and heart",
    "gritty 1970s crime drama with morally ambiguous characters",
]

for q in queries:
    print(f"\n  Query: \"{q}\"")
    vec = encode_query(q)
    rows = run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.title AS title, m.year AS year, score
        ORDER BY score DESC
    """, k=TOP_K, vec=vec)
    print_results(rows)


# ================================================================
# TEST 2 — GRAPH SEARCH (pivot-based)
# ================================================================
# Semantic search → take top hit → use its graph embedding as pivot
# → ANN over Movie.embedding_graph.
# Finds movies structurally similar (same cast/genre/director patterns).

header("TEST 2 — GRAPH SEARCH (pivot-based)")

for q in queries:
    print(f"\n  Query: \"{q}\"")
    vec = encode_query(q)

    # Step 1: get top semantic hit
    pivot = run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', 1, $vec)
        YIELD node AS m, score
        RETURN m.title AS title, m.movieId AS movieId, m.embedding_graph AS gvec
    """, vec=vec)

    if not pivot or pivot[0].get("gvec") is None:
        print("  (no pivot found or pivot has no graph embedding)")
        continue

    pivot_title = pivot[0]["title"]
    g_vec       = pivot[0]["gvec"]
    print(f"  Pivot: {pivot_title}")

    rows = run("""
        CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
        YIELD node AS m, score
        WHERE m.title <> $pivot_title
        RETURN m.title AS title, m.year AS year, score
        ORDER BY score DESC
        LIMIT $k
    """, k=TOP_K + 1, vec=g_vec, pivot_title=pivot_title)
    print_results(rows[:TOP_K])


# ================================================================
# TEST 3 — GENRE SLIDER STEERING
# ================================================================
# Fetch the genre node's embedding from Neo4j → add α*genre_emb
# to the query vector → ANN on graph index.
# "More Action" means α > 0; "Less Action" means α < 0.

header("TEST 3 — GENRE SLIDER STEERING")

def steer_by_entity(query_text, entity_label, entity_id_prop,
                    entity_id_val, alpha=0.5, index="movie_graph_idx"):
    """
    Steer a query vector using an entity's graph embedding.
    alpha > 0  → more like that entity
    alpha < 0  → less like that entity
    """
    vec = encode_query(query_text)

    # Fetch entity embedding from Neo4j
    entity_rows = run(f"""
        MATCH (e:{entity_label} {{{entity_id_prop}: $val}})
        RETURN e.embedding_graph AS emb
    """, val=entity_id_val)

    if not entity_rows or entity_rows[0]["emb"] is None:
        print(f"  ⚠️  {entity_label} '{entity_id_val}' has no embedding")
        return []

    e_emb = np.array(entity_rows[0]["emb"], dtype=np.float32)

    # Semantic query is in semantic space; we need it in graph space.
    # Use the top semantic hit as the graph-space anchor (same pivot
    # pattern as Test 2), then steer from there.
    pivot = run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', 1, $vec)
        YIELD node AS m RETURN m.embedding_graph AS gvec
    """, vec=vec)

    if not pivot or pivot[0].get("gvec") is None:
        return []

    g_vec  = np.array(pivot[0]["gvec"], dtype=np.float32)
    steered = g_vec + alpha * e_emb
    steered = steered / (np.linalg.norm(steered) + 1e-8)

    return run("""
        CALL db.index.vector.queryNodes($index, $k, $vec)
        YIELD node AS m, score
        RETURN m.title AS title, m.year AS year, score
        ORDER BY score DESC
        LIMIT $k
    """, index=index, k=TOP_K, vec=steered.tolist())


q = "an exciting movie with great characters"

print(f"\n  Base query: \"{q}\"")

for genre_name, alpha in [("Action", 0.6), ("Comedy", 0.6),
                           ("Horror", 0.6), ("Animation", 0.6)]:
    print(f"\n  ── Steered toward genre: '{genre_name}'  (α={alpha}) ──")
    rows = steer_by_entity(q, "Genre", "name", genre_name, alpha=alpha)
    print_results(rows)


# ================================================================
# TEST 4 — DECADE SLIDER
# ================================================================

header("TEST 4 — DECADE SLIDER")

q = "a great drama with powerful performances"
for decade_label, alpha in [("1970s", 0.5), ("1990s", 0.5), ("2010s", 0.5)]:
    print(f"\n  ── '{q}' steered toward {decade_label} (α={alpha}) ──")
    rows = steer_by_entity(q, "Decade", "label", decade_label, alpha=alpha)
    print_results(rows)


# ================================================================
# TEST 5 — DIRECTOR STYLE SLIDER
# ================================================================
# Fetch a director's embedding by name (not ID), then steer.

header("TEST 5 — DIRECTOR STYLE STEERING")

q = "a tense, cerebral thriller"

directors_to_test = ["Christopher Nolan", "Stanley Kubrick", "Steven Spielberg"]
for dname in directors_to_test:
    # Find director_id by name
    d = run("MATCH (d:Director {name: $n}) RETURN d.directorId AS did", n=dname)
    if not d:
        print(f"\n  Director '{dname}' not in Neo4j"); continue
    did = d[0]["did"]
    print(f"\n  ── '{q}' steered toward {dname} (α=0.5) ──")
    rows = steer_by_entity(q, "Director", "directorId", did, alpha=0.5)
    print_results(rows)


# ================================================================
# TEST 6 — HYBRID SCORE (semantic + graph blended)
# ================================================================
# Final ranking formula used in the app:
#   hybrid_score = λ * semantic_score + (1-λ) * graph_score
# This requires running both ANN queries and merging in Python.
# At production scale, do this in the API layer (FastAPI/Node).

header("TEST 6 — HYBRID SCORE  (λ=0.6 semantic + 0.4 graph)")

def hybrid_search(query_text, lam=0.6, k=TOP_K):
    vec = encode_query(query_text)

    # Semantic candidates
    sem_rows = run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.movieId AS movieId, m.title AS title, m.year AS year,
               score AS sem_score, m.embedding_graph AS gvec
    """, k=k*3, vec=vec)   # fetch 3x to allow for graph re-ranking

    if not sem_rows:
        return []

    # Pivot: top semantic hit's graph vector
    pivot_gvec = None
    for r in sem_rows:
        if r.get("gvec"):
            pivot_gvec = np.array(r["gvec"], dtype=np.float32)
            break

    if pivot_gvec is None:
        # Fallback: semantic only
        return sorted(sem_rows, key=lambda x: -x["sem_score"])[:k]

    # Graph candidates from pivot
    graph_rows = run("""
        CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.movieId AS movieId, score AS graph_score
    """, k=k*3, vec=pivot_gvec.tolist())

    graph_scores = {r["movieId"]: r["graph_score"] for r in graph_rows}

    # Merge and blend
    results = []
    for r in sem_rows:
        mid = r["movieId"]
        s_score = r["sem_score"]
        g_score = graph_scores.get(mid, 0.0)
        hybrid  = lam * s_score + (1 - lam) * g_score
        results.append({**r, "score": hybrid,
                        "sem_score": s_score, "graph_score": g_score})

    return sorted(results, key=lambda x: -x["score"])[:k]


for q in queries:
    print(f"\n  Query: \"{q}\"")
    rows = hybrid_search(q)
    for r in rows:
        print(f"  {r['title']:<48} {str(r.get('year','')):<6}  "
              f"hybrid={r['score']:.3f}  "
              f"(sem={r['sem_score']:.3f} graph={r['graph_score']:.3f})")


# ================================================================
# TEST 7 — FILTERED SEARCH (semantic + WHERE constraint)
# ================================================================
# Semantic ANN + Cypher WHERE for hard filters (genre, year range).
# This is the approach for the filter chips in the UI.
# Note: WHERE filters apply AFTER the ANN — if the ANN returns k
# results and all fail the filter, you get 0. Use a larger k (3-5x)
# and then filter, or use pre-filtering if Neo4j version supports it.

header("TEST 7 — FILTERED SEMANTIC SEARCH")

print("\n  Horror movies about isolation (genre filter):")
vec = encode_query("a movie about isolation and psychological horror")
rows = run("""
    CALL db.index.vector.queryNodes('movie_semantic_idx', 50, $vec)
    YIELD node AS m, score
    WHERE EXISTS {
        MATCH (m)-[:HAS_GENRE]->(g:Genre {name: 'Horror'})
    }
    RETURN m.title AS title, m.year AS year, score
    ORDER BY score DESC
    LIMIT $k
""", vec=vec, k=TOP_K)
print_results(rows)

print("\n  Action movies from the 1990s (genre + decade filter):")
vec = encode_query("high energy action blockbuster")
rows = run("""
    CALL db.index.vector.queryNodes('movie_semantic_idx', 50, $vec)
    YIELD node AS m, score
    WHERE EXISTS { MATCH (m)-[:HAS_GENRE]->(g:Genre {name: 'Action'}) }
      AND EXISTS { MATCH (m)-[:IN_DECADE]->(dc:Decade {label: '1990s'}) }
    RETURN m.title AS title, m.year AS year, score
    ORDER BY score DESC
    LIMIT $k
""", vec=vec, k=TOP_K)
print_results(rows)

print("\n  Sci-Fi movies with rating > 3.8:")
vec = encode_query("epic sci-fi adventure in space")
rows = run("""
    CALL db.index.vector.queryNodes('movie_semantic_idx', 50, $vec)
    YIELD node AS m, score
    WHERE EXISTS { MATCH (m)-[:HAS_GENRE]->(g:Genre {name: 'Science Fiction'}) }
      AND m.avgRating >= 3.8
    RETURN m.title AS title, m.year AS year,
           round(m.avgRating * 100) / 100 AS rating, score
    ORDER BY score DESC
    LIMIT $k
""", vec=vec, k=TOP_K)
for r in rows:
    print(f"  {r['title']:<48} {str(r.get('year','')):<6} ★{r.get('rating',0):.2f}  {r['score']:.4f}")


driver.close()
print("\n✅ All tests complete")