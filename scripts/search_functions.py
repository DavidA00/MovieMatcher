"""
search_functions.py
All search patterns and experiments for MovieMatcher.

Each function is self-contained and designed to be called by:
  - the test harness at the bottom of this file
  - a LangChain/LangGraph tool wrapping (function signatures are clean)
  - the FastAPI search endpoint

EMBEDDING SPACES — used throughout:
  embedding_semantic (256-dim, Jina):
    Encodes text meaning. Used for vibe/natural language queries.
    Query → encode with Jina → ANN on movie_semantic_idx.

  embedding_graph (256-dim, GAT):
    Encodes structural position in the KG (genre, cast, director,
    decade, keyword neighbors). Used for structural similarity,
    slider steering, and group preference math.
    Query → find semantic pivot → use pivot's graph vector → ANN on movie_graph_idx.

  Entity graph embeddings (genre, decade, director, actor, keyword):
    Same 256-dim GAT space as movie graph embeddings.
    Used as steering directions: query_graph + α * entity_emb.

PROPERTY NAMES on Movie nodes (as stored by the loader):
  avg_rating, rating_count, popularity, title, year, overview, poster_path
"""

import os, sys, warnings
import numpy as np
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
import torch
from collections import defaultdict

warnings.filterwarnings("ignore")
load_dotenv()

# ── Connection ────────────────────────────────────────────────
NEO4J_URI      = os.environ["NEO4J_URI"]
NEO4J_USER     = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
EMB_DIM        = 256
TOP_K          = 7

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
driver.verify_connectivity()

def _run(cypher, **params):
    with driver.session() as s:
        return s.run(cypher, **params).data()

# ── Detect actual property names ──────────────────────────────
_sample = _run("MATCH (m:Movie) WHERE m.title IS NOT NULL RETURN keys(m) AS p LIMIT 1")
_props  = set(_sample[0]["p"]) if _sample else set()
RATING_PROP = next((p for p in ["avg_rating","avgRating","average_rating"] if p in _props), None)
COUNT_PROP  = next((p for p in ["rating_count","ratingCount"] if p in _props), None)
POP_PROP    = next((p for p in ["popularity","popularity_tmdb"] if p in _props), None)
print(f"Detected: rating={RATING_PROP}, count={COUNT_PROP}, popularity={POP_PROP}")

# ── Jina encoder ─────────────────────────────────────────────
print("Loading Jina encoder...")
from transformers import AutoModel
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
_jina = AutoModel.from_pretrained(
    "jinaai/jina-embeddings-v5-text-nano",
    trust_remote_code=True, torch_dtype=torch.bfloat16
).to(DEVICE)
_jina.eval()

def encode_query(text: str) -> np.ndarray:
    """Encode natural language text → 256-dim L2-normalised numpy vector.
    Uses Jina v5-nano with retrieval task (asymmetric: query prefix applied).
    Returns semantic embedding, NOT graph embedding.
    """
    with torch.no_grad():
        emb = _jina.encode(texts=[text], task="retrieval",
                           prompt_name="query", truncate_dim=EMB_DIM)
    if isinstance(emb, torch.Tensor):
        emb = emb.cpu().float().numpy()
    emb = np.array(emb[0], dtype=np.float32)
    return emb / (np.linalg.norm(emb) + 1e-8)


# ================================================================
# CORE SEARCH FUNCTIONS
# ================================================================

def semantic_search(query: str, k: int = TOP_K) -> list[dict]:
    """
    USES: embedding_semantic
    Encode query text → ANN over Movie.embedding_semantic.
    Returns movies whose text (overview + metadata) matches the vibe.
    Best for: natural language queries, mood/theme search.

    Args:
        query: natural language search string
        k: number of results
    Returns:
        list of {title, year, score, movieId}
    """
    vec = encode_query(query).tolist()
    return _run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.movieId AS movieId, m.title AS title,
               m.year AS year, score
        ORDER BY score DESC
    """, k=k, vec=vec)


def graph_search_multi_pivot(query: str, n_pivots: int = 5,
                              k: int = TOP_K) -> dict:
    """
    USES: embedding_semantic (to find pivots) + embedding_graph (for retrieval)
    Improvement over single-pivot: take top-N semantic hits, fetch their
    graph embeddings, average them → more stable graph-space query vector.

    Why this is better than single pivot:
      Single pivot locks onto one movie's structural neighborhood.
      Multi-pivot averages over N representative movies, giving a
      centroid of the graph region that matches the query.

    Args:
        query: natural language search string
        n_pivots: how many semantic hits to average graph embeddings over
        k: number of results
    Returns:
        dict with keys: pivots (list of titles used), results (list of movies),
                        pivot_vector_std (how spread out the pivot embeddings were)
    """
    vec = encode_query(query).tolist()

    # Fetch top-N semantic hits that have graph embeddings
    pivots = _run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', $n, $vec)
        YIELD node AS m, score
        WHERE m.embedding_graph IS NOT NULL
        RETURN m.title AS title, m.embedding_graph AS gvec, score
        ORDER BY score DESC LIMIT $n
    """, n=n_pivots * 3, vec=vec)   # fetch 3x to filter nulls
    pivots = [p for p in pivots if p.get("gvec")][:n_pivots]

    if not pivots:
        return {"pivots": [], "results": [], "pivot_vector_std": 0.0}

    pivot_matrix = np.array([p["gvec"] for p in pivots], dtype=np.float32)
    # Average the pivot graph embeddings and re-normalise
    avg_pivot = pivot_matrix.mean(axis=0)
    avg_pivot = avg_pivot / (np.linalg.norm(avg_pivot) + 1e-8)
    # Pivot spread = std of pairwise cosine similarities between pivots.
    # High std (~0.1+) means pivots are in different graph neighborhoods
    # (query is ambiguous or crosses clusters).
    # Low std (~0.01) means pivots agree — tight, confident query.
    if len(pivot_matrix) > 1:
        sim_matrix = pivot_matrix @ pivot_matrix.T   # (N, N) cosine sims
        upper_tri  = sim_matrix[np.triu_indices(len(pivot_matrix), k=1)]
        pivot_std  = float(upper_tri.std())
    else:
        pivot_std = 0.0

    pivot_titles = [p["title"] for p in pivots]

    results = _run("""
        CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
        YIELD node AS m, score
        WHERE NOT m.title IN $pivot_titles
        RETURN m.movieId AS movieId, m.title AS title,
               m.year AS year, score
        ORDER BY score DESC LIMIT $k
    """, k=k + len(pivot_titles), vec=avg_pivot.tolist(),
         pivot_titles=pivot_titles)[:k]

    return {
        "pivots": pivot_titles,
        "results": results,
        "pivot_vector_std": pivot_std
    }


def steer_by_genres(query: str, genre_weights: dict[str, float],
                    k: int = TOP_K) -> list[dict]:
    """
    USES: embedding_semantic (pivot) + embedding_graph (retrieval) + Genre.embedding_graph (steering)
    Find movies matching the query AND steered toward a weighted blend of genres.

    genre_weights: {genre_name: alpha} — weights will be normalised to sum=1 internally.
    Supports any number of genres. Default use case: 2 genres (genre slider in UI).

    Steering formula:
        steered = normalize(pivot_graph + Σ alpha_i * genre_emb_i)
    where Σ alpha_i = 1 (enforced internally).

    Args:
        query: natural language base query
        genre_weights: e.g. {"Action": 0.7, "Comedy": 0.3}
        k: number of results
    Returns:
        list of {movieId, title, year, score}
    """
    total = sum(genre_weights.values())
    norm_weights = {g: w / total for g, w in genre_weights.items()} if total > 0 else {}

    # Get graph-space pivot from semantic query
    vec = encode_query(query).tolist()
    pivot = _run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', 20, $vec)
        YIELD node AS m, score
        WHERE m.embedding_graph IS NOT NULL
        RETURN m.embedding_graph AS gvec
        ORDER BY score DESC LIMIT 1
    """, vec=vec)
    if not pivot:
        return []

    steered = np.array(pivot[0]["gvec"], dtype=np.float32)

    # Add weighted genre directions
    for genre_name, alpha in norm_weights.items():
        g = _run("MATCH (g:Genre {name: $n}) RETURN g.embedding_graph AS emb",
                 n=genre_name)
        if g and g[0].get("emb"):
            steered = steered + alpha * np.array(g[0]["emb"], dtype=np.float32)

    steered = steered / (np.linalg.norm(steered) + 1e-8)

    return _run("""
        CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.movieId AS movieId, m.title AS title,
               m.year AS year, score
        ORDER BY score DESC LIMIT $k
    """, k=k, vec=steered.tolist())


def what_decade_does_this_feel_like(movie_title: str) -> list[dict]:
    """
    USES: embedding_graph (movie) + Decade.embedding_graph
    Given a movie, find which decade its graph embedding is closest to.
    Reveals whether a movie "feels" older or newer than its release year.
    E.g. a 2015 film scoring closest to 1980s node = has 80s structural feel.

    Args:
        movie_title: exact or partial title match
    Returns:
        list of {decade, score, actual_year} sorted by score desc
    """
    movie = _run("""
        MATCH (m:Movie)
        WHERE m.title IS NOT NULL
          AND toLower(toString(m.title)) CONTAINS toLower($t)
          AND m.embedding_graph IS NOT NULL
        RETURN m.title AS title, m.year AS year,
               m.embedding_graph AS gvec
        ORDER BY size(toString(m.title)) ASC LIMIT 1
    """, t=movie_title)

    if not movie:
        return []

    gvec       = np.array(movie[0]["gvec"], dtype=np.float32)
    actual_year = movie[0]["year"]
    title       = movie[0]["title"]

    decades = _run("""
        MATCH (dc:Decade) WHERE dc.embedding_graph IS NOT NULL
        RETURN dc.label AS decade, dc.embedding_graph AS emb
        ORDER BY dc.label
    """)

    scored = []
    for d in decades:
        d_emb = np.array(d["emb"], dtype=np.float32)
        score = float(np.dot(gvec, d_emb))
        scored.append({"decade": d["decade"], "score": score, "actual_year": actual_year})

    scored.sort(key=lambda x: -x["score"])
    print(f"\n  '{title}' (actual year: {actual_year})")
    for s in scored:
        marker = " ← feels like this" if s == scored[0] else ""
        print(f"    {s['decade']}  {s['score']:.4f}{marker}")
    return scored


def director_style_map(min_movies: int = 2) -> dict:
    """
    USES: Director.embedding_graph
    Project director graph embeddings to 2D using UMAP.
    Directors with overlapping filmographies cluster together.
    Returns coordinates for visualisation (e.g. scatter plot).

    Args:
        min_movies: only include directors with >= N movies in Neo4j
    Returns:
        dict with keys: directors (list of names), coords (N x 2 array),
                        embeddings (N x 256 array)
    """
    try:
        import umap
    except ImportError:
        print("Install umap-learn: pip install umap-learn")
        return {}

    rows = _run("""
        MATCH (d:Director)
        WHERE d.embedding_graph IS NOT NULL AND d.name IS NOT NULL
        WITH d, size([(d)<-[:DIRECTED_BY]-() | 1]) AS n_movies
        WHERE n_movies >= $min_movies
        RETURN d.name AS name, d.directorId AS did,
               d.embedding_graph AS emb, n_movies
        ORDER BY n_movies DESC
    """, min_movies=min_movies)

    if not rows:
        print("No directors with embeddings found.")
        return {}

    names = [r["name"] for r in rows]
    embs  = np.array([r["emb"] for r in rows], dtype=np.float32)
    n_movies = [r["n_movies"] for r in rows]

    reducer = umap.UMAP(n_components=2, random_state=42, n_neighbors=min(15, len(names)-1))
    coords  = reducer.fit_transform(embs)

    return {
        "directors": names,
        "n_movies":  n_movies,
        "coords":    coords,       # shape (N, 2) — plot with matplotlib/plotly
        "embeddings": embs
    }


def connector_movie(selected_movie_ids: list[int], k: int = TOP_K,
                    embedding_space: str = "graph") -> list[dict]:
    """
    USES: embedding_graph OR embedding_semantic (controlled by embedding_space arg)
    Find the movies whose embeddings are closest to the centroid of a set
    of selected movies. The "consensus picks" for a group.

    embedding_space: "graph" (structural similarity) or "semantic" (thematic similarity)

    Args:
        selected_movie_ids: list of movieId integers (user selections)
        k: number of connector movies to return
        embedding_space: "graph" or "semantic"
    Returns:
        list of {movieId, title, year, score}
    """
    emb_prop  = "embedding_graph" if embedding_space == "graph" else "embedding_semantic"
    index_name = "movie_graph_idx" if embedding_space == "graph" else "movie_semantic_idx"

    rows = _run(f"""
        MATCH (m:Movie)
        WHERE m.movieId IN $ids AND m.{emb_prop} IS NOT NULL
        RETURN m.movieId AS movieId, m.{emb_prop} AS emb
    """, ids=selected_movie_ids)

    if not rows:
        return []

    embs = np.array([r["emb"] for r in rows], dtype=np.float32)
    centroid = embs.mean(axis=0)
    centroid = centroid / (np.linalg.norm(centroid) + 1e-8)

    return _run(f"""
        CALL db.index.vector.queryNodes($idx, $k, $vec)
        YIELD node AS m, score
        WHERE NOT m.movieId IN $ids
        RETURN m.movieId AS movieId, m.title AS title,
               m.year AS year, score
        ORDER BY score DESC LIMIT $k
    """, idx=index_name, k=k + len(selected_movie_ids),
         vec=centroid.tolist(), ids=selected_movie_ids)[:k]


def era_slider(query: str, decade_label: str,
               alpha: float = 0.8, n_pivots: int = 5,
               k: int = TOP_K) -> list[dict]:
    """
    USES: embedding_semantic (pivots) + embedding_graph (retrieval) + Decade.embedding_graph (steering)
    Find movies matching the query that also feel like they're from a target decade.

    Steering formula:
        pivot_graph = mean of top-N semantic hits' graph embeddings
        steered = normalize(pivot_graph + alpha * decade_emb[decade_label])

    Why multi-pivot: a single pivot locks onto one structural neighborhood.
    Averaging N pivots gives a more representative graph-space anchor for
    the query, making the decade steering more reliable.

    alpha controls the era strength. Default 0.8 is intentionally strong —
    at 0.5 the pivot dominates and the decade signal is too weak to escape
    dominant movies in the pivot's cluster. At 0.8 the decade direction
    has enough weight to pull results toward that era.

    Args:
        query: natural language base query
        decade_label: e.g. "1970s", "1990s", "2010s"
        alpha: steering strength — recommend 0.7-1.0 for era to be visible
        n_pivots: number of semantic hits to average for graph anchor
        k: number of results
    Returns:
        list of {movieId, title, year, score}
    """
    vec = encode_query(query).tolist()

    pivots = _run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', $n, $vec)
        YIELD node AS m, score
        WHERE m.embedding_graph IS NOT NULL
        RETURN m.embedding_graph AS gvec
        ORDER BY score DESC LIMIT $n
    """, n=n_pivots * 3, vec=vec)
    pivots = [p for p in pivots if p.get("gvec")][:n_pivots]

    if not pivots:
        return []

    avg_pivot = np.mean([np.array(p["gvec"], dtype=np.float32) for p in pivots], axis=0)
    avg_pivot /= (np.linalg.norm(avg_pivot) + 1e-8)

    decade = _run("MATCH (dc:Decade {label: $l}) RETURN dc.embedding_graph AS emb",
                  l=decade_label)
    if not decade or not decade[0].get("emb"):
        print(f"Decade '{decade_label}' not found or has no embedding")
        return []

    d_emb   = np.array(decade[0]["emb"], dtype=np.float32)
    steered = avg_pivot + alpha * d_emb
    steered = steered / (np.linalg.norm(steered) + 1e-8)

    return _run("""
        CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.movieId AS movieId, m.title AS title,
               m.year AS year, score
        ORDER BY score DESC LIMIT $k
    """, k=k, vec=steered.tolist())


def group_preference_map(user_liked_movies: dict[str, list[int]],
                         user_preference_weight: float = 0.1) -> dict:
    """
    USES: embedding_graph AND embedding_semantic (both, separately reported)

    Computes a taste vector per user (mean of their liked movies' embeddings,
    then L2-normalised to a unit vector). Then computes pairwise cosine
    similarity between every pair of users in both embedding spaces.

    ── How the percentage is computed ───────────────────────────────────────
    Step 1: for user Alice with liked movies [Matrix, Inception, Blade Runner]:
        fetch each movie's embedding_graph → shape (3, 256)
        row-wise mean → shape (256,)   ← "average taste position"
        L2-normalise → unit vector     ← direction only, not magnitude

    Step 2: cosine_sim(Alice, Bob) = dot(alice_vec, bob_vec)
        because both are unit vectors, dot product == cosine similarity.
        Range: [-1, +1]

    Step 3: displayed as percentage = cosine_sim * 100

    ── What negative cosine similarity means ────────────────────────────────
    Cosine similarity measures the ANGLE between two vectors, not distance.
      +1.0 (100%): vectors point in exactly the same direction
                   → users like structurally identical films
       0.0 (  0%): vectors are 90° apart — orthogonal, no relationship
                   → tastes are completely unrelated, neither similar nor opposite
      -1.0 (-100%): vectors point in opposite directions
                   → users like films on opposite ends of the embedding space

    In practice you will rarely see values below -0.2 for real users because
    most films share at least some structural elements (language, era, etc.).
    Values below 0 are meaningful: it means the users' average taste positions
    pull in genuinely opposite structural directions — e.g. one user likes
    only 1920s silent films and the other likes only 2020s CGI blockbusters.

    The function stores both the raw cosine similarity AND a 0-100 rescaled
    version. The rescaled version maps [-1,+1] → [0,100] linearly:
        rescaled = (cosine_sim + 1) / 2 * 100
    This is more intuitive for UI display — 0% means opposite, 50% means
    unrelated, 100% means identical. The raw cosine is kept for math downstream.

    Args:
        user_liked_movies: {"Alice": [movieId1, ...], "Bob": [...], ...}
        user_preference_weight: stored in output for downstream steering use
    Returns:
        dict with:
          graph_similarity:        pairwise raw cosine sim [-1,+1] as percentage
          semantic_similarity:     same for semantic space
          graph_similarity_scaled: pairwise [0,100] rescaled (for UI display)
          semantic_similarity_scaled: same
          taste_vectors_graph:     {user: 256-dim numpy unit vector}
          taste_vectors_semantic:  {user: 256-dim numpy unit vector}
          liked_counts:            {user: how many movies had embeddings}
          user_preference_weight:  passed through for downstream use
    """
    taste_graph = {}
    taste_sem   = {}
    liked_counts = {}

    for user, movie_ids in user_liked_movies.items():
        rows = _run("""
            MATCH (m:Movie) WHERE m.movieId IN $ids
              AND m.embedding_graph IS NOT NULL
              AND m.embedding_semantic IS NOT NULL
            RETURN m.movieId AS movieId,
                   m.embedding_graph AS gvec,
                   m.embedding_semantic AS svec
        """, ids=movie_ids)

        if not rows:
            continue

        liked_counts[user] = len(rows)
        g_embs = np.array([r["gvec"] for r in rows], dtype=np.float32)
        s_embs = np.array([r["svec"] for r in rows], dtype=np.float32)

        g_mean = g_embs.mean(axis=0); g_mean /= (np.linalg.norm(g_mean) + 1e-8)
        s_mean = s_embs.mean(axis=0); s_mean /= (np.linalg.norm(s_mean) + 1e-8)

        taste_graph[user] = g_mean
        taste_sem[user]   = s_mean

    users = list(taste_graph.keys())

    def pairwise(taste_dict):
        """Returns (raw_dict, scaled_dict) both as percentage-formatted floats."""
        raw, scaled = {}, {}
        for u1 in users:
            raw[u1], scaled[u1] = {}, {}
            for u2 in users:
                cos = float(np.dot(taste_dict[u1], taste_dict[u2]))
                raw[u1][u2]    = round(cos * 100, 1)
                # Rescale [-1,+1] → [0,100] for UI display
                scaled[u1][u2] = round((cos + 1) / 2 * 100, 1)
        return raw, scaled

    if len(users) > 1:
        g_raw, g_scaled = pairwise(taste_graph)
        s_raw, s_scaled = pairwise(taste_sem)
    else:
        g_raw = g_scaled = s_raw = s_scaled = {}

    return {
        "graph_similarity":           g_raw,
        "semantic_similarity":        s_raw,
        "graph_similarity_scaled":    g_scaled,    # use this for UI display
        "semantic_similarity_scaled": s_scaled,    # use this for UI display
        "taste_vectors_graph":        taste_graph,
        "taste_vectors_semantic":     taste_sem,
        "liked_counts":               liked_counts,
        "user_preference_weight":     user_preference_weight,
    }


def filtered_search(query: str, genres: list[str] = None,
                    decade: str = None, min_rating: float = None,
                    k: int = TOP_K) -> list[dict]:
    """
    USES: embedding_semantic
    Semantic ANN + hard structural filters via Cypher WHERE.
    Fetches 100 ANN candidates then filters — increase inner_k if results are sparse.

    Args:
        query: natural language search string
        genres: list of genre names to require (AND logic between genres)
        decade: e.g. "1990s" (requires IN_DECADE edge)
        min_rating: minimum avg_rating value
        k: number of results to return
    Returns:
        list of {movieId, title, year, score, avg_rating}
    """
    vec = encode_query(query).tolist()

    where_clauses = ["1=1"]
    if genres:
        for g in genres:
            where_clauses.append(
                f"EXISTS {{ MATCH (m)-[:HAS_GENRE]->(g:Genre {{name: '{g}'}}) }}"
            )
    if decade:
        where_clauses.append(
            f"EXISTS {{ MATCH (m)-[:IN_DECADE]->(dc:Decade {{label: '{decade}'}}) }}"
        )
    if min_rating and RATING_PROP:
        where_clauses.append(f"m.{RATING_PROP} >= {min_rating}")

    where_str = " AND ".join(where_clauses)
    rating_return = f"m.{RATING_PROP} AS avg_rating," if RATING_PROP else ""

    return _run(f"""
        CALL db.index.vector.queryNodes('movie_semantic_idx', 100, $vec)
        YIELD node AS m, score
        WHERE {where_str}
        RETURN m.movieId AS movieId, m.title AS title,
               m.year AS year, {rating_return} score
        ORDER BY score DESC LIMIT $k
    """, vec=vec, k=k)


def hybrid_search(query: str, lam: float = 0.6,
                  n_pivots: int = 5, k: int = TOP_K) -> list[dict]:
    """
    USES: embedding_semantic + embedding_graph
    Blend semantic and graph scores using multi-pivot graph vector.
    hybrid_score = lam * semantic_score + (1 - lam) * graph_score

    Uses score floor (not 0) for movies absent from the graph pool,
    preserving meaningful ranking across both lists.

    Args:
        query: natural language search string
        lam: weight for semantic score (1-lam goes to graph)
        n_pivots: pivots to average for graph-space query vector
        k: number of results
    Returns:
        list of {movieId, title, year, score, sem_score, graph_score, in_graph}
    """
    vec = encode_query(query).tolist()

    sem_rows = _run("""
        CALL db.index.vector.queryNodes('movie_semantic_idx', $k, $vec)
        YIELD node AS m, score
        WHERE m.embedding_graph IS NOT NULL
        RETURN m.movieId AS movieId, m.title AS title, m.year AS year,
               score AS sem_score, m.embedding_graph AS gvec
    """, k=k * 4, vec=vec)

    if not sem_rows:
        return []

    # Multi-pivot graph vector
    pivot_embs = [np.array(r["gvec"], dtype=np.float32)
                  for r in sem_rows[:n_pivots] if r.get("gvec")]
    if not pivot_embs:
        return sorted(sem_rows, key=lambda x: -x["sem_score"])[:k]

    avg_pivot = np.mean(pivot_embs, axis=0)
    avg_pivot /= (np.linalg.norm(avg_pivot) + 1e-8)

    graph_rows = _run("""
        CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
        YIELD node AS m, score
        RETURN m.movieId AS movieId, score AS graph_score
    """, k=k * 4, vec=avg_pivot.tolist())

    graph_scores = {r["movieId"]: r["graph_score"] for r in graph_rows}
    floor = min(graph_scores.values()) * 0.9 if graph_scores else 0.0

    results = []
    for r in sem_rows:
        mid     = r["movieId"]
        s_score = r["sem_score"]
        g_score = graph_scores.get(mid, floor)
        results.append({
            **r,
            "score":       lam * s_score + (1 - lam) * g_score,
            "sem_score":   s_score,
            "graph_score": g_score,
            "in_graph":    mid in graph_scores,
        })

    return sorted(results, key=lambda x: -x["score"])[:k]


# ================================================================
# EXPERIMENTS
# ================================================================

def experiment_single_vs_multi_pivot(queries: list[str],
                                      n_pivots_list: list[int] = [1, 3, 5, 10],
                                      k: int = TOP_K) -> dict:
    """
    EXPERIMENT 1:
    Compare single-pivot vs multi-pivot graph search.

    For each query and each n_pivots value:
      - Compute the graph query vector (single or averaged pivots)
      - Measure: vector distance from single-pivot vector
      - Measure: result overlap with single-pivot results (Jaccard)

    Answers: does averaging more pivots meaningfully change the
    graph query direction and/or the final results?

    Args:
        queries: list of query strings to benchmark
        n_pivots_list: pivot counts to compare
        k: result set size for overlap measurement
    Returns:
        per-query benchmark dict
    """
    results = {}

    for q in queries:
        vec = encode_query(q).tolist()
        results[q] = {"pivot_experiments": {}}

        # Get all candidate pivot graph vectors
        pivots = _run("""
            CALL db.index.vector.queryNodes('movie_semantic_idx', 30, $vec)
            YIELD node AS m, score
            WHERE m.embedding_graph IS NOT NULL
            RETURN m.title AS title, m.embedding_graph AS gvec, score
            ORDER BY score DESC LIMIT 15
        """, vec=vec)
        pivots = [p for p in pivots if p.get("gvec")]

        if not pivots:
            continue

        # Single pivot (n=1) is the baseline
        baseline_vec = np.array(pivots[0]["gvec"], dtype=np.float32)
        baseline_vec /= (np.linalg.norm(baseline_vec) + 1e-8)

        baseline_results = _run("""
            CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
            YIELD node AS m, score
            RETURN m.movieId AS movieId, m.title AS title, score
            ORDER BY score DESC LIMIT $k
        """, k=k, vec=baseline_vec.tolist())
        baseline_ids = {r["movieId"] for r in baseline_results}

        results[q]["pivot_1"] = {
            "pivot_title": pivots[0]["title"],
            "top_results": [r["title"] for r in baseline_results],
        }

        for n in [p for p in n_pivots_list if p > 1]:
            if n > len(pivots):
                continue
            multi_vecs = [np.array(p["gvec"], dtype=np.float32) for p in pivots[:n]]
            avg_vec = np.mean(multi_vecs, axis=0)
            avg_vec /= (np.linalg.norm(avg_vec) + 1e-8)

            # Vector distance from baseline
            cosine_dist = 1.0 - float(np.dot(baseline_vec, avg_vec))

            multi_results = _run("""
                CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
                YIELD node AS m, score
                RETURN m.movieId AS movieId, m.title AS title, score
                ORDER BY score DESC LIMIT $k
            """, k=k, vec=avg_vec.tolist())
            multi_ids = {r["movieId"] for r in multi_results}

            jaccard = len(baseline_ids & multi_ids) / len(baseline_ids | multi_ids) \
                      if (baseline_ids | multi_ids) else 0.0

            results[q]["pivot_experiments"][f"n={n}"] = {
                "cosine_distance_from_single_pivot": round(cosine_dist, 4),
                "jaccard_overlap_with_single_pivot": round(jaccard, 4),
                "top_results": [r["title"] for r in multi_results],
                "pivot_titles": [p["title"] for p in pivots[:n]],
            }

    return results


def experiment_semantic_as_graph_query(queries: list[str],
                                        k: int = TOP_K) -> dict:
    """
    EXPERIMENT 2:
    What happens if we use the semantic query embedding directly as a
    graph-space query vector (without going through a pivot)?

    Two comparisons:
      A) Semantic search results (embedding_semantic index)
      B) Multi-pivot graph search results (embedding_graph index, 5 pivots)
      C) Semantic-as-graph: use raw semantic vector directly in graph index

    For each query, measures:
      - Vector distance: ||sem_vec - avg_graph_pivot_vec||
      - Result overlap: Jaccard(B ∩ C) / Jaccard(B ∪ C)
      - Qualitative: prints all three result lists side by side

    Answers: are the two 256-dim spaces aligned? Would skipping the
    pivot step (cheaper, no extra Neo4j call) give similar results?

    Args:
        queries: list of query strings
        k: result set size
    Returns:
        benchmark dict per query
    """
    results = {}

    for q in queries:
        sem_vec = encode_query(q)
        sem_vec_list = sem_vec.tolist()

        # A: pure semantic search
        sem_results = _run("""
            CALL db.index.vector.queryNodes('movie_semantic_idx', $k, $vec)
            YIELD node AS m, score
            RETURN m.movieId AS movieId, m.title AS title, score AS sem_score
            ORDER BY score DESC LIMIT $k
        """, k=k, vec=sem_vec_list)

        # B: multi-pivot graph search (5 pivots)
        pivots = _run("""
            CALL db.index.vector.queryNodes('movie_semantic_idx', 15, $vec)
            YIELD node AS m, score
            WHERE m.embedding_graph IS NOT NULL
            RETURN m.embedding_graph AS gvec
            ORDER BY score DESC LIMIT 5
        """, vec=sem_vec_list)
        pivots = [p for p in pivots if p.get("gvec")]

        graph_pivot_vec = None
        if pivots:
            arrs = [np.array(p["gvec"], dtype=np.float32) for p in pivots]
            graph_pivot_vec = np.mean(arrs, axis=0)
            graph_pivot_vec /= (np.linalg.norm(graph_pivot_vec) + 1e-8)

        pivot_results = []
        if graph_pivot_vec is not None:
            pivot_results = _run("""
                CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
                YIELD node AS m, score
                RETURN m.movieId AS movieId, m.title AS title, score AS graph_score
                ORDER BY score DESC LIMIT $k
            """, k=k, vec=graph_pivot_vec.tolist())

        # C: semantic vector used directly in graph index
        sem_as_graph_results = _run("""
            CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
            YIELD node AS m, score
            RETURN m.movieId AS movieId, m.title AS title, score AS direct_score
            ORDER BY score DESC LIMIT $k
        """, k=k, vec=sem_vec_list)

        # Compute distances and overlaps
        vec_dist_B_C = None
        if graph_pivot_vec is not None:
            vec_dist_B_C = round(float(1.0 - np.dot(graph_pivot_vec, sem_vec)), 4)

        ids_B = {r["movieId"] for r in pivot_results}
        ids_C = {r["movieId"] for r in sem_as_graph_results}
        jaccard_B_C = len(ids_B & ids_C) / len(ids_B | ids_C) if (ids_B | ids_C) else 0.0

        results[q] = {
            "vector_distance_pivot_vs_direct": vec_dist_B_C,
            "jaccard_pivot_vs_direct":         round(jaccard_B_C, 4),
            "semantic_results":       [r["title"] for r in sem_results],
            "graph_pivot_results":    [r["title"] for r in pivot_results],
            "semantic_as_graph":      [r["title"] for r in sem_as_graph_results],
        }

    return results


# ================================================================
# PRINT HELPERS
# ================================================================

def _print_movie_list(rows, label="", score_key="score"):
    if label:
        print(f"\n  {label}")
    for r in rows:
        title = r.get("title","?")[:48]
        year  = str(r.get("year",""))[:6]
        score = r.get(score_key, 0)
        print(f"    {title:<48} {year:<6}  {score:.4f}")

def _print_experiment_1(results: dict):
    print("\n" + "="*70)
    print("EXPERIMENT 1 — Single vs Multi-Pivot Graph Search")
    print("="*70)
    for q, data in results.items():
        print(f"\n  Query: \"{q}\"")
        p1 = data.get("pivot_1", {})
        print(f"  Pivot-1 ({p1.get('pivot_title','')}): {p1.get('top_results',[])[:3]}")
        for key, exp in data.get("pivot_experiments", {}).items():
            print(f"  {key}: vec_dist={exp['cosine_distance_from_single_pivot']:.4f}  "
                  f"jaccard={exp['jaccard_overlap_with_single_pivot']:.4f}  "
                  f"top3={exp['top_results'][:3]}")

def _print_experiment_2(results: dict):
    print("\n" + "="*70)
    print("EXPERIMENT 2 — Semantic Embedding Used Directly in Graph Index")
    print("="*70)
    for q, data in results.items():
        print(f"\n  Query: \"{q}\"")
        print(f"  vec_dist(pivot_vec, sem_vec) = {data['vector_distance_pivot_vs_direct']}")
        print(f"  jaccard(pivot_results, direct_results) = {data['jaccard_pivot_vs_direct']}")
        print(f"  Semantic:     {data['semantic_results'][:3]}")
        print(f"  Graph pivot:  {data['graph_pivot_results'][:3]}")
        print(f"  Sem-as-graph: {data['semantic_as_graph'][:3]}")
        if data["jaccard_pivot_vs_direct"] > 0.5:
            print("  → HIGH overlap: spaces are well-aligned for this query type")
        else:
            print("  → LOW overlap: spaces diverge — pivot step is important here")


# ================================================================
# MAIN TEST HARNESS
# ================================================================
if __name__ == "__main__":

    def header(t):
        print(f"\n{'='*65}\n  {t}\n{'='*65}")

    QUERIES = [
        "slow atmospheric sci-fi about communication with aliens",
        "feel-good animated family movie with humor and heart",
        "gritty 1970s crime drama with morally ambiguous characters",
    ]

    # ── Test 1: Semantic search ───────────────────────────────
    header("TEST 1 — SEMANTIC SEARCH")
    for q in QUERIES:
        print(f"\n  Query: \"{q}\"")
        _print_movie_list(semantic_search(q))

    # ── Test 2: Multi-pivot graph search ─────────────────────
    header("TEST 2 — MULTI-PIVOT GRAPH SEARCH (5 pivots)")
    for q in QUERIES:
        print(f"\n  Query: \"{q}\"")
        r = graph_search_multi_pivot(q, n_pivots=5)
        print(f"  Pivots used: {r['pivots']}")
        print(f"  Pivot spread (std): {r['pivot_vector_std']:.4f}")
        _print_movie_list(r["results"])

    # ── Test 3: Genre bridge slider ───────────────────────────
    header("TEST 3 — GENRE BRIDGE SLIDER")
    q = "an exciting movie with great characters"
    print(f"\n  Base query: \"{q}\"")
    for combo in [
        {"Action": 1.0},
        {"Comedy": 1.0},
        {"Action": 0.7, "Comedy": 0.3},
        {"Horror": 0.5, "Comedy": 0.5},
        {"Action": 0.4, "Drama": 0.4, "Comedy": 0.2},
    ]:
        print(f"\n  ── Genre weights: {combo} ──")
        _print_movie_list(steer_by_genres(q, combo))

    # ── Test 4: What decade does this feel like ───────────────
    header("TEST 4 — WHAT DECADE DOES THIS FEEL LIKE?")
    for title in ["The Matrix", "Arrival", "Blade Runner 2049",
                  "Mad Max: Fury Road", "Toy Story"]:
        what_decade_does_this_feel_like(title)

    # ── Test 5: Director style steering ──────────────────────
    header("TEST 5 — DIRECTOR STYLE STEERING")
    # Strategy: use the director's OWN films as graph-space pivots.
    # This avoids the text query landing in a horror/thriller region
    # that has nothing to do with the director's actual style.
    # We average the graph embeddings of the director's top films,
    # then steer that centroid further using the director entity embedding.
    # The text query is only used to bias which of the director's
    # films we anchor on (films most semantically similar to the query).
    q = "a tense cerebral thriller"
    DIRECTOR_ALPHA = 0.3   # low — director's own films are already the anchor
    for dname in ["Christopher Nolan", "Stanley Kubrick",
                  "Steven Spielberg", "Quentin Tarantino"]:
        d = _run("MATCH (d:Director {name:$n}) RETURN d.directorId AS did, "
                 "d.embedding_graph IS NOT NULL AS has_emb", n=dname)
        if not d:
            print(f"\n  '{dname}' not in Neo4j"); continue
        if not d[0]["has_emb"]:
            print(f"\n  '{dname}' has no embedding"); continue

        did = d[0]["did"]

        # Pivot = mean graph embedding of this director's films
        dir_films = _run("""
            MATCH (d:Director {directorId:$did})<-[:DIRECTED_BY]-(m:Movie)
            WHERE m.embedding_graph IS NOT NULL
            RETURN m.embedding_graph AS gvec, m.title AS title
        """, did=did)
        if not dir_films:
            print(f"\n  No films with graph embeddings for {dname}"); continue

        film_vecs = np.array([r["gvec"] for r in dir_films], dtype=np.float32)
        pivot_vec = film_vecs.mean(axis=0)
        pivot_vec /= (np.linalg.norm(pivot_vec) + 1e-8)

        # Steer lightly with director entity embedding
        d_emb = _run("MATCH (d:Director {directorId:$did}) RETURN d.embedding_graph AS emb",
                     did=did)
        de = np.array(d_emb[0]["emb"], dtype=np.float32)
        steered = pivot_vec + DIRECTOR_ALPHA * de
        steered /= (np.linalg.norm(steered) + 1e-8)

        rows = _run("""
            CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
            YIELD node AS m, score
            WHERE NOT (m)-[:DIRECTED_BY]->(:Director {directorId: $did})
            RETURN m.title AS title, m.year AS year, score
            ORDER BY score DESC LIMIT $k
        """, k=TOP_K + 5, vec=steered.tolist(), did=did)[:TOP_K]

        film_titles = [r["title"] for r in dir_films[:5]]
        print(f"\n  ── Style of {dname} ({len(dir_films)} films) ──")
        print(f"     Anchored on: {film_titles}")
        _print_movie_list(rows)

    # ── Test 6: Era slider ────────────────────────────────────
    header("TEST 6 — ERA SLIDER")
    q = "a great drama with powerful performances"
    # alpha=0.5 was too weak — Dionysus in '69 dominated every decade.
    # Higher alpha forces the decade direction to actually compete.
    # Also test with multi-pivot: average 5 semantic pivots before
    # steering, so the drama query anchor is more representative.
    for decade, alpha in [("1970s", 0.8), ("1990s", 0.8), ("2010s", 0.8)]:
        print(f"\n  ── '{q}' → {decade} (α={alpha}, multi-pivot) ──")
        # Multi-pivot version of era_slider
        vec = encode_query(q).tolist()
        pivots = _run("""
            CALL db.index.vector.queryNodes('movie_semantic_idx', 20, $vec)
            YIELD node AS m, score
            WHERE m.embedding_graph IS NOT NULL
            RETURN m.embedding_graph AS gvec
            ORDER BY score DESC LIMIT 5
        """, vec=vec)
        if not pivots:
            continue
        avg_pivot = np.mean([np.array(p["gvec"], dtype=np.float32) for p in pivots], axis=0)
        avg_pivot /= (np.linalg.norm(avg_pivot) + 1e-8)

        d_emb = _run("MATCH (dc:Decade {label:$l}) RETURN dc.embedding_graph AS emb",
                     l=decade)
        if not d_emb or not d_emb[0].get("emb"):
            print(f"  Decade {decade} not found"); continue

        d_vec   = np.array(d_emb[0]["emb"], dtype=np.float32)
        steered = avg_pivot + alpha * d_vec
        steered /= (np.linalg.norm(steered) + 1e-8)

        rows = _run("""
            CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
            YIELD node AS m, score
            RETURN m.title AS title, m.year AS year, score
            ORDER BY score DESC LIMIT $k
        """, k=TOP_K, vec=steered.tolist())
        _print_movie_list(rows)

    # ── Test 7: Connector movie ───────────────────────────────
    header("TEST 7 — CONNECTOR MOVIE (group consensus)")
    # Simulate a group selecting a few movies
    group_picks = _run("""
        MATCH (m:Movie) WHERE m.title IN
          ['The Matrix', 'Pulp Fiction', 'Fargo']
          AND m.movieId IS NOT NULL
        RETURN m.movieId AS movieId, m.title AS title
    """)
    if group_picks:
        ids = [r["movieId"] for r in group_picks]
        print(f"\n  Group selected: {[r['title'] for r in group_picks]}")
        print("  Connector movies (graph):")
        _print_movie_list(connector_movie(ids, embedding_space="graph"))
        print("  Connector movies (semantic):")
        _print_movie_list(connector_movie(ids, embedding_space="semantic"))

    # ================================================================
    # TESTS 8A–8F — GROUP PREFERENCE MAP (comprehensive)
    # ================================================================
    # Each test uses a different user configuration to stress a
    # different aspect of the similarity measurement.
    # ================================================================

    def _lookup_ids(titles: list[str]) -> list[int]:
        """Resolve a list of title strings to movieIds (partial match, case-insensitive)."""
        ids = []
        for t in titles:
            r = _run("""
                MATCH (m:Movie)
                WHERE m.title IS NOT NULL
                  AND toLower(toString(m.title)) CONTAINS toLower($t)
                  AND m.embedding_graph IS NOT NULL
                RETURN m.movieId AS mid, m.title AS title
                ORDER BY size(toString(m.title)) ASC LIMIT 1
            """, t=t)
            if r:
                ids.append(r[0]["mid"])
            else:
                print(f"    ⚠️  '{t}' not found")
        return ids

    def _print_group_map(pref_map: dict, label: str = ""):
        """Print both raw and scaled similarity for every user pair."""
        users = list(pref_map["taste_vectors_graph"].keys())
        if label:
            print(f"\n  {label}")

        g_raw    = pref_map["graph_similarity"]
        s_raw    = pref_map["semantic_similarity"]
        g_scaled = pref_map["graph_similarity_scaled"]
        s_scaled = pref_map["semantic_similarity_scaled"]
        counts   = pref_map["liked_counts"]

        for u in users:
            print(f"    {u}: {counts.get(u, 0)} movies with embeddings")

        print(f"\n    {'Pair':<20}  {'Graph raw':>10}  {'Graph 0-100':>12}  "
              f"{'Sem raw':>10}  {'Sem 0-100':>12}  Interpretation")
        print(f"    {'-'*90}")

        for i, u1 in enumerate(users):
            for u2 in users[i+1:]:
                gr  = g_raw[u1][u2]
                gs  = g_scaled[u1][u2]
                sr  = s_raw[u1][u2]
                ss  = s_scaled[u1][u2]

                # Interpretation of graph raw score
                if gr >= 70:    interp = "nearly identical taste"
                elif gr >= 40:  interp = "overlapping taste"
                elif gr >= 10:  interp = "some common ground"
                elif gr >= -10: interp = "unrelated tastes"
                elif gr >= -40: interp = "diverging tastes"
                else:           interp = "opposite ends of the space"

                print(f"    {u1+' ↔ '+u2:<20}  {gr:>9.1f}%  {gs:>11.1f}%  "
                      f"{sr:>9.1f}%  {ss:>11.1f}%  {interp}")

        # Gap between spaces: large gap means semantic conflates real differences
        if len(users) == 2:
            u1, u2 = users[0], users[1]
            gap = s_raw[u1][u2] - g_raw[u1][u2]
            if abs(gap) > 15:
                print(f"\n    ⚡ Space gap: semantic is {gap:+.1f}% higher than graph")
                print(f"       → semantic is conflating quality/language signal")
                print(f"         across structurally different taste profiles")

    header("TEST 8A — VERY SIMILAR USERS (same genre taste)")
    # Two users who both like sci-fi blockbusters — expect high similarity
    alice_ids = _lookup_ids(["The Matrix", "Inception", "Interstellar",
                              "Blade Runner 2049", "Arrival"])
    bob_ids   = _lookup_ids(["2001: A Space Odyssey", "Contact",
                              "Solaris", "Moon", "Ex Machina"])
    if alice_ids and bob_ids:
        pm = group_preference_map({"Alice (sci-fi)": alice_ids,
                                    "Bob (sci-fi)":   bob_ids})
        _print_group_map(pm, "Both users like thoughtful sci-fi")

    header("TEST 8B — CLEARLY DIFFERENT USERS (opposite genres)")
    # Alice: dark adult dramas. Bob: family animation. Expect low graph sim.
    alice_ids = _lookup_ids(["Schindler's List", "Requiem for a Dream",
                              "American History X", "Prisoners", "Zodiac"])
    bob_ids   = _lookup_ids(["Toy Story", "Finding Nemo", "The Lion King",
                              "Shrek", "Up"])
    if alice_ids and bob_ids:
        pm = group_preference_map({"Alice (dark drama)":   alice_ids,
                                    "Bob (family animation)": bob_ids})
        _print_group_map(pm, "Dark drama vs family animation")

    header("TEST 8C — THREE USERS, MIXED OVERLAP")
    alice_ids = _lookup_ids(["Pulp Fiction", "Reservoir Dogs", "Fargo",
                              "No Country for Old Men", "Blood Simple"])
    bob_ids   = _lookup_ids(["The Godfather", "Goodfellas", "Scarface",
                              "Heat", "Casino"])
    carol_ids = _lookup_ids(["Toy Story", "Finding Nemo", "Monsters Inc",
                              "The Incredibles", "WALL-E"])
    if alice_ids and bob_ids and carol_ids:
        pm = group_preference_map({
            "Alice (indie crime)":    alice_ids,
            "Bob (gangster)":         bob_ids,
            "Carol (Pixar)":          carol_ids,
        })
        _print_group_map(pm, "3 users: indie crime / gangster / Pixar")
        # Alice and Bob should be closer to each other than either is to Carol

    header("TEST 8D — SINGLE OVERLAPPING MOVIE")
    # Users share just one film — how much does one shared film matter?
    alice_ids = _lookup_ids(["Pulp Fiction", "Kill Bill: Vol. 1",
                              "Reservoir Dogs", "Django Unchained"])
    bob_ids   = _lookup_ids(["Pulp Fiction", "The Notebook",
                              "Pride & Prejudice", "La La Land"])
    if alice_ids and bob_ids:
        pm = group_preference_map({"Alice (Tarantino fan)":    alice_ids,
                                    "Bob (romance + 1 Tarantino)": bob_ids})
        _print_group_map(pm, "Shared film (Pulp Fiction) vs very different other picks")

    header("TEST 8E — NEGATIVE / NEAR-ZERO SIMILARITY TEST")
    # Designed to find a near-zero or negative cosine similarity.
    # Old silent films vs modern superhero films — maximally different structure.
    alice_ids = _lookup_ids(["Metropolis", "Nosferatu", "The General",
                              "Battleship Potemkin", "Cabinet of Dr. Caligari"])
    bob_ids   = _lookup_ids(["Avengers: Endgame", "Iron Man",
                              "Captain America", "Thor", "Spider-Man"])
    if alice_ids and bob_ids:
        pm = group_preference_map({"Alice (1920s silent)":   alice_ids,
                                    "Bob (MCU)": bob_ids})
        _print_group_map(pm,
            "Silent cinema vs MCU — testing for near-zero or negative graph similarity")

    header("TEST 8F — CONNECTOR MOVIE FOR EACH GROUP ABOVE")
    # For groups with low similarity, find the best connector film
    # using graph space (structural bridge) and compare with semantic.
    groups_to_connect = [
        ("Dark drama vs Animation", alice_ids, bob_ids),   # from 8B
    ]
    # Re-resolve 8B alice/bob since they may have changed
    alice_8b = _lookup_ids(["Schindler's List", "Requiem for a Dream",
                              "American History X", "Prisoners", "Zodiac"])
    bob_8b   = _lookup_ids(["Toy Story", "Finding Nemo", "The Lion King",
                              "Shrek", "Up"])
    alice_8c = _lookup_ids(["Pulp Fiction", "Reservoir Dogs", "Fargo",
                              "No Country for Old Men", "Blood Simple"])
    bob_8c   = _lookup_ids(["The Godfather", "Goodfellas", "Scarface",
                              "Heat", "Casino"])
    carol_8c = _lookup_ids(["Toy Story", "Finding Nemo", "Monsters Inc",
                              "The Incredibles", "WALL-E"])

    for label, g1, g2 in [
        ("Dark drama ↔ Animation",       alice_8b, bob_8b),
        ("Indie crime ↔ Gangster",        alice_8c, bob_8c),
        ("Gangster ↔ Pixar",              bob_8c, carol_8c),
        ("All three (crime+gangster+Pixar)", alice_8c + bob_8c, carol_8c),
    ]:
        all_ids = list(set(g1 + g2))
        if not all_ids:
            continue
        print(f"\n  Connector for '{label}':")
        graph_conn = connector_movie(all_ids, embedding_space="graph")
        sem_conn   = connector_movie(all_ids, embedding_space="semantic")
        print(f"    Graph:    ", end="")
        print(", ".join(f"{r['title']} ({int(r['year']) if r.get('year') else '?'})"
                        for r in graph_conn[:3]))
        print(f"    Semantic: ", end="")
        print(", ".join(f"{r['title']} ({int(r['year']) if r.get('year') else '?'})"
                        for r in sem_conn[:3]))

    header("TEST 8G — PREFERENCE-STEERED SEARCH (user taste vector applied)")
    # Take Alice's taste vector from 8C (indie crime) and apply it as a
    # light bias on a generic query. Then show the same query without bias.
    # Demonstrates the user_preference_weight mechanism.
    if alice_8c:
        pm_alice = group_preference_map({"Alice": alice_8c})
        alice_taste_g = pm_alice["taste_vectors_graph"].get("Alice")

        if alice_taste_g is not None:
            base_query = "an exciting movie with interesting characters"
            vec = encode_query(base_query).tolist()

            # Unbiased: pure semantic pivot → graph ANN
            piv = _run("""
                CALL db.index.vector.queryNodes('movie_semantic_idx', 20, $vec)
                YIELD node AS m WHERE m.embedding_graph IS NOT NULL
                RETURN m.embedding_graph AS gvec ORDER BY 1 LIMIT 1
            """, vec=vec)

            print(f"\n  Query: \"{base_query}\"")

            if piv:
                base_vec = np.array(piv[0]["gvec"], dtype=np.float32)

                # Unbiased
                rows_unbiased = _run("""
                    CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
                    YIELD node AS m, score
                    RETURN m.title AS title, m.year AS year, score
                    ORDER BY score DESC LIMIT $k
                """, k=TOP_K, vec=base_vec.tolist())

                # Biased with Alice's taste (weight=0.2)
                PREF_WEIGHT = 0.2
                biased_vec  = base_vec + PREF_WEIGHT * alice_taste_g
                biased_vec /= (np.linalg.norm(biased_vec) + 1e-8)
                rows_biased = _run("""
                    CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
                    YIELD node AS m, score
                    RETURN m.title AS title, m.year AS year, score
                    ORDER BY score DESC LIMIT $k
                """, k=TOP_K, vec=biased_vec.tolist())

                print(f"\n  Without preference bias:")
                _print_movie_list(rows_unbiased)
                print(f"\n  With Alice's indie-crime preference (weight={PREF_WEIGHT}):")
                _print_movie_list(rows_biased)

                overlap_titles = (set(r["title"] for r in rows_unbiased) &
                                  set(r["title"] for r in rows_biased))
                print(f"\n  Overlap: {len(overlap_titles)}/{TOP_K} films unchanged")
                if len(overlap_titles) < TOP_K // 2:
                    print("  → Preference bias significantly shifted results (good)")
                else:
                    print("  → Preference bias had minor effect — try higher weight")

    # ── Test 9: Filtered search ───────────────────────────────
    header("TEST 9 — FILTERED SEARCH")
    print("\n  Horror + isolation:")
    _print_movie_list(filtered_search("isolation and psychological horror",
                                      genres=["Horror"]))
    print("\n  Action 1990s:")
    _print_movie_list(filtered_search("high energy action blockbuster",
                                      genres=["Action"], decade="1990s"))
    if RATING_PROP:
        print(f"\n  Sci-Fi, {RATING_PROP} >= 3.5:")
        _print_movie_list(filtered_search("epic sci-fi adventure",
                                          genres=["Science Fiction"],
                                          min_rating=3.5))

    # ── Test 10: Hybrid ───────────────────────────────────────
    header("TEST 10 — HYBRID SEARCH (λ=0.6 sem, 0.4 graph, 5 pivots)")
    for q in QUERIES:
        print(f"\n  Query: \"{q}\"")
        for r in hybrid_search(q):
            tag = "G" if r["in_graph"] else "·"
            print(f"  [{tag}] {r['title']:<46} {str(r.get('year','')):<6}  "
                  f"hybrid={r['score']:.3f}  sem={r['sem_score']:.3f}  "
                  f"graph={r['graph_score']:.3f}")

    # ── Experiment 1: single vs multi pivot ──────────────────
    header("EXPERIMENT 1 — SINGLE VS MULTI-PIVOT")
    exp1 = experiment_single_vs_multi_pivot(QUERIES, n_pivots_list=[1, 3, 5, 10])
    _print_experiment_1(exp1)

    # ── Experiment 2: semantic vec as graph query ─────────────
    header("EXPERIMENT 2 — SEMANTIC EMBEDDING AS GRAPH QUERY")
    exp2 = experiment_semantic_as_graph_query(QUERIES)
    _print_experiment_2(exp2)

    # ── Director style map (UMAP) ────────────────────────────
    header("DIRECTOR STYLE MAP (UMAP)")
    try:
        dmap = director_style_map(min_movies=3)
        if dmap:
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(12, 10))
            coords = dmap["coords"]
            ax.scatter(coords[:, 0], coords[:, 1],
                       s=[n * 20 for n in dmap["n_movies"]],
                       alpha=0.6, c="#2563eb")
            for i, name in enumerate(dmap["directors"]):
                ax.annotate(name, coords[i], fontsize=6, alpha=0.8)
            ax.set_title("Director Style Map (UMAP of graph embeddings)")
            plt.tight_layout()
            plt.savefig("director_style_map.png", dpi=150)
            print(f"  Saved: director_style_map.png ({len(dmap['directors'])} directors)")
    except Exception as e:
        print(f"  UMAP plot skipped: {e}")

    driver.close()
    print("\n✅ All tests and experiments complete")