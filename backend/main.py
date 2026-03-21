"""
MovieMatcher FastAPI backend — v3

FAST: /api/search       → Neo4j embedding search + post-filter, <2s
SLOW: /api/enrich       → batched LLM enrichment, 3-8s
FAST: /api/reformulate  → query reformulations only, 1-3s
FAST: /api/movie/autocomplete → title search for mixer, <0.5s
"""

import os, json, uuid, time, traceback, threading
from typing import Optional
from contextlib import asynccontextmanager
from collections import defaultdict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import numpy as np

# ── Neo4j resilient connection ────────────────────────────────

import search_functions
from neo4j.exceptions import SessionExpired, ServiceUnavailable
from neo4j import GraphDatabase

_neo4j_lock = threading.Lock()

def _resilient_run(cypher, **params):
    for attempt in range(2):
        try:
            with search_functions.driver.session() as s:
                return s.run(cypher, **params).data()
        except (SessionExpired, ServiceUnavailable, OSError, BrokenPipeError) as e:
            print(f"[NEO4J] Retry {attempt+1}: {type(e).__name__}")
            with _neo4j_lock:
                try: search_functions.driver.close()
                except: pass
                search_functions.driver = GraphDatabase.driver(
                    search_functions.NEO4J_URI,
                    auth=(search_functions.NEO4J_USER, search_functions.NEO4J_PASSWORD),
                    max_connection_lifetime=200, keep_alive=True,
                )
            time.sleep(0.3)
    raise SessionExpired("Neo4j connection failed after retries")

search_functions._run = _resilient_run
try: search_functions.driver.close()
except: pass
search_functions.driver = GraphDatabase.driver(
    search_functions.NEO4J_URI,
    auth=(search_functions.NEO4J_USER, search_functions.NEO4J_PASSWORD),
    max_connection_lifetime=200, keep_alive=True,
)
search_functions.driver.verify_connectivity()
print("[NEO4J] Driver reconfigured with keepalive")

from search_functions import (
    semantic_search, graph_search_multi_pivot, steer_by_genres,
    hybrid_search, filtered_search, connector_movie,
    group_preference_map, era_slider, encode_query,
    RATING_PROP,
)
import movie_agents
movie_agents.neo4j_run = _resilient_run
neo4j_run = _resilient_run
from movie_agents import _FILTER_OPTIONS, make_llm, _extract_text, _parse_json

TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"
_llm = make_llm(temperature=0.5, pro=False)


# ================================================================
# SESSION STORE
# ================================================================

sessions: dict[str, dict] = defaultdict(lambda: {
    "user_id": "", "user_name": "", "party_name": "",
    "liked_ids": [], "liked_titles": [],
    "disliked_ids": [], "disliked_titles": [],
    "genre_weights": {}, "decade_hint": None,
    "preference_intensity": 0.5,
    "search_history": [],
})

# Party state for Fuse multiplayer
parties: dict[str, dict] = defaultdict(lambda: {
    "round": 1,
    "admin_sid": None,    # session_id of explicit creator/admin
    "users": {},          # {session_id: {name, ready, session_id}}
    "round_summaries": [],
    "fuse_in_progress": False,
    "secret": "",
})
MAX_PARTY_SIZE = 12


# ================================================================
# SEARCH ENGINE — post-filters hard genres/decades on any mode
# ================================================================

def _build_steered_vector(
    base_vec: np.ndarray,
    genre_weights: dict,
    decade_hints: list,
    query_weight: float = 1.0,
    genre_strength: float = 0.6,
    era_strength: float = 0.5,
) -> np.ndarray:
    """
    Compose a search vector from multiple signals with explicit weights.
    
    Unlike simple vector addition, this normalizes each component independently
    and blends them with controlled strengths, preventing any single signal
    from dominating the direction.
    
    Args:
        base_vec: The starting vector (query pivot, mixer centroid, or hybrid pivot)
        genre_weights: {genre_name: weight} for genre steering
        decade_hints: List of decade labels for era steering
        query_weight: How much the base query direction matters (0-1)
        genre_strength: How strongly genres pull the vector (0-1) 
        era_strength: How strongly decades pull the vector (0-1)
    
    Returns:
        Normalized composite vector in graph embedding space
    """
    # Start with the base query direction
    components = [(base_vec / (np.linalg.norm(base_vec) + 1e-8), query_weight)]
    
    # Add genre direction(s)
    if genre_weights:
        genre_vecs = []
        total_gw = sum(genre_weights.values()) or 1.0
        for gname, alpha in genre_weights.items():
            g = neo4j_run("MATCH (g:Genre {name: $n}) RETURN g.embedding_graph AS emb", n=gname)
            if g and g[0].get("emb"):
                gv = np.array(g[0]["emb"], dtype=np.float32)
                genre_vecs.append(gv * (alpha / total_gw))
        if genre_vecs:
            genre_dir = sum(genre_vecs)
            genre_dir = genre_dir / (np.linalg.norm(genre_dir) + 1e-8)
            components.append((genre_dir, genre_strength))
    
    # Add era direction(s)
    if decade_hints:
        era_vecs = []
        for dh in decade_hints[:3]:
            dc = neo4j_run("MATCH (dc:Decade {label: $l}) RETURN dc.embedding_graph AS emb", l=dh)
            if dc and dc[0].get("emb"):
                era_vecs.append(np.array(dc[0]["emb"], dtype=np.float32))
        if era_vecs:
            era_dir = np.mean(era_vecs, axis=0)
            era_dir = era_dir / (np.linalg.norm(era_dir) + 1e-8)
            components.append((era_dir, era_strength))
    
    # Weighted blend of all normalized component directions
    total_weight = sum(w for _, w in components)
    composite = np.zeros_like(base_vec)
    for vec, w in components:
        composite += (w / total_weight) * vec
    
    return composite / (np.linalg.norm(composite) + 1e-8)


def _graph_ann(vec: np.ndarray, k: int, exclude_ids: list[int] | None = None) -> list[dict]:
    """Run graph-space ANN search, optionally excluding specific movie IDs."""
    if exclude_ids:
        return neo4j_run("""
            CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
            YIELD node AS m, score
            WHERE NOT m.movieId IN $exclude
            RETURN m.movieId AS movieId, m.title AS title, m.year AS year, score
            ORDER BY score DESC LIMIT $k
        """, k=k, vec=vec.tolist(), exclude=exclude_ids)
    else:
        return neo4j_run("""
            CALL db.index.vector.queryNodes('movie_graph_idx', $k, $vec)
            YIELD node AS m, score
            RETURN m.movieId AS movieId, m.title AS title, m.year AS year, score
            ORDER BY score DESC LIMIT $k
        """, k=k, vec=vec.tolist())


def _apply_preference_rerank(
    results: list[dict], liked_ids: list[int], disliked_ids: list[int], intensity: float,
) -> list[dict]:
    """Re-rank results based on preference_intensity.
    0 = unchanged. 1 = strongly push toward liked, away from disliked."""
    if intensity <= 0.01 or (not liked_ids and not disliked_ids) or not results:
        return results

    try:
        liked_vec = None
        if liked_ids:
            lr = neo4j_run("MATCH (m:Movie) WHERE m.movieId IN $ids AND m.embedding_graph IS NOT NULL RETURN m.embedding_graph AS e", ids=liked_ids)
            if lr:
                embs = np.array([r["e"] for r in lr], dtype=np.float32)
                liked_vec = embs.mean(axis=0)
                liked_vec /= (np.linalg.norm(liked_vec) + 1e-8)

        disliked_vec = None
        if disliked_ids:
            dr = neo4j_run("MATCH (m:Movie) WHERE m.movieId IN $ids AND m.embedding_graph IS NOT NULL RETURN m.embedding_graph AS e", ids=disliked_ids)
            if dr:
                embs = np.array([r["e"] for r in dr], dtype=np.float32)
                disliked_vec = embs.mean(axis=0)
                disliked_vec /= (np.linalg.norm(disliked_vec) + 1e-8)

        if liked_vec is None and disliked_vec is None:
            return results

        rids = [r.get("movieId") for r in results if r.get("movieId")]
        if not rids:
            return results
        emb_rows = neo4j_run("MATCH (m:Movie) WHERE m.movieId IN $ids AND m.embedding_graph IS NOT NULL RETURN m.movieId AS mid, m.embedding_graph AS e", ids=rids)
        emb_map = {r["mid"]: np.array(r["e"], dtype=np.float32) for r in emb_rows}

        for r in results:
            mid = r.get("movieId")
            orig = r.get("score", r.get("sem_score", 0.5))
            if mid not in emb_map:
                r["score"] = orig
                continue
            mv = emb_map[mid]
            boost = 0.0
            if liked_vec is not None:
                boost += float(np.dot(mv, liked_vec))
            if disliked_vec is not None:
                boost -= 0.5 * float(np.dot(mv, disliked_vec))
            r["score"] = (1 - intensity) * orig + intensity * (orig + 0.3 * boost)

        results.sort(key=lambda x: -x.get("score", 0))
    except Exception as e:
        print(f"[PREF] Re-ranking failed: {e}")
    return results


def _fast_search(
    query: str,
    filters: dict,
    prefs: dict,
    lam: float = 0.6,
    mixer_weights: dict[str, float] | None = None,
    preference_intensity: float = 0.0,
    liked_ids: list[int] | None = None,
    disliked_ids: list[int] | None = None,
    k_fetch: int = 30,
    steering_strength: float = 0.6,
) -> tuple[list[dict], str]:
    """
    Unified search dispatch.
    
    Flow:
      1. Determine base vector (mixer centroid OR query semantic pivot OR hybrid)
      2. If genre_weights or decade_hints: compose steered vector via _build_steered_vector
      3. Run graph ANN with k_fetch candidates
      4. Post-filter by hard genre/decade/IMDB constraints
      5. Return top results
    """
    genre_weights = prefs.get("genre_weights", {})
    decade_hints = prefs.get("decade_hints", [])
    has_steering = bool(genre_weights) or bool(decade_hints)

    try:
        # ── MIXER MODE ────────────────────────────────────────
        if mixer_weights and len(mixer_weights) >= 2:
            movie_ids = [int(mid) for mid in mixer_weights.keys()]
            rows = neo4j_run("""
                MATCH (m:Movie) WHERE m.movieId IN $ids AND m.embedding_graph IS NOT NULL
                RETURN m.movieId AS movieId, m.embedding_graph AS emb
            """, ids=movie_ids)

            results = []
            if rows:
                emb_map = {r["movieId"]: np.array(r["emb"], dtype=np.float32) for r in rows}
                vecs, ws = [], []
                for mid_str, w in mixer_weights.items():
                    mid = int(mid_str)
                    if mid in emb_map:
                        vecs.append(emb_map[mid])
                        ws.append(w)
                if vecs:
                    ws_arr = np.array(ws, dtype=np.float32)
                    ws_arr /= ws_arr.sum() + 1e-8
                    base_vec = sum(w * v for v, w in zip(vecs, ws_arr))
                    
                    if has_steering:
                        vec = _build_steered_vector(base_vec, genre_weights, decade_hints,
                                                    genre_strength=steering_strength, era_strength=steering_strength)
                        mode = "mixer+genre_era" if (genre_weights and decade_hints) else \
                               "mixer+genre" if genre_weights else "mixer+era"
                    else:
                        vec = base_vec / (np.linalg.norm(base_vec) + 1e-8)
                        mode = "mixer"
                    
                    results = _graph_ann(vec, k_fetch, exclude_ids=movie_ids)
            else:
                mode = "mixer_no_data"

        # ── STEERED MODE (genre and/or era active, with text query) ──
        elif has_steering:
            from search_functions import encode_query as _enc
            sem_vec = _enc(query).tolist()
            
            # Get multiple graph-space pivots for a more stable base direction
            pivots = neo4j_run("""
                CALL db.index.vector.queryNodes('movie_semantic_idx', 20, $vec)
                YIELD node AS m, score
                WHERE m.embedding_graph IS NOT NULL
                RETURN m.embedding_graph AS gvec, score
                ORDER BY score DESC LIMIT 5
            """, vec=sem_vec)
            
            if pivots:
                # Weighted average of top-5 semantic matches as the base pivot
                pvecs = [np.array(p["gvec"], dtype=np.float32) for p in pivots]
                pscores = np.array([p["score"] for p in pivots], dtype=np.float32)
                pscores /= pscores.sum() + 1e-8
                base_vec = sum(s * v for v, s in zip(pvecs, pscores))
                
                # Compose: query pivot + genre direction + era direction
                vec = _build_steered_vector(base_vec, genre_weights, decade_hints,
                                            genre_strength=steering_strength, era_strength=steering_strength)
                
                if genre_weights and decade_hints:
                    mode = "genre_era_steered"
                elif genre_weights:
                    mode = "genre_steered"
                else:
                    mode = "era_steered"
                
                results = _graph_ann(vec, k_fetch)
            else:
                results = hybrid_search(query, lam=lam, n_pivots=5, k=k_fetch)
                mode = "hybrid_fallback"

        # ── DEFAULT: hybrid search ────────────────────────────
        else:
            results = hybrid_search(query, lam=lam, n_pivots=5, k=k_fetch)
            mode = "hybrid"

        # ── Post-filter by hard genre/decade constraints ──────
        hard_genres = set(filters.get("genres") or [])
        hard_decades = set(filters.get("decades") or [])

        if hard_genres or hard_decades:
            # Fetch genre/decade data for all result movies
            result_ids = [r.get("movieId") for r in results if r.get("movieId")]
            if result_ids:
                meta = neo4j_run("""
                    MATCH (m:Movie) WHERE m.movieId IN $ids
                    OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
                    OPTIONAL MATCH (m)-[:IN_DECADE]->(dc:Decade)
                    RETURN m.movieId AS movieId,
                           collect(DISTINCT g.name) AS genres,
                           collect(DISTINCT dc.label) AS decades
                """, ids=result_ids)
                meta_map = {r["movieId"]: r for r in meta}

                filtered = []
                for r in results:
                    mid = r.get("movieId")
                    m = meta_map.get(mid, {})
                    movie_genres = set(m.get("genres", []))
                    movie_decades = set(m.get("decades", []))

                    # Genre filter: movie must have ALL selected genres
                    if hard_genres and not hard_genres.issubset(movie_genres):
                        continue
                    # Decade filter: movie must be in ANY selected decade
                    if hard_decades and not hard_decades.intersection(movie_decades):
                        continue
                    filtered.append(r)

                results = filtered
                mode += "+filtered"

        return results[:10], mode

    except Exception as e:
        print(f"[SEARCH] Error: {e}")
        traceback.print_exc()
        try:
            return semantic_search(query, k=10), "semantic_fallback"
        except:
            return [], "error"


def _enrich_metadata(results: list[dict]) -> list[dict]:
    movie_ids = [r.get("movieId") for r in results if r.get("movieId")]
    if not movie_ids:
        return results
    try:
        meta_rows = neo4j_run("""
            MATCH (m:Movie) WHERE m.movieId IN $ids
            OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
            OPTIONAL MATCH (m)-[:DIRECTED_BY]->(d:Director)
            RETURN m.movieId AS movieId, m.poster_path AS poster_path,
                   m.overview AS overview, m.popularity AS popularity,
                   m.imdb_rating AS imdb_rating, m.imdb_votes AS imdb_votes,
                   m.year AS year,
                   collect(DISTINCT g.name) AS genres,
                   collect(DISTINCT d.name) AS directors
        """, ids=movie_ids)
        meta_map = {r["movieId"]: r for r in meta_rows}
        for r in results:
            mid = r.get("movieId")
            if mid and mid in meta_map:
                m = meta_map[mid]
                r["poster_path"] = m.get("poster_path")
                r["overview"] = m.get("overview", "")
                r["genres"] = m.get("genres", [])
                r["directors"] = m.get("directors", [])
                r["popularity"] = m.get("popularity")
                r["imdb_rating"] = m.get("imdb_rating")
                r["imdb_votes"] = m.get("imdb_votes")
                if not r.get("year"): r["year"] = m.get("year")
                p = r.get("poster_path", "")
                r["poster_url"] = f"{TMDB_IMG_BASE}{p}" if p and not p.startswith("http") else (p or None)
    except Exception as e:
        print(f"[META] Enrichment failed: {e}")
    return results


# ================================================================
# LLM ENRICHMENT
# ================================================================

ENRICH_SYSTEM = """You are a movie recommendation assistant. Given a user's search query and a list of movie results, generate:

1. REFORMULATIONS: 3 alternative search queries that refine the original along different dimensions (mood, era, style, theme, comparison). Each with a brief rationale.

2. EXPLANATIONS: For each movie, write 1-2 sentences explaining why it matches the search query. Use the numeric ID from the brackets [ID] as the key. Be specific — mention plot details, cast, or genre connections.

3. FILTER_SUGGESTIONS: Suggest 1-3 genre and decade filters that would help narrow results, based on the query.

USER CONTEXT:
{user_context}

SEARCH QUERY: "{query}"

RESULTS:
{results_block}

Respond in valid JSON only. No markdown fences.
{{
  "reformulations": [
    {{"query": "...", "dimension": "mood|era|style|theme|comparison", "rationale": "..."}}
  ],
  "explanations": {{
    "12345": "explanation text...",
    "67890": "explanation text..."
  }},
  "filter_suggestions": {{
    "genres": ["Genre1", "Genre2"],
    "decades": ["1990s"]
  }}
}}"""


def _batched_enrich(query: str, results: list[dict], prefs: dict, group_context: str = "") -> dict:
    if not results:
        return {"reformulations": [], "explanations": {}, "filter_suggestions": {}}

    liked = prefs.get("liked_titles", [])[:5]
    disliked = prefs.get("disliked_titles", [])[:3]
    ctx_lines = []
    if liked: ctx_lines.append(f"Liked: {', '.join(liked)}")
    if disliked: ctx_lines.append(f"Disliked: {', '.join(disliked)}")
    user_context = "\n".join(ctx_lines) or "No preference history."
    user_context += group_context  # append group context if available

    results_block = "\n".join(
        f"- [{r.get('movieId')}] \"{r.get('title','?')}\" ({r.get('year','?')}) "
        f"[{', '.join((r.get('genres') or [])[:3])}] "
        f"— {(r.get('overview') or '')[:120]}"
        for r in results[:10]
    )

    prompt = ENRICH_SYSTEM.format(
        query=query, results_block=results_block, user_context=user_context,
    )

    try:
        from langchain_core.messages import HumanMessage
        t0 = time.time()
        response = _llm.invoke([HumanMessage(content=prompt)])
        print(f"[ENRICH] Gemini responded in {time.time()-t0:.1f}s")
        parsed = _parse_json(response.content)

        raw_expls = parsed.get("explanations", {})
        explanations = {}
        for k, v in raw_expls.items():
            explanations[str(k).replace("ID:", "").strip()] = v

        return {
            "reformulations": parsed.get("reformulations", [])[:4],
            "explanations": explanations,
            "filter_suggestions": parsed.get("filter_suggestions", {}),
        }
    except Exception as e:
        print(f"[ENRICH] LLM error: {e}")
        traceback.print_exc()
        return {"reformulations": [], "explanations": {}, "filter_suggestions": {}}


REFORMULATE_PROMPT = """You are a movie search query specialist.
Given a partial or complete search query, suggest exactly 3 improved versions.
Each should refine the query along a DIFFERENT dimension.

DIMENSIONS: mood, era, style, theme, comparison

Query: "{query}"

Respond in valid JSON only. No markdown fences.
{{
  "reformulations": [
    {{"query": "...", "dimension": "mood", "rationale": "..."}},
    {{"query": "...", "dimension": "theme", "rationale": "..."}},
    {{"query": "...", "dimension": "comparison", "rationale": "..."}}
  ]
}}"""


# ================================================================
# FASTAPI APP
# ================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🎬 MovieMatcher API starting...")
    yield
    try: search_functions.driver.close()
    except: pass

app = FastAPI(title="MovieMatcher API", version="3.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])


# ── Models ────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str = ""
    session_id: str = "default"
    genre_weights: dict[str, float] = Field(default_factory=dict)
    decade_hints: list[str] = Field(default_factory=list)
    active_genres: list[str] = Field(default_factory=list)
    active_decades: list[str] = Field(default_factory=list)
    mixer_weights: dict[str, float] = Field(default_factory=dict)
    lam: float = 0.6
    pref_intensity: float = 0.0  # 0=explore, 1=heavily personalize
    imdb_min: Optional[float] = None
    imdb_max: Optional[float] = None
    sort_by: str = "relevance"
    sort_dir: str = "desc"
    k_fetch: int = 30  # how many candidates to fetch before filtering
    steering_strength: float = 0.6  # 0-1, how strongly genre/era steering pulls results

class EnrichRequest(BaseModel):
    query: str
    session_id: str = "default"
    movie_ids: list[int] = Field(default_factory=list)
    party_name: str = ""  # if set and round > 1, includes group context

class ReformulateRequest(BaseModel):
    query: str
    session_id: str = "default"
    party_name: str = ""

class FeedbackRequest(BaseModel):
    session_id: str
    movie_id: int
    movie_title: str
    action: str

class MovieDetailRequest(BaseModel):
    movie_id: int

class NeighborhoodRequest(BaseModel):
    movie_id: int
    depth: int = 1

class MixerRequest(BaseModel):
    movie_ids: list[int]
    embedding_space: str = "graph"
    k: int = 7

class JoinRequest(BaseModel):
    user_name: str
    party_secret: str

class CreatePartyRequest(BaseModel):
    user_name: str
    party_secret: str

class AutocompleteRequest(BaseModel):
    q: str
    session_id: str = "default"
    limit: int = 8


# ================================================================
# ENDPOINTS
# ================================================================

def _validate_party_secret(secret: str) -> str:
    s = (secret or "").strip()
    if not (8 <= len(s) <= 12):
        raise HTTPException(400, "Party secret must be 8-12 characters")
    has_letter = any(c.isalpha() for c in s)
    has_digit = any(c.isdigit() for c in s)
    if not (has_letter and has_digit):
        raise HTTPException(400, "Party secret must include letters and numbers")
    return s

def _join_party(user_name: str, party_name: str, sid: str):
    sessions[sid]["user_id"] = sid
    sessions[sid]["user_name"] = user_name
    sessions[sid]["party_name"] = party_name

    p = parties[party_name]

    # Check if this is a reconnecting user (same name, case-insensitive)
    is_reconnect = False
    old_sids = [sid_k for sid_k, u in p["users"].items()
                if u["name"].lower() == user_name.lower() and sid_k != sid]
    for old_sid in old_sids:
        is_reconnect = True
        if p["admin_sid"] == old_sid:
            p["admin_sid"] = sid
        del p["users"][old_sid]
        print(f"[PARTY] Removed stale session for '{user_name}' in '{party_name}'")

    if not is_reconnect and len(p["users"]) >= MAX_PARTY_SIZE:
        raise HTTPException(403, f"Party '{party_name}' is full ({MAX_PARTY_SIZE} players max)")

    p["users"][sid] = {
        "name": user_name,
        "ready": False,
        "session_id": sid,
    }
    return p

@app.post("/api/party/create")
def create_party(req: CreatePartyRequest):
    user_name = req.user_name.strip()
    if not user_name:
        raise HTTPException(400, "User name is required")
    party_secret = _validate_party_secret(req.party_secret)
    if party_secret in parties and parties[party_secret]["users"]:
        raise HTTPException(409, "Party secret already exists")

    sid = str(uuid.uuid4())[:8]
    p = parties[party_secret]
    p["secret"] = party_secret
    p["round"] = 1
    p["round_summaries"] = []
    p["fuse_in_progress"] = False
    p["users"] = {}
    p["admin_sid"] = sid

    _join_party(user_name, party_secret, sid)
    print(f"[PARTY] '{user_name}' created '{party_secret}'")
    return {
        "session_id": sid,
        "user_name": user_name,
        "party_name": party_secret,
        "round": p["round"],
        "is_admin": True,
    }

@app.post("/api/party/join")
def join_party(req: JoinRequest):
    user_name = req.user_name.strip()
    if not user_name:
        raise HTTPException(400, "User name is required")
    party_secret = _validate_party_secret(req.party_secret)
    if party_secret not in parties or not parties[party_secret]["users"]:
        raise HTTPException(404, "Party not found")
    p = parties[party_secret]
    if p.get("secret") != party_secret:
        raise HTTPException(403, "Invalid party secret")

    sid = str(uuid.uuid4())[:8]
    p = _join_party(user_name, party_secret, sid)

    print(f"[PARTY] {user_name} joined '{party_secret}' "
          f"(round {p['round']}, {len(p['users'])} users, admin={p['admin_sid'] == sid})")

    return {
        "session_id": sid,
        "user_name": user_name,
        "party_name": party_secret,
        "round": p["round"],
        "is_admin": p["admin_sid"] == sid,
    }

@app.get("/api/session/{session_id}")
def get_session(session_id: str):
    s = sessions[session_id]
    return {
        "session_id": session_id, "user_name": s["user_name"],
        "liked": [{"id": i, "title": t} for i, t in zip(s["liked_ids"], s["liked_titles"])],
        "disliked": [{"id": i, "title": t} for i, t in zip(s["disliked_ids"], s["disliked_titles"])],
    }

@app.get("/api/filters")
def get_filters():
    return {"genres": _FILTER_OPTIONS["genres"], "decades": _FILTER_OPTIONS["decades"]}


# ── Fast Search ───────────────────────────────────────────────

@app.post("/api/search")
def search(req: SearchRequest):
    t0 = time.time()
    s = sessions[req.session_id]

    prefs = {
        "liked_ids": s["liked_ids"],
        "disliked_ids": s["disliked_ids"],
        "genre_weights": req.genre_weights or {},
        "decade_hints": req.decade_hints or [],
    }
    filters = {
        "genres": req.active_genres or None,
        "decades": req.active_decades or None,
    }

    query = req.query or "popular well-rated movie"
    results, mode = _fast_search(
        query, filters, prefs,
        lam=req.lam,
        mixer_weights=req.mixer_weights or None,
        k_fetch=req.k_fetch,
        steering_strength=req.steering_strength,
    )
    results = _enrich_metadata(results)

    # ── Preference re-ranking ─────────────────────────────────
    # pref_intensity: 0 = ignore likes/dislikes, 1 = heavily re-rank
    if req.pref_intensity > 0.05 and (s["liked_ids"] or s["disliked_ids"]):
        # Get liked/disliked movie genres for overlap scoring
        pref_ids = s["liked_ids"] + s["disliked_ids"]
        if pref_ids:
            pref_meta = neo4j_run("""
                MATCH (m:Movie) WHERE m.movieId IN $ids
                OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
                OPTIONAL MATCH (m)-[:DIRECTED_BY]->(d:Director)
                RETURN m.movieId AS movieId,
                       collect(DISTINCT g.name) AS genres,
                       collect(DISTINCT d.name) AS directors
            """, ids=pref_ids)
            liked_set = set(s["liked_ids"])
            disliked_set = set(s["disliked_ids"])

            liked_genres = set()
            liked_dirs = set()
            disliked_genres = set()
            for pm in pref_meta:
                if pm["movieId"] in liked_set:
                    liked_genres.update(pm.get("genres", []))
                    liked_dirs.update(pm.get("directors", []))
                elif pm["movieId"] in disliked_set:
                    disliked_genres.update(pm.get("genres", []))

            alpha = req.pref_intensity
            for r in results:
                base_score = r.get("score", r.get("sem_score", 0.5))
                r_genres = set(r.get("genres", []))
                r_dirs = set(r.get("directors", []))

                # Boost for liked genre/director overlap
                liked_overlap = len(r_genres & liked_genres) + len(r_dirs & liked_dirs) * 2
                # Penalize for disliked genre overlap
                disliked_overlap = len(r_genres & disliked_genres)

                boost = alpha * 0.05 * (liked_overlap - disliked_overlap * 0.5)
                r["score"] = max(0, base_score + boost)

            # Re-sort by boosted score (only if sorting by relevance)
            if req.sort_by == "relevance":
                results.sort(key=lambda r: -r.get("score", 0))

    # ── IMDB range filter ─────────────────────────────────────
    if req.imdb_min is not None or req.imdb_max is not None:
        filtered = []
        for r in results:
            rating = r.get("imdb_rating")
            if rating is None:
                continue  # skip unrated movies when IMDB filter active
            if req.imdb_min is not None and rating < req.imdb_min:
                continue
            if req.imdb_max is not None and rating > req.imdb_max:
                continue
            filtered.append(r)
        results = filtered

    # ── Sort ──────────────────────────────────────────────────
    reverse = req.sort_dir == "desc"
    if req.sort_by == "imdb_rating":
        results.sort(key=lambda r: r.get("imdb_rating") or 0, reverse=reverse)
    elif req.sort_by == "year":
        results.sort(key=lambda r: r.get("year") or 0, reverse=reverse)
    elif req.sort_by == "title":
        results.sort(key=lambda r: (r.get("title") or "").lower(), reverse=reverse)
    # else: "relevance" — keep original score order

    top_score = max((r.get("score", r.get("sem_score", 0)) for r in results), default=0.0)
    elapsed = time.time() - t0

    # Track search history for Fuse
    if query and query != "popular well-rated movie":
        s["search_history"].append(query)
        if len(s["search_history"]) > 20:
            s["search_history"] = s["search_history"][-20:]

    print(f"[SEARCH] '{query}' → {len(results)} results, mode={mode}, {elapsed:.2f}s")

    return {
        "search_results": results,
        "search_mode": mode,
        "top_score": top_score,
        "elapsed_ms": int(elapsed * 1000),
    }


# ── Enrich ────────────────────────────────────────────────────

@app.post("/api/enrich")
def enrich(req: EnrichRequest):
    empty = {"reformulations": [], "explanations": {}, "filter_suggestions": {}}
    try:
        s = sessions[req.session_id]
        results = []
        if req.movie_ids:
            results = neo4j_run("""
                MATCH (m:Movie) WHERE m.movieId IN $ids
                OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
                RETURN m.movieId AS movieId, m.title AS title, m.year AS year,
                       m.overview AS overview, collect(DISTINCT g.name) AS genres
            """, ids=req.movie_ids)
        prefs = {"liked_titles": s["liked_titles"], "disliked_titles": s["disliked_titles"]}

        # Include group context if in round 2+
        group_context = ""
        if req.party_name and req.party_name in parties:
            p = parties[req.party_name]
            if p["round_summaries"]:
                last = p["round_summaries"][-1]["summary"]
                sims = last.get('similarities', '')
                diffs = last.get('differences', '')
                if isinstance(sims, list): sims = "; ".join(sims)
                if isinstance(diffs, list): diffs = "; ".join(diffs)
                group_context = (
                    f"\n\nGROUP CONTEXT (from previous round with other users):\n"
                    f"Shared tastes: {sims}\n"
                    f"Differences: {diffs}\n"
                    f"When writing explanations and reformulations, take the group's shared tastes "
                    f"into account. Reformulations should appeal to the group, not just this user."
                )

        enrichment = _batched_enrich(req.query, results, prefs, group_context=group_context)
        return enrichment
    except Exception as e:
        print(f"[ENRICH] Error: {e}")
        traceback.print_exc()
        return empty


# ── Reformulate ───────────────────────────────────────────────

@app.post("/api/reformulate")
def reformulate(req: ReformulateRequest):
    if len(req.query.strip()) < 5:
        return {"reformulations": []}
    try:
        from langchain_core.messages import HumanMessage
        t0 = time.time()

        # Build group context for round 2+
        group_addition = ""
        if req.party_name and req.party_name in parties:
            p = parties[req.party_name]
            if p["round_summaries"]:
                last = p["round_summaries"][-1]["summary"]
                sims = last.get('similarities', '')
                if isinstance(sims, list): sims = "; ".join(sims)
                group_addition = (
                    f"\n\nGROUP CONTEXT — this person is in a party with friends. "
                    f"Their shared tastes: {sims[:300]}\n"
                    f"Subtly steer 1-2 of the reformulations toward what the group might enjoy together, "
                    f"while keeping the others focused on this person's individual query."
                )

        prompt = REFORMULATE_PROMPT.format(query=req.query) + group_addition
        response = _llm.invoke([HumanMessage(content=prompt)])
        parsed = _parse_json(response.content)
        refs = parsed.get("reformulations", [])[:3]
        print(f"[REFORMULATE] '{req.query}' → {len(refs)} refs in {time.time()-t0:.1f}s")
        return {"reformulations": refs}
    except Exception as e:
        print(f"[REFORMULATE] Error: {e}")
        return {"reformulations": []}


# ── Movie Autocomplete (for mixer) ────────────────────────────

@app.post("/api/movie/autocomplete")
def movie_autocomplete(req: AutocompleteRequest):
    """Fast title search for the mixer. Liked movies appear first."""
    s = sessions[req.session_id]
    q = req.q.strip()

    results = []
    # Always include liked movies that match
    if s["liked_ids"]:
        liked_results = neo4j_run("""
            MATCH (m:Movie) WHERE m.movieId IN $ids
            RETURN m.movieId AS movieId, m.title AS title, m.year AS year,
                   m.poster_path AS poster_path
            ORDER BY m.title
        """, ids=s["liked_ids"])
        if q:
            liked_results = [r for r in liked_results if q.lower() in (r.get("title") or "").lower()]
        for r in liked_results:
            r["is_liked"] = True
            p = r.get("poster_path", "")
            r["poster_url"] = f"{TMDB_IMG_BASE}{p}" if p and not p.startswith("http") else None
        results.extend(liked_results)

    # Search by title
    if q and len(q) >= 2:
        search_results = neo4j_run("""
            MATCH (m:Movie)
            WHERE m.title IS NOT NULL
              AND toLower(toString(m.title)) CONTAINS toLower($q)
            RETURN m.movieId AS movieId, m.title AS title, m.year AS year,
                   m.poster_path AS poster_path
            ORDER BY m.popularity DESC
            LIMIT $limit
        """, q=q, limit=req.limit)

        seen_ids = {r["movieId"] for r in results}
        for r in search_results:
            if r["movieId"] not in seen_ids:
                r["is_liked"] = False
                p = r.get("poster_path", "")
                r["poster_url"] = f"{TMDB_IMG_BASE}{p}" if p and not p.startswith("http") else None
                results.append(r)

    return {"results": results[:req.limit]}


# ── Feedback ──────────────────────────────────────────────────

@app.post("/api/feedback")
def feedback(req: FeedbackRequest):
    s = sessions[req.session_id]
    if req.action == "like":
        if req.movie_id not in s["liked_ids"]:
            s["liked_ids"].append(req.movie_id)
            s["liked_titles"].append(req.movie_title)
        if req.movie_id in s["disliked_ids"]:
            idx = s["disliked_ids"].index(req.movie_id)
            s["disliked_ids"].pop(idx); s["disliked_titles"].pop(idx)
    elif req.action == "dislike":
        if req.movie_id not in s["disliked_ids"]:
            s["disliked_ids"].append(req.movie_id)
            s["disliked_titles"].append(req.movie_title)
        if req.movie_id in s["liked_ids"]:
            idx = s["liked_ids"].index(req.movie_id)
            s["liked_ids"].pop(idx); s["liked_titles"].pop(idx)
    elif req.action == "clear":
        if req.movie_id in s["liked_ids"]:
            idx = s["liked_ids"].index(req.movie_id)
            s["liked_ids"].pop(idx); s["liked_titles"].pop(idx)
        if req.movie_id in s["disliked_ids"]:
            idx = s["disliked_ids"].index(req.movie_id)
            s["disliked_ids"].pop(idx); s["disliked_titles"].pop(idx)
    return {
        "liked": [{"id": i, "title": t} for i, t in zip(s["liked_ids"], s["liked_titles"])],
        "disliked": [{"id": i, "title": t} for i, t in zip(s["disliked_ids"], s["disliked_titles"])],
    }


# ── Movie Detail ──────────────────────────────────────────────

@app.post("/api/movie/detail")
def movie_detail(req: MovieDetailRequest):
    rating_clause = f"m.{RATING_PROP} AS rating" if RATING_PROP else "null AS rating"
    rows = neo4j_run(f"""
        MATCH (m:Movie {{movieId: $mid}})
        OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
        OPTIONAL MATCH (m)-[:DIRECTED_BY]->(d:Director)
        OPTIONAL MATCH (m)-[:HAS_ACTOR]->(a:Actor)
        OPTIONAL MATCH (m)-[:HAS_KEYWORD]->(kw:Keyword)
        OPTIONAL MATCH (m)-[:IN_DECADE]->(dc:Decade)
        OPTIONAL MATCH (m)-[:IN_LANGUAGE]->(lang:Language)
        RETURN m.title AS title, m.year AS year, m.overview AS overview,
               m.poster_path AS poster_path, m.popularity AS popularity,
               m.imdb_rating AS imdb_rating, m.imdb_votes AS imdb_votes,
               {rating_clause},
               collect(DISTINCT g.name) AS genres,
               collect(DISTINCT d.name) AS directors,
               collect(DISTINCT a.name) AS actors,
               collect(DISTINCT kw.name) AS keywords,
               collect(DISTINCT dc.label) AS decades,
               collect(DISTINCT lang.name) AS languages
    """, mid=req.movie_id)
    if not rows: raise HTTPException(404, "Movie not found")
    movie = rows[0]
    p = movie.get("poster_path", "")
    movie["poster_url"] = f"{TMDB_IMG_BASE}{p}" if p and not p.startswith("http") else (p or None)
    movie["movieId"] = req.movie_id
    return movie


# ── Neighborhood ──────────────────────────────────────────────

@app.post("/api/movie/neighborhood")
def movie_neighborhood(req: NeighborhoodRequest):
    rows = neo4j_run("""
        MATCH (m:Movie {movieId: $mid})
        OPTIONAL MATCH (m)-[r1]->(n1)
        WHERE n1:Genre OR n1:Director OR n1:Actor OR n1:Keyword OR n1:Decade
        RETURN m.title AS center_title,
               type(r1) AS rel_type,
               labels(n1)[0] AS node_type,
               CASE WHEN n1:Decade THEN n1.label ELSE n1.name END AS node_name
    """, mid=req.movie_id)
    if not rows: raise HTTPException(404, "Movie not found")
    nodes = [{"id": f"movie_{req.movie_id}", "name": rows[0]["center_title"], "type": "Movie", "isCenter": True}]
    links = []
    seen = {f"movie_{req.movie_id}"}
    for r in rows:
        if not r.get("node_name"): continue
        nid = f"{r['node_type']}_{r['node_name']}"
        if nid not in seen:
            nodes.append({"id": nid, "name": r["node_name"], "type": r["node_type"], "isCenter": False})
            seen.add(nid)
        links.append({"source": f"movie_{req.movie_id}", "target": nid, "type": r["rel_type"]})
    return {"nodes": nodes, "links": links}


# ── FUSE: Party multiplayer system ────────────────────────────

FUSE_SUMMARY_PROMPT = """You are a movie taste analyst helping a group of friends find a movie to watch together.

For each person below, you see their liked movies, disliked movies, and recent search queries.

{users_block}

{previous_round_context}

ANALYSIS INSTRUCTIONS — be thorough and careful:
- Look for EXACT genre overlaps (e.g. both liked Action movies) but also SUBTLE thematic overlaps (e.g. one likes heist films, another likes clever plots — both enjoy "smart" movies)
- Consider the MOOD and TONE of liked movies, not just genre labels. Two people might like very different genres but share a love for dark humor, or for emotional depth, or for fast-paced thrills.
- Look at what they searched for — search queries reveal intent that liked/disliked movies alone don't capture.
- Consider ERA preferences — do they gravitate toward the same decades?
- Consider what they DISLIKE — shared dislikes are just as useful as shared likes for narrowing down.
- Don't force similarities that don't exist. If tastes are genuinely divergent, say so honestly.
- Think about directors, actors, and filmmaking style, not just plot/genre.

Respond in valid JSON only. No markdown fences.
{{
  "similarities": [
    "First shared taste point — be specific, mention names and movies as evidence",
    "Second shared taste point — look for non-obvious thematic connections",
    "Third point — consider mood, tone, era, or filmmaking style overlaps"
  ],
  "differences": [
    "First divergence — name the person and what they uniquely prefer",
    "Second divergence — be honest about conflicts between members",
    "Third point — note any strong dislikes that others enjoy"
  ],
  "group_query": "A rich, detailed search query (30-60 words is ideal — longer is better!) that describes the perfect movie for this group. Be very specific about genre, tone, era, themes, pacing, and style. This query will be used to search a movie vector database, so descriptive natural language works best. Example: 'A clever, fast-paced crime thriller from the 1990s or 2000s with dark humor, ensemble cast chemistry, and a twisty plot — something that rewards attention like a heist film but with emotional stakes and memorable dialogue'",
  "reasoning": "Why this query balances everyone's preferences. Which specific elements address which person's tastes."
}}"""

FUSE_GROUP_PERSPECTIVE_PROMPT = """You are analyzing how a specific movie would be received by a group of friends.

GROUP TASTE PROFILE:
{group_summary}

PARTY MEMBERS: {member_names}

MOVIE:
Title: "{title}" ({year}) [{genres}]
Overview: {overview}

Write 2-3 sentences about how this movie would land with the group. For EACH person mentioned, briefly say whether they'd likely enjoy it or not and why, based on the taste profile above. Be specific — reference their known preferences. No JSON, just plain conversational text."""


class PartyStatusRequest(BaseModel):
    session_id: str
    party_name: str

class ReadyRequest(BaseModel):
    session_id: str
    party_name: str
    ready: bool = True

class FuseRequest(BaseModel):
    party_name: str
    session_id: str  # who triggered fuse

class GroupPerspectiveRequest(BaseModel):
    party_name: str
    movie_ids: list[int] = Field(default_factory=list)


@app.post("/api/party/status")
def party_status(req: PartyStatusRequest):
    """Get current party state: users, ready status, round, summaries, admin."""
    p = parties[req.party_name]
    users = []
    for sid, u in p["users"].items():
        s = sessions.get(sid, {})
        users.append({
            "session_id": sid,
            "name": u["name"],
            "ready": u["ready"],
            "liked_count": len(s.get("liked_ids", [])),
            "disliked_count": len(s.get("disliked_ids", [])),
            "search_count": len(s.get("search_history", [])),
            "is_admin": sid == p["admin_sid"],
        })
    return {
        "party_name": req.party_name,
        "round": p["round"],
        "users": users,
        "admin_sid": p["admin_sid"],
        "all_ready": all(u["ready"] for u in p["users"].values()) and len(p["users"]) >= 2,
        "fuse_in_progress": p["fuse_in_progress"],
        "round_summaries": p["round_summaries"],
    }


class RemovePlayerRequest(BaseModel):
    session_id: str      # admin's session
    party_name: str
    target_sid: str      # who to remove

class LeavePartyRequest(BaseModel):
    session_id: str
    party_name: str

class LeaveAssignRequest(BaseModel):
    session_id: str
    party_name: str
    new_admin_sid: str

class CancelPartyRequest(BaseModel):
    session_id: str
    party_name: str

@app.post("/api/party/remove")
def party_remove(req: RemovePlayerRequest):
    """Admin removes a player from the party."""
    p = parties[req.party_name]
    if p["admin_sid"] != req.session_id:
        raise HTTPException(403, "Only the party admin can remove players")
    if req.target_sid == req.session_id:
        raise HTTPException(400, "Cannot remove yourself")
    if req.target_sid not in p["users"]:
        raise HTTPException(404, "Player not found in party")

    removed_name = p["users"][req.target_sid]["name"]
    del p["users"][req.target_sid]
    print(f"[PARTY] Admin removed '{removed_name}' from '{req.party_name}'")
    return party_status(PartyStatusRequest(session_id=req.session_id, party_name=req.party_name))

@app.post("/api/party/leave")
def party_leave(req: LeavePartyRequest):
    p = parties[req.party_name]
    if req.session_id not in p["users"]:
        raise HTTPException(404, "You are not in this party")
    if p["admin_sid"] == req.session_id:
        raise HTTPException(400, "Admin must assign a new admin or cancel the party")
    name = p["users"][req.session_id]["name"]
    del p["users"][req.session_id]
    sessions.pop(req.session_id, None)
    print(f"[PARTY] {name} left '{req.party_name}'")
    return {"status": "left"}

@app.post("/api/party/leave_assign")
def party_leave_assign(req: LeaveAssignRequest):
    p = parties[req.party_name]
    if p["admin_sid"] != req.session_id:
        raise HTTPException(403, "Only admin can transfer admin role")
    if req.new_admin_sid == req.session_id:
        raise HTTPException(400, "Pick another user as new admin")
    if req.new_admin_sid not in p["users"]:
        raise HTTPException(404, "New admin is not in this party")
    old_name = p["users"][req.session_id]["name"]
    new_name = p["users"][req.new_admin_sid]["name"]
    p["admin_sid"] = req.new_admin_sid
    del p["users"][req.session_id]
    sessions.pop(req.session_id, None)
    print(f"[PARTY] {old_name} left '{req.party_name}' and assigned admin to '{new_name}'")
    return {"status": "left_assigned", "new_admin_sid": req.new_admin_sid}

@app.post("/api/party/cancel")
def party_cancel(req: CancelPartyRequest):
    p = parties[req.party_name]
    if p["admin_sid"] != req.session_id:
        raise HTTPException(403, "Only admin can cancel the party")
    sids = list(p["users"].keys())
    for sid in sids:
        sessions.pop(sid, None)
    parties.pop(req.party_name, None)
    print(f"[PARTY] Cancelled '{req.party_name}'")
    return {"status": "cancelled"}


@app.post("/api/party/ready")
def party_ready(req: ReadyRequest):
    """Toggle user's ready status."""
    p = parties[req.party_name]
    if req.session_id in p["users"]:
        p["users"][req.session_id]["ready"] = req.ready
        name = p["users"][req.session_id]["name"]
        print(f"[PARTY] {name} is {'READY' if req.ready else 'not ready'} in '{req.party_name}'")
    return party_status(PartyStatusRequest(session_id=req.session_id, party_name=req.party_name))


@app.post("/api/party/fuse")
def party_fuse(req: FuseRequest):
    """
    Run the Fuse: generate group summary, search query, and movie suggestions.
    Called when all users are ready.
    """
    p = parties[req.party_name]
    if p["fuse_in_progress"]:
        return {"status": "already_in_progress"}

    p["fuse_in_progress"] = True
    t0 = time.time()

    try:
        # Build user profiles for the LLM
        user_lines = []
        for sid, u in p["users"].items():
            s = sessions.get(sid, {})
            liked = s.get("liked_titles", [])
            disliked = s.get("disliked_titles", [])
            history = s.get("search_history", [])[-5:]  # last 5 searches
            user_lines.append(
                f"**{u['name']}**:\n"
                f"  Liked: {', '.join(liked) if liked else 'none yet'}\n"
                f"  Disliked: {', '.join(disliked) if disliked else 'none yet'}\n"
                f"  Recent searches: {', '.join(history) if history else 'none yet'}"
            )
        users_block = "\n\n".join(user_lines)

        # Include previous round context if round > 1
        prev_context = ""
        if p["round_summaries"]:
            last = p["round_summaries"][-1]
            sims = last['summary'].get('similarities', '')
            diffs = last['summary'].get('differences', '')
            if isinstance(sims, list): sims = "\n".join(f"- {s}" for s in sims)
            if isinstance(diffs, list): diffs = "\n".join(f"- {d}" for d in diffs)
            prev_context = (
                f"PREVIOUS ROUND SUMMARY (round {last['round']}):\n"
                f"Similarities:\n{sims}\n"
                f"Differences:\n{diffs}\n"
                f"Previous group query: {last['summary'].get('group_query', '')}\n"
                f"Take this into account — build on what was learned, don't repeat it."
            )

        prompt = FUSE_SUMMARY_PROMPT.format(
            users_block=users_block,
            previous_round_context=prev_context,
        )

        from langchain_core.messages import HumanMessage
        response = _llm.invoke([HumanMessage(content=prompt)])
        summary = _parse_json(response.content)

        # Use the group_query to search for suggestions
        group_query = summary.get("group_query", "popular well-rated movie everyone enjoys")
        print(f"[FUSE] Group query: '{group_query}'")

        suggestions_raw = hybrid_search(group_query, lam=0.6, n_pivots=5, k=15)
        suggestions_raw = _enrich_metadata(suggestions_raw)

        # Filter out movies anyone already liked/disliked
        all_seen_ids = set()
        for sid in p["users"]:
            s = sessions.get(sid, {})
            all_seen_ids.update(s.get("liked_ids", []))
            all_seen_ids.update(s.get("disliked_ids", []))
        suggestions = [r for r in suggestions_raw if r.get("movieId") not in all_seen_ids][:10]

        # Store round summary
        round_data = {
            "round": p["round"],
            "summary": summary,
            "suggestions": [{"movieId": r.get("movieId"), "title": r.get("title"), "year": r.get("year"),
                            "poster_url": r.get("poster_url"), "genres": r.get("genres", []),
                            "imdb_rating": r.get("imdb_rating"), "overview": (r.get("overview") or "")[:200]}
                           for r in suggestions],
        }
        p["round_summaries"].append(round_data)

        # Advance to next round, reset ready status
        p["round"] += 1
        for sid in p["users"]:
            p["users"][sid]["ready"] = False

        elapsed = time.time() - t0
        print(f"[FUSE] Round {round_data['round']} complete for '{req.party_name}' in {elapsed:.1f}s, "
              f"{len(suggestions)} suggestions")

        return {
            "status": "complete",
            "round_data": round_data,
            "new_round": p["round"],
        }
    except Exception as e:
        print(f"[FUSE] Error: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}
    finally:
        p["fuse_in_progress"] = False


@app.post("/api/party/group_perspective")
def group_perspective(req: GroupPerspectiveRequest):
    """Generate group perspective text for specific movies (round 2+ feature).
    Uses a single batched LLM call for efficiency."""
    p = parties[req.party_name]
    if not p["round_summaries"]:
        return {"perspectives": {}}

    last_summary = p["round_summaries"][-1]["summary"]
    sims = last_summary.get('similarities', '')
    diffs = last_summary.get('differences', '')
    # Handle both string and array formats
    if isinstance(sims, list): sims = "\n".join(f"- {s}" for s in sims)
    if isinstance(diffs, list): diffs = "\n".join(f"- {d}" for d in diffs)
    group_summary = f"Similarities:\n{sims}\nDifferences:\n{diffs}"
    member_names = ", ".join(u["name"] for u in p["users"].values())

    # Fetch movie metadata
    movies = neo4j_run("""
        MATCH (m:Movie) WHERE m.movieId IN $ids
        OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
        RETURN m.movieId AS movieId, m.title AS title, m.year AS year,
               m.overview AS overview, collect(DISTINCT g.name) AS genres
    """, ids=req.movie_ids)

    if not movies:
        return {"perspectives": {}}

    # Build a single batched prompt for all movies (much faster than 1 call per movie)
    movie_block = "\n".join(
        f"[{m.get('movieId')}] \"{m.get('title','?')}\" ({m.get('year','?')}) "
        f"[{', '.join(m.get('genres', [])[:3])}] — {(m.get('overview') or '')[:150]}"
        for m in movies[:8]
    )

    prompt = f"""You are analyzing how specific movies would be received by a group of friends.

GROUP TASTE PROFILE:
{group_summary}

PARTY MEMBERS: {member_names}

MOVIES:
{movie_block}

For EACH movie (use its [ID] as key), write 2-3 sentences about how it would land with the group.
Name each person and say whether they'd enjoy it and why, based on the taste profile.

Respond in valid JSON only. No markdown fences.
{{{{
  "ID1": "perspective text...",
  "ID2": "perspective text..."
}}}}"""

    try:
        from langchain_core.messages import HumanMessage
        t0 = time.time()
        resp = _llm.invoke([HumanMessage(content=prompt)])
        parsed = _parse_json(resp.content)
        # Normalize keys
        perspectives = {str(k).strip(): v for k, v in parsed.items()}
        print(f"[FUSE] Group perspectives for {len(perspectives)} movies in {time.time()-t0:.1f}s")
        return {"perspectives": perspectives}
    except Exception as e:
        print(f"[FUSE] Group perspective batch error: {e}")
        return {"perspectives": {}}


# ── Health ────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)