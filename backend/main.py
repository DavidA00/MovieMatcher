"""
MovieMatcher FastAPI backend — v2 (speed-first architecture)

FAST PATH:  /api/search      → pure Neo4j embedding search, <1 second
SLOW PATH:  /api/enrich      → batched LLM (reformulations + explanations), 3-8 seconds
            /api/reformulate → lightweight reformulation-only, 2-4 seconds

The frontend calls /search first, shows results immediately,
then calls /enrich in the background to add AI reasoning.
"""

import os, json, uuid, time, traceback, threading
from typing import Optional
from contextlib import asynccontextmanager
from collections import defaultdict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import numpy as np

# ================================================================
# NEO4J RESILIENT CONNECTION — monkey-patch before other imports
# ================================================================

import search_functions
from neo4j.exceptions import SessionExpired, ServiceUnavailable
from neo4j import GraphDatabase

_neo4j_lock = threading.Lock()

def _resilient_run(cypher, **params):
    """_run with retry on broken connections."""
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

# Patch
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

# Now import everything
from search_functions import (
    semantic_search, graph_search_multi_pivot, steer_by_genres,
    hybrid_search, filtered_search, connector_movie,
    group_preference_map, era_slider, encode_query,
    RATING_PROP,
)

# Patch movie_agents too
import movie_agents
movie_agents.neo4j_run = _resilient_run
neo4j_run = _resilient_run

from movie_agents import _FILTER_OPTIONS, make_llm, _extract_text, _parse_json

TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"

# Single LLM instance for all enrichment (flash = fast)
_llm = make_llm(temperature=0.5, pro=False)


# ================================================================
# SESSION STORE
# ================================================================

sessions: dict[str, dict] = defaultdict(lambda: {
    "user_id": "", "user_name": "",
    "liked_ids": [], "liked_titles": [],
    "disliked_ids": [], "disliked_titles": [],
    "genre_weights": {}, "decade_hint": None,
    "preference_intensity": 0.5,
    "search_history": [],
})


# ================================================================
# FAST SEARCH — pure Neo4j, no LLM
# ================================================================

def _fast_search(query: str, filters: dict, prefs: dict) -> tuple[list[dict], str]:
    """
    Direct search dispatch — same logic as movie_agents.search_node
    but called directly without LangGraph overhead.
    Returns (results, mode).
    """
    genre_weights = prefs.get("genre_weights", {})
    decade_hint = prefs.get("decade_hint")
    liked_ids = prefs.get("liked_ids", [])

    try:
        if genre_weights:
            return steer_by_genres(query, genre_weights, k=10), "genre_steered"
        elif decade_hint:
            return era_slider(query, decade_hint, alpha=0.8, k=10), "era_slider"
        elif filters.get("genres") or filters.get("decades"):
            return filtered_search(
                query,
                genres=filters.get("genres"),
                decade=(filters.get("decades") or [None])[0],
                min_rating=filters.get("min_rating"),
                k=10,
            ), "filtered"
        elif liked_ids:
            results = hybrid_search(query, lam=0.6, n_pivots=5, k=10)
            return results, "hybrid_personalized"
        else:
            return hybrid_search(query, lam=0.6, n_pivots=5, k=10), "hybrid"
    except Exception as e:
        print(f"[SEARCH] Primary search failed: {e}")
        try:
            return semantic_search(query, k=10), "semantic_fallback"
        except Exception as e2:
            print(f"[SEARCH] Fallback also failed: {e2}")
            return [], "error"


def _enrich_metadata(results: list[dict]) -> list[dict]:
    """Add poster URLs, genres, directors, overview from Neo4j."""
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
                p = r.get("poster_path", "")
                r["poster_url"] = f"{TMDB_IMG_BASE}{p}" if p and not p.startswith("http") else (p or None)
    except Exception as e:
        print(f"[META] Enrichment failed: {e}")

    return results


# ================================================================
# BATCHED LLM ENRICHMENT — single call for all movies
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


def _batched_enrich(query: str, results: list[dict], prefs: dict) -> dict:
    """
    Single LLM call to generate reformulations + explanations + filter suggestions.
    Much faster than 7 separate calls.
    """
    if not results:
        return {"reformulations": [], "explanations": {}, "filter_suggestions": {}}

    liked = prefs.get("liked_titles", [])[:5]
    disliked = prefs.get("disliked_titles", [])[:3]
    ctx_lines = []
    if liked: ctx_lines.append(f"Liked: {', '.join(liked)}")
    if disliked: ctx_lines.append(f"Disliked: {', '.join(disliked)}")
    user_context = "\n".join(ctx_lines) or "No preference history."

    # Build results block with clear numeric IDs
    results_block = "\n".join(
        f"- [{r.get('movieId')}] \"{r.get('title','?')}\" ({r.get('year','?')}) "
        f"[{', '.join((r.get('genres') or [])[:3])}] "
        f"— {(r.get('overview') or '')[:120]}"
        for r in results[:7]
    )

    # Build the actual movie ID list for the prompt
    id_list = ", ".join(f'"{r.get("movieId")}"' for r in results[:7])

    prompt = ENRICH_SYSTEM.format(
        query=query, results_block=results_block, user_context=user_context,
    )

    try:
        from langchain_core.messages import HumanMessage
        response = _llm.invoke([HumanMessage(content=prompt)])
        parsed = _parse_json(response.content)

        # Normalize explanation keys to strings of movieId
        raw_expls = parsed.get("explanations", {})
        explanations = {}
        for k, v in raw_expls.items():
            # Strip any prefix like "ID:" and convert to string
            clean_key = str(k).replace("ID:", "").strip()
            explanations[clean_key] = v

        return {
            "reformulations": parsed.get("reformulations", [])[:4],
            "explanations": explanations,
            "filter_suggestions": parsed.get("filter_suggestions", {}),
        }
    except Exception as e:
        print(f"[ENRICH] LLM error: {e}")
        traceback.print_exc()
        return {"reformulations": [], "explanations": {}, "filter_suggestions": {}}


# ================================================================
# FASTAPI APP
# ================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🎬 MovieMatcher API starting...")
    yield
    try: search_functions.driver.close()
    except: pass

app = FastAPI(title="MovieMatcher API", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


# ── Request models ────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str
    session_id: str = "default"
    genre_weights: dict[str, float] = Field(default_factory=dict)
    decade_hint: Optional[str] = None
    active_genres: list[str] = Field(default_factory=list)
    active_decades: list[str] = Field(default_factory=list)

class EnrichRequest(BaseModel):
    query: str
    session_id: str = "default"
    movie_ids: list[int] = Field(default_factory=list)

class FeedbackRequest(BaseModel):
    session_id: str
    movie_id: int
    movie_title: str
    action: str  # "like" | "dislike" | "clear"

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

class GroupReadyRequest(BaseModel):
    session_ids: list[str]
    user_labels: dict[str, str] = Field(default_factory=dict)


# ================================================================
# ENDPOINTS
# ================================================================

# ── Session / Join ────────────────────────────────────────────

@app.post("/api/join")
def join(req: JoinRequest):
    """Create a session for a user (Kahoot-style join)."""
    sid = str(uuid.uuid4())[:8]
    sessions[sid]["user_id"] = sid
    sessions[sid]["user_name"] = req.user_name
    return {"session_id": sid, "user_name": req.user_name}

@app.get("/api/session/{session_id}")
def get_session(session_id: str):
    s = sessions[session_id]
    return {
        "session_id": session_id,
        "user_name": s["user_name"],
        "liked": [{"id": i, "title": t} for i, t in zip(s["liked_ids"], s["liked_titles"])],
        "disliked": [{"id": i, "title": t} for i, t in zip(s["disliked_ids"], s["disliked_titles"])],
    }


# ── Filters ───────────────────────────────────────────────────

@app.get("/api/filters")
def get_filters():
    return {"genres": _FILTER_OPTIONS["genres"], "decades": _FILTER_OPTIONS["decades"]}


# ── FAST Search (no LLM) ─────────────────────────────────────

@app.post("/api/search")
def search(req: SearchRequest):
    """
    FAST search — pure Neo4j embeddings, no LLM calls.
    Returns results in <1 second. Frontend calls /enrich after.
    """
    t0 = time.time()
    s = sessions[req.session_id]

    if req.genre_weights: s["genre_weights"] = req.genre_weights
    if req.decade_hint: s["decade_hint"] = req.decade_hint

    prefs = {
        "liked_ids": s["liked_ids"],
        "disliked_ids": s["disliked_ids"],
        "genre_weights": req.genre_weights or s["genre_weights"],
        "decade_hint": req.decade_hint or s["decade_hint"],
    }
    filters = {
        "genres": req.active_genres if req.active_genres else None,
        "decades": req.active_decades if req.active_decades else None,
    }

    results, mode = _fast_search(req.query, filters, prefs)
    results = _enrich_metadata(results)

    top_score = max((r.get("score", r.get("sem_score", 0)) for r in results), default=0.0)
    s["search_history"].append(req.query)

    elapsed = time.time() - t0
    print(f"[SEARCH] '{req.query}' → {len(results)} results, mode={mode}, {elapsed:.2f}s")

    return {
        "search_results": results,
        "search_mode": mode,
        "top_score": top_score,
        "elapsed_ms": int(elapsed * 1000),
    }


# ── SLOW Enrich (batched LLM) ────────────────────────────────

@app.post("/api/enrich")
def enrich(req: EnrichRequest):
    """
    Batched LLM enrichment — reformulations + explanations + filter suggestions
    in a SINGLE Gemini call. Called by frontend after search results are displayed.
    Takes 3-8 seconds. Never returns 500 — gracefully degrades.
    """
    t0 = time.time()
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

        prefs = {
            "liked_titles": s["liked_titles"],
            "disliked_titles": s["disliked_titles"],
        }

        enrichment = _batched_enrich(req.query, results, prefs)

        elapsed = time.time() - t0
        print(f"[ENRICH] '{req.query}' → {len(enrichment.get('reformulations',[]))} refs, "
              f"{len(enrichment.get('explanations',{}))} expls, {elapsed:.2f}s")

        return enrichment
    except Exception as e:
        print(f"[ENRICH] Endpoint error: {e}")
        traceback.print_exc()
        return empty


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
               CASE
                 WHEN n1:Decade THEN n1.label
                 ELSE n1.name
               END AS node_name
    """, mid=req.movie_id)
    if not rows: raise HTTPException(404, "Movie not found")

    nodes = [{"id": f"movie_{req.movie_id}", "name": rows[0]["center_title"],
              "type": "Movie", "isCenter": True}]
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


# ── Mixer ─────────────────────────────────────────────────────

@app.post("/api/mixer")
def mixer(req: MixerRequest):
    results = connector_movie(req.movie_ids, k=req.k, embedding_space=req.embedding_space)
    results = _enrich_metadata(results)
    return {"connectors": results, "source_ids": req.movie_ids}


# ── Group ─────────────────────────────────────────────────────

@app.post("/api/group/ready")
def group_ready(req: GroupReadyRequest):
    user_liked = {}
    for sid in req.session_ids:
        s = sessions[sid]
        label = req.user_labels.get(sid, s.get("user_name", sid))
        if s["liked_ids"]: user_liked[label] = s["liked_ids"]

    if len(user_liked) < 2:
        raise HTTPException(400, "Need at least 2 users with liked movies")

    pref_map = group_preference_map(user_liked)
    all_ids = list(set(mid for ids in user_liked.values() for mid in ids))
    connectors = connector_movie(all_ids, k=7, embedding_space="graph")
    connectors = _enrich_metadata(connectors)

    safe_map = {
        "graph_similarity_scaled": pref_map.get("graph_similarity_scaled", {}),
        "semantic_similarity_scaled": pref_map.get("semantic_similarity_scaled", {}),
        "liked_counts": pref_map.get("liked_counts", {}),
    }
    return {"group_similarity": safe_map, "connectors": connectors}


# ── Health ────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)