"""
movie_agents.py — v4
LangGraph + LangChain agent system for MovieMatcher.

SETUP
─────
pip install -U langchain-google-genai langchain langgraph langchain-community tavily-python
gcloud auth application-default login

.env:
  GOOGLE_CLOUD_PROJECT=your-project-id
  GOOGLE_CLOUD_LOCATION=us-central1
  TAVILY_API_KEY=tvly-...

FIXES IN v4
───────────
1. avg_rating: removed hardcoded property name; uses RATING_PROP from search_functions
   (auto-detected at startup — no more flooding warnings)
2. Explanation empty response: handle empty/None LLM content gracefully; cap overview
   to 150 chars to stay within token budget; increase max_output_tokens
3. Group summary truncation: max_output_tokens raised to 4096 for creative LLM
4. Cypher schema: corrected relationship arrow directions (Movie→Director, Movie→Actor);
   added actual genre list so model stops hallucinating 'Sci-Fi' etc.
5. Explanation model: gemini-2.5-pro for higher-quality grounded explanations
"""

import os, json, re, warnings
from typing import TypedDict, Literal, Optional

import numpy as np
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolMessage
# from langchain_community.tools.tavily_search import TavilySearchResults  # LangGraph pipeline — not used in final implementation
# from langgraph.graph import StateGraph, END                                 # LangGraph pipeline — not used in final implementation

warnings.filterwarnings("ignore")
load_dotenv()

# ── Model ─────────────────────────────────────────────────────────────────
PROJECT  = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
MODEL         = "gemini-2.5-flash"
MODEL_PRO     = "gemini-2.5-pro"    # used for explanation generation only


def make_llm(temperature: float = 0.5, pro: bool = False) -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=MODEL_PRO if pro else MODEL,
        project=PROJECT,
        location=LOCATION,
        temperature=temperature,
        max_output_tokens=4096,   # raised from 2048 — prevents JSON truncation
    )


# NOTE: The following LLM instances and web search tool were used exclusively by the
# LangGraph pipeline nodes. They are commented out since the pipeline was not used
# in the final implementation (see comment block further below).
#
# llm_structured = make_llm(temperature=0.5)
# llm_creative   = make_llm(temperature=1.0)
# llm_explain    = make_llm(temperature=0.5, pro=True)
#
# web_search = TavilySearchResults(
#     max_results=3,
#     search_depth="basic",
#     api_key=os.environ.get("TAVILY_API_KEY", ""),
# )

# ── Search functions ──────────────────────────────────────────────────────
try:
    from search_functions import (
        semantic_search, graph_search_multi_pivot, steer_by_genres,
        hybrid_search, filtered_search, what_decade_does_this_feel_like,
        connector_movie, group_preference_map, era_slider,
        encode_query, _run as neo4j_run, RATING_PROP,
    )
    SEARCH_AVAILABLE = True
except ImportError:
    SEARCH_AVAILABLE = False
    print("⚠️  search_functions.py not found — search nodes will be stubs")


# ================================================================
# UTILITY — extract text from any AIMessage content shape
# ================================================================
# Gemini sometimes returns content as:
#   - a plain string: "Hello"
#   - a list of blocks: [{"type": "text", "text": "Hello"}, ...]
#   - a list with a thinking block: [{"type": "thinking", ...}, {"type": "text", ...}]
# This helper normalises all shapes to a single string.
# It also optionally extracts thinking tokens for debug display.

def _extract_text(content, include_thinking: bool = False) -> str:
    """Normalise AIMessage.content → plain string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict):
                btype = block.get("type", "")
                if btype == "text":
                    parts.append(block.get("text", ""))
                elif btype == "thinking" and include_thinking:
                    parts.append(f"[THINKING]: {block.get('thinking','')}")
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(p for p in parts if p)
    return str(content)


def _strip_fences(text: str) -> str:
    """Strip ```json, ```cypher, ``` from LLM output."""
    return re.sub(r"```(?:json|cypher|)\s*", "", text).strip().strip("`").strip()


def _parse_json(content) -> dict | list:
    """Extract text from content, strip fences, parse JSON."""
    text = _extract_text(content)
    return json.loads(_strip_fences(text))


# ================================================================
# DYNAMIC FILTER OPTIONS — fetched from Neo4j at startup
# ================================================================
# Instead of hardcoding genre/decade lists, we fetch the top ones
# from the actual data. This means the filter suggestions always
# reflect what's actually in the database.

def _fetch_filter_options() -> dict:
    """Fetch top genres and decades from Neo4j for use in prompts."""
    if not SEARCH_AVAILABLE:
        return {"genres": [], "decades": []}
    try:
        genre_rows = neo4j_run("""
            MATCH (g:Genre)<-[:HAS_GENRE]-(m:Movie)
            RETURN g.name AS name, count(m) AS n
            ORDER BY n DESC LIMIT 20
        """)
        decade_rows = neo4j_run("""
            MATCH (dc:Decade)<-[:IN_DECADE]-(m:Movie)
            RETURN dc.label AS label, count(m) AS n
            ORDER BY dc.label
        """)
        genres  = [r["name"]  for r in genre_rows  if r.get("name")]
        decades = [r["label"] for r in decade_rows if r.get("label")]
        return {"genres": genres, "decades": decades}
    except Exception as e:
        print(f"Filter options fetch error: {e}")
        return {"genres": [], "decades": []}


# Fetch once at module load — these are stable
_FILTER_OPTIONS = _fetch_filter_options()
print(f"Filter options loaded: {len(_FILTER_OPTIONS['genres'])} genres, "
      f"{len(_FILTER_OPTIONS['decades'])} decades")



# ================================================================
# NOTE: The LangGraph pipeline below was the original intended architecture
# for MovieMatcher — a multi-node graph that ran reformulation, filter
# suggestion, search, unknown-movie lookup, explanation generation, and
# group summarisation as sequential graph nodes.
#
# It was commented out because the sequential per-node LLM calls were too
# slow for real-time use (8-15s per request). The final implementation in
# main.py replaces the entire pipeline with a single batched LLM call
# (_batched_enrich) and direct search_functions calls, bringing latency
# down to <2s for search and 3-8s for enrichment.
# ================================================================

# # ================================================================
# # LANGGRAPH STATE
# # ================================================================
#
# class MovieSearchState(TypedDict):
#     # ── Input ────────────────────────────────────────────────
#     raw_query:         str
#     user_id:           str
#     session_id:        str
#     user_preferences:  dict   # {liked_ids, liked_titles, disliked_titles,
#                                #  disliked_ids, genre_weights, decade_hint}
#
#     # ── Reformulator ─────────────────────────────────────────
#     reformulations:    list[dict]   # [{query, dimension, rationale}]
#     active_query:      str
#
#     # ── Filter suggester ─────────────────────────────────────
#     suggested_filters: dict         # {genres, decades, min_rating, language}
#
#     # ── Search ───────────────────────────────────────────────
#     search_results:    list[dict]
#     top_score:         float
#     search_mode:       str
#     pivot_spread:      float        # std of pivot similarities (query confidence)
#
#     # ── Unknown movie lookup ─────────────────────────────────
#     unknown_movie_query: bool
#     lookup_results:      list[dict]
#
#     # ── Explanations ─────────────────────────────────────────
#     explanations:      dict         # {str(movieId): explanation_text}
#
#     # ── Group ─────────────────────────────────────────────────
#     all_users_ready:       bool
#     group_similarity:      dict
#     group_summary:         str
#     group_recommendations: list[dict]
#
#     # ── Persona ───────────────────────────────────────────────
#     persona_discussion: str
#
#     # ── Debug / transparency ──────────────────────────────────
#     llm_thinking:      dict    # {node_name: thinking_text} for UI transparency
#
#
# # ================================================================
# # NODE 1 — QUERY REFORMULATOR
# # ================================================================
#
# REFORMULATOR_SYSTEM = """You are a movie search query specialist.
# Suggest exactly 4 improved versions of the user's search query.
#
# RULES:
# - Each reformulation must improve along a DIFFERENT dimension.
# - Explain concisely why each is better than the original.
# - Never repeat the original query verbatim.
# - Preserve the user's core intent.
# - Output valid JSON only. No markdown fences, no preamble.
#
# DIMENSIONS (use 4 different ones):
#   mood       — clarify emotional tone / atmosphere
#   era        — specify time period
#   style      — add directorial / visual style signal
#   theme      — make central theme more vivid and specific
#   comparison — "like [Film X] but [twist]"
#   negative   — explicitly state what user does NOT want
#   character  — focus on character archetype or dynamic
#   pacing     — add pacing / length preference
#
# OUTPUT (JSON only):
# {
#   "reformulations": [
#     {"query": "...", "dimension": "mood",       "rationale": "..."},
#     {"query": "...", "dimension": "era",        "rationale": "..."},
#     {"query": "...", "dimension": "theme",      "rationale": "..."},
#     {"query": "...", "dimension": "comparison", "rationale": "..."}
#   ]
# }"""
#
#
# def reformulate_node(state: MovieSearchState) -> dict:
#     """
#     NODE 1: Query Reformulator.
#     Fires on every user action. Returns 4 dimension-diversified
#     reformulations with rationales inspired by the DataScout paper.
#     Debounce by ~400ms in the API layer.
#     """
#     query = state["raw_query"].strip()
#     if len(query) < 3:
#         return {"reformulations": [], "active_query": query, "llm_thinking": {}}
#
#     prefs    = state.get("user_preferences", {})
#     liked    = prefs.get("liked_titles", [])[:5]
#     disliked = prefs.get("disliked_titles", [])[:3]
#     ctx_lines = []
#     if liked:    ctx_lines.append(f"Recently liked: {', '.join(liked)}")
#     if disliked: ctx_lines.append(f"Recently disliked: {', '.join(disliked)}")
#     context = "\n".join(ctx_lines) or "No preference history yet."
#
#     try:
#         response = llm_structured.invoke([
#             SystemMessage(content=REFORMULATOR_SYSTEM),
#             HumanMessage(content=f'Query: "{query}"\n\nUser context:\n{context}'),
#         ])
#         raw_content   = response.content
#         thinking_text = _extract_text(raw_content, include_thinking=True)
#         reformulations = _parse_json(raw_content).get("reformulations", [])
#     except Exception as e:
#         print(f"Reformulator error: {e}")
#         reformulations = []
#         thinking_text  = ""
#
#     return {
#         "reformulations": reformulations,
#         "active_query":   query,
#         "llm_thinking":   state.get("llm_thinking", {}) | {"reformulator": thinking_text},
#     }
#
#
# # ================================================================
# # NODE 2 — FILTER SUGGESTER
# # ================================================================
# # Uses live genre + decade lists fetched from Neo4j at startup.
# # Future improvement: make these lists user-specific based on
# # their like/dislike history and current search session state.
#
# def _build_filter_system_prompt() -> str:
#     genres_list  = ", ".join(_FILTER_OPTIONS["genres"]) or "Action, Drama, Comedy"
#     decades_list = ", ".join(_FILTER_OPTIONS["decades"]) or "1980s, 1990s, 2000s, 2010s"
#     return f"""You are a movie filter specialist.
# Infer the best search filters from the user's query.
#
# AVAILABLE FILTERS (sourced from the actual database):
#   genres     — pick from: {genres_list}
#   decades    — pick from: {decades_list}
#   min_rating — float threshold (e.g. 3.5). Only suggest if quality is implied.
#   language   — string (e.g. "English", "Japanese") or null
#
# RULES:
# - Only suggest filters the query STRONGLY implies.
# - Under-filter rather than over-filter.
# - Genres must match the list above exactly (case-sensitive).
# - Decades must match the list above exactly.
# - Output valid JSON only. No markdown fences.
#
# OUTPUT:
# {{
#   "filters": {{
#     "genres": ["Crime", "Drama"],
#     "decades": ["1970s"],
#     "min_rating": null,
#     "language": null
#   }},
#   "rationale": "brief explanation of why these filters were chosen"
# }}"""
#
#
# def filter_suggester_node(state: MovieSearchState) -> dict:
#     """
#     NODE 2: Filter Suggester.
#     Dynamically builds the prompt with live genre/decade options from Neo4j.
#     Returns filter suggestions the user can toggle in the UI.
#     """
#     query = state.get("active_query", state.get("raw_query", ""))
#     if not query:
#         return {"suggested_filters": {}}
#
#     # Also hint from user's genre_weights if they've used sliders
#     prefs = state.get("user_preferences", {})
#     hint  = ""
#     if prefs.get("genre_weights"):
#         hint = f"\nUser's current genre sliders: {prefs['genre_weights']}"
#     if prefs.get("decade_hint"):
#         hint += f"\nUser mentioned era preference: {prefs['decade_hint']}"
#
#     try:
#         response = llm_structured.invoke([
#             SystemMessage(content=_build_filter_system_prompt()),
#             HumanMessage(content=f'Query: "{query}"{hint}'),
#         ])
#         filters = {k: v for k, v in
#                    _parse_json(response.content).get("filters", {}).items()
#                    if v is not None}
#         # Validate genres against known list to prevent hallucinated genre names
#         if filters.get("genres"):
#             valid = set(_FILTER_OPTIONS["genres"])
#             filters["genres"] = [g for g in filters["genres"] if g in valid]
#             if not filters["genres"]:
#                 del filters["genres"]
#         # Validate decades
#         if filters.get("decades"):
#             valid_d = set(_FILTER_OPTIONS["decades"])
#             filters["decades"] = [d for d in filters["decades"] if d in valid_d]
#             if not filters["decades"]:
#                 del filters["decades"]
#     except Exception as e:
#         print(f"Filter suggester error: {e}")
#         filters = {}
#
#     return {"suggested_filters": filters}
#
#
# # ================================================================
# # NODE 3 — SEARCH ENGINE (no LLM)
# # ================================================================
# # Intelligently selects which search_function to use based on state:
# #   - genre_weights in prefs → steer_by_genres
# #   - decade_hint in prefs   → era_slider
# #   - liked_ids + no filters → hybrid with preference bias
# #   - filters present        → filtered_semantic
# #   - default                → hybrid (semantic + graph, 5 pivots)
#
# SCORE_THRESHOLD = 0.72
#
#
# def search_node(state: MovieSearchState) -> dict:
#     """
#     NODE 3: Search Engine.
#     Selects the right search function based on the full session state.
#     No LLM involved — pure embedding + Neo4j ANN.
#     """
#     if not SEARCH_AVAILABLE:
#         return {"search_results": [], "top_score": 0.0,
#                 "search_mode": "unavailable", "unknown_movie_query": False,
#                 "pivot_spread": 0.0}
#
#     query   = state.get("active_query", state.get("raw_query", ""))
#     filters = state.get("suggested_filters", {})
#     prefs   = state.get("user_preferences", {})
#
#     genre_weights = prefs.get("genre_weights", {})
#     decade_hint   = prefs.get("decade_hint")
#     liked_ids     = prefs.get("liked_ids", [])
#
#     pivot_spread = 0.0
#
#     # Priority 1: explicit genre sliders → steer_by_genres
#     if genre_weights:
#         results = steer_by_genres(query, genre_weights, k=10)
#         mode    = f"genre_steered({list(genre_weights.keys())})"
#
#     # Priority 2: era preference → era_slider
#     elif decade_hint and not filters.get("decades"):
#         results = era_slider(query, decade_hint, alpha=0.8, k=10)
#         mode    = f"era_slider({decade_hint})"
#
#     # Priority 3: hard filters from suggester
#     elif filters.get("genres") or filters.get("decades"):
#         results = filtered_search(
#             query,
#             genres=filters.get("genres"),
#             decade=(filters.get("decades") or [None])[0],
#             min_rating=filters.get("min_rating"),
#             k=10,
#         )
#         mode = "filtered_semantic"
#
#     # Priority 4: has liked movies → hybrid with preference context
#     elif liked_ids:
#         r    = graph_search_multi_pivot(query, n_pivots=5, k=10)
#         results      = r.get("results", [])
#         pivot_spread = r.get("pivot_vector_std", 0.0)
#         # Blend with semantic to also catch thematic matches
#         sem = semantic_search(query, k=10)
#         # Merge: prefer items in both lists, then graph, then semantic
#         seen = {r["movieId"] for r in results}
#         for s in sem:
#             if s["movieId"] not in seen and len(results) < 10:
#                 results.append(s); seen.add(s["movieId"])
#         mode = "hybrid_preference"
#
#     # Default: hybrid
#     else:
#         results = hybrid_search(query, lam=0.6, n_pivots=5, k=10)
#         mode    = "hybrid"
#
#     top_score = max(
#         (r.get("score", r.get("sem_score", 0)) for r in results),
#         default=0.0
#     )
#
#     return {
#         "search_results":      results,
#         "top_score":           top_score,
#         "search_mode":         mode,
#         "pivot_spread":        pivot_spread,
#         "unknown_movie_query": top_score < SCORE_THRESHOLD,
#     }
#
#
# # ================================================================
# # NODE 4 — UNKNOWN MOVIE LOOKUP
# # ================================================================
#
# LOOKUP_SYSTEM = """You are a movie research assistant.
# A user searched a movie database and got poor results.
#
# Determine:
# A) They typed a SPECIFIC MOVIE TITLE not in the database
#    → Use web_search to find its plot/overview
#    → Return: {"type":"specific_movie","title":"...","year":2024,"overview":"..."}
#
# B) Their query is just vague
#    → Do NOT search
#    → Return: {"type":"vague","suggestion":"more specific reformulation"}
#
# C) Movie searched but not found
#    → Return: {"type":"not_found","message":"brief explanation"}
#
# Output valid JSON only. No markdown fences."""
#
#
# def unknown_movie_lookup_node(state: MovieSearchState) -> dict:
#     """
#     NODE 4: Unknown Movie Lookup.
#     Fires when search score < SCORE_THRESHOLD.
#     Searches the web, embeds the found overview with Jina, re-queries Neo4j.
#     """
#     if not state.get("unknown_movie_query", False):
#         return {}
#
#     query   = state.get("active_query", state.get("raw_query", ""))
#     tools   = [web_search]
#     model   = make_llm(temperature=0.2).bind_tools(tools)
#     by_name = {t.name: t for t in tools}
#
#     messages = [
#         SystemMessage(content=LOOKUP_SYSTEM),
#         HumanMessage(content=f'User searched for: "{query}"'),
#     ]
#
#     try:
#         for _ in range(5):
#             ai = model.invoke(messages)
#             messages.append(ai)
#             calls = getattr(ai, "tool_calls", None) or []
#             if not calls:
#                 break
#             for call in calls:
#                 fn = by_name.get(call.get("name", ""))
#                 if fn is None:
#                     continue
#                 tool_result = fn.invoke(call.get("args", {}))
#                 # tool_result may be a list of dicts — serialise safely
#                 if not isinstance(tool_result, str):
#                     tool_result = json.dumps(tool_result)
#                 messages.append(ToolMessage(
#                     content=tool_result,
#                     tool_call_id=call.get("id", ""),
#                 ))
#
#         # Find last AIMessage with non-empty text content
#         last_text = next(
#             (_extract_text(m.content)
#              for m in reversed(messages)
#              if isinstance(m, AIMessage) and _extract_text(m.content).strip()),
#             "{}"
#         )
#         parsed = json.loads(_strip_fences(last_text))
#     except Exception as e:
#         print(f"Lookup error: {e}")
#         return {"lookup_results": [], "unknown_movie_query": False}
#
#     if parsed.get("type") == "specific_movie" and parsed.get("overview") and SEARCH_AVAILABLE:
#         try:
#             emb     = encode_query(parsed["overview"])
#             results = neo4j_run("""
#                 CALL db.index.vector.queryNodes('movie_semantic_idx', 10, $vec)
#                 YIELD node AS m, score
#                 RETURN m.movieId AS movieId, m.title AS title,
#                        m.year AS year, score
#                 ORDER BY score DESC LIMIT 7
#             """, vec=emb.tolist())
#             print(f"  Lookup found: '{parsed.get('title')}' ({parsed.get('year')}) "
#                   f"→ embedded and re-searched")
#             return {
#                 "lookup_results":      results,
#                 "unknown_movie_query": False,
#                 "search_results":      results,
#             }
#         except Exception as e:
#             print(f"Lookup re-search error: {e}")
#
#     return {"lookup_results": [], "unknown_movie_query": False}
#
#
# # ================================================================
# # NODE 5 — EXPLANATION GENERATOR
# # ================================================================
# # Strictly grounded — only uses facts from the KG context block.
# # Correct relationship: HAS_ACTOR (from the original loader).
#
# EXPLANATION_SYSTEM = """You are a movie recommendation explainer.
# Write a short, specific explanation for why ONE movie is recommended to this user.
#
# RULES:
# - 2-3 sentences. No filler, no praise, no "you'll love this".
# - Use ONLY the facts in the provided KG context. Do not invent anything.
# - Every explanation MUST mention at least one specific plot detail from the overview.
# - If a KG connection exists (⭐ shared director/actor/genre with liked movies), lead with it.
# - Write in second person: "Recommended because..."
# - Make the explanation SPECIFIC to THIS movie. Never write the same sentence twice across movies.
#
# BAD: "Recommended because it aligns with your search."
# GOOD: "Recommended because its premise — a detective investigating her own husband — directly
#        matches your search for unreliable detectives, and it shares cinematographer Roger Deakins
#        with Prisoners which you recently liked."
#
# Return a single plain string. No JSON, no markdown."""
#
#
# def _fetch_kg_context(movie_id: int, liked_ids: list[int]) -> str:
#     """Fetch KG context for one movie."""
#     if not SEARCH_AVAILABLE:
#         return "Search unavailable."
#     try:
#         rating_clause = f"m.{RATING_PROP} AS rating" if RATING_PROP else "null AS rating"
#         meta = neo4j_run(f"""
#             MATCH (m:Movie {{movieId: $mid}})
#             OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
#             OPTIONAL MATCH (m)-[:DIRECTED_BY]->(d:Director)
#             OPTIONAL MATCH (m)-[:HAS_ACTOR]->(a:Actor)
#             RETURN collect(DISTINCT g.name)[..5]  AS genres,
#                    collect(DISTINCT d.name)[..2]  AS directors,
#                    collect(DISTINCT a.name)[..5]  AS actors,
#                    m.overview AS overview,
#                    {rating_clause}
#             LIMIT 1
#         """, mid=movie_id)
#
#         shared = neo4j_run("""
#             MATCH (liked:Movie) WHERE liked.movieId IN $liked_ids
#             MATCH (m:Movie {movieId: $mid})
#             OPTIONAL MATCH (m)-[:DIRECTED_BY]->(d:Director)<-[:DIRECTED_BY]-(liked)
#             OPTIONAL MATCH (m)-[:HAS_ACTOR]->(a:Actor)<-[:HAS_ACTOR]-(liked)
#             OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)<-[:HAS_GENRE]-(liked)
#             RETURN
#               collect(DISTINCT d.name)[..2]      AS shared_directors,
#               collect(DISTINCT a.name)[..3]      AS shared_actors,
#               collect(DISTINCT g.name)[..3]      AS shared_genres,
#               collect(DISTINCT liked.title)[..3] AS via_movies
#         """, mid=movie_id, liked_ids=liked_ids) if liked_ids else [{}]
#
#         m, s = (meta[0] if meta else {}), (shared[0] if shared else {})
#         parts = []
#         if m.get("genres"):           parts.append(f"Genres: {', '.join(m['genres'])}")
#         if m.get("directors"):        parts.append(f"Director: {', '.join(m['directors'])}")
#         if m.get("rating"):           parts.append(f"Avg rating: {m['rating']:.2f}")
#         if s.get("shared_directors"): parts.append(
#             f"⭐ Shares director with liked: {', '.join(s['shared_directors'])}"
#             f" (via {', '.join(s.get('via_movies',[]))})")
#         if s.get("shared_actors"):    parts.append(
#             f"⭐ Shares cast with liked: {', '.join(s.get('via_movies',[]))}")
#         if s.get("shared_genres"):    parts.append(
#             f"Shared genre with liked: {', '.join(s['shared_genres'])}")
#         # Always include full overview — it's the main differentiator between movies
#         if m.get("overview"):         parts.append(f"Overview: {m['overview'][:200]}")
#         return "\n".join(parts) or "No KG context available."
#     except Exception as e:
#         return f"KG error: {e}"
#
#
# def _web_search_movie_context(title: str, year) -> str:
#     """Run a quick web search for a movie to enrich explanation context."""
#     try:
#         query = f"{title} {year} film plot review"
#         results = web_search.invoke({"query": query})
#         if not results:
#             return ""
#         # Take the first result snippet only
#         first = results[0] if isinstance(results, list) else results
#         snippet = first.get("content", "") if isinstance(first, dict) else str(first)
#         return f"Web context: {snippet[:200]}"
#     except Exception:
#         return ""
#
#
# def explanation_generator_node(state: MovieSearchState,
#                                 use_web_search: bool = True) -> dict:
#     """
#     NODE 5: Explanation Generator.
#     Calls the LLM once per movie (not batched) so each gets specific attention.
#     Optionally enriches context with a web search snippet per movie.
#     Uses gemini-2.5-pro for quality.
#     """
#     results = state.get("search_results", [])
#     if not results:
#         return {"explanations": {}}
#
#     query        = state.get("active_query", state.get("raw_query", ""))
#     prefs        = state.get("user_preferences", {})
#     liked_ids    = prefs.get("liked_ids", [])
#     liked_titles = prefs.get("liked_titles", [])
#     mode         = state.get("search_mode", "hybrid")
#
#     pref_line = f"User liked: {', '.join(liked_titles[:5])}" if liked_titles \
#                 else "No preference history."
#
#     explanations = {}
#     for r in results[:5]:   # cap at 5 to keep cost reasonable with pro model
#         mid   = r.get("movieId")
#         title = r.get("title", "?")
#         year  = r.get("year", "?")
#         score = r.get("score", r.get("sem_score", 0))
#
#         kg = _fetch_kg_context(mid, liked_ids) if mid else "No KG context."
#
#         # Optional web search enrichment
#         web_ctx = ""
#         if use_web_search and title and title != "?":
#             web_ctx = _web_search_movie_context(title, year)
#
#         prompt = (
#             f'User query: "{query}"\n'
#             f"{pref_line}\n\n"
#             f"Movie: {title} ({year}) — score {score:.3f} via {mode}\n"
#             f"KG context:\n{kg}\n"
#             + (f"\n{web_ctx}" if web_ctx else "")
#             + "\n\nWrite the explanation for this specific movie."
#         )
#
#         try:
#             response = llm_explain.invoke([
#                 SystemMessage(content=EXPLANATION_SYSTEM),
#                 HumanMessage(content=prompt),
#             ])
#             raw = _extract_text(response.content).strip()
#             if not raw:
#                 raise ValueError("empty response")
#             explanations[str(mid)] = raw
#         except Exception as e:
#             print(f"Explanation error for {title}: {e}")
#             explanations[str(mid)] = (
#                 f"Recommended because it matches your search for '{query}' "
#                 f"(score: {score:.2f})."
#             )
#
#     return {"explanations": explanations}
#
#
# # ================================================================
# # NODE 6 — GROUP SUMMARY
# # ================================================================
#
# GROUP_SUMMARY_SYSTEM = """You are a group movie night facilitator.
# You have pairwise similarity data between users and connector movie suggestions.
#
# Write:
# 1. A group summary (3-4 sentences): state the overlap directly, name the main tension,
#    identify who is the outlier if any. Be direct and specific — no exclamation marks,
#    no filler phrases like "what an interesting pair" or "fantastic".
# 2. A short rationale per connector movie: what specifically does each person get from it,
#    based on their actual taste profile.
#
# Output valid JSON only. No markdown fences.
# {
#   "summary": "...",
#   "connector_rationale": [
#     {"title": "...", "rationale": "..."}
#   ]
# }"""
#
#
# def group_summary_node(state: MovieSearchState) -> dict:
#     """NODE 6: Group Summary. Only fires when all_users_ready=True."""
#     if not state.get("all_users_ready", False):
#         return {}
#
#     group_sim = state.get("group_similarity", {})
#     results   = state.get("search_results", [])
#     if not group_sim:
#         return {"group_summary": "Not enough data for group summary."}
#
#     g_scal = group_sim.get("graph_similarity_scaled", {})
#     s_scal = group_sim.get("semantic_similarity_scaled", {})
#     counts = group_sim.get("liked_counts", {})
#     users  = list(g_scal.keys())
#
#     sim_lines = [
#         f"  {u1} ↔ {u2}: structural {g_scal.get(u1,{}).get(u2,50):.0f}%, "
#         f"thematic {s_scal.get(u1,{}).get(u2,50):.0f}%"
#         for i, u1 in enumerate(users) for u2 in users[i+1:]
#     ]
#     connector_titles = [r.get("title", "") for r in results[:5]]
#
#     prompt = (
#         f"Group similarity:\n{chr(10).join(sim_lines)}\n\n"
#         f"Movies liked per user: {json.dumps(counts)}\n\n"
#         f"Connector movies: {', '.join(connector_titles)}\n\n"
#         "Generate summary and per-movie rationale."
#     )
#
#     try:
#         response = llm_creative.invoke([
#             SystemMessage(content=GROUP_SUMMARY_SYSTEM),
#             HumanMessage(content=prompt),
#         ])
#         parsed   = _parse_json(response.content)
#         summary  = parsed.get("summary", "")
#         rational = parsed.get("connector_rationale", [])
#     except Exception as e:
#         print(f"Group summary error: {e}")
#         summary, rational = "Unable to generate group summary.", []
#
#     return {
#         "group_summary": summary,
#         "group_recommendations": [
#             {**r, "rationale": next(
#                 (x["rationale"] for x in rational
#                  if x.get("title","") in r.get("title","")), ""
#             )}
#             for r in results[:5]
#         ],
#     }
#
#
# # ================================================================
# # NODE 7 — PERSONA AGENT
# # ================================================================
#
# PERSONA_SYSTEM = """You are simulating friends discussing whether to watch a movie.
# Each friend has a distinct taste profile shown below.
# Use ONLY the provided data — do not invent opinions.
# Keep each voice brief (1-2 sentences). End with a facilitator consensus.
#
# Output valid JSON only. No markdown fences.
# {
#   "discussion": [
#     {"speaker": "Alice", "message": "..."},
#     {"speaker": "Bob",   "message": "..."}
#   ],
#   "consensus": "...",
#   "recommendation": "watch" | "skip" | "split"
# }"""
#
#
# def persona_node(state: MovieSearchState) -> dict:
#     """NODE 7: Persona Agent. Simulates user debate about the top result."""
#     results   = state.get("search_results", [])
#     group_sim = state.get("group_similarity", {})
#     if not results or not group_sim.get("taste_vectors_graph"):
#         return {"persona_discussion": "Need group data and results to simulate."}
#
#     top    = results[0]
#     counts = group_sim.get("liked_counts", {})
#     expl   = state.get("explanations", {}).get(
#         str(top.get("movieId")), "No explanation available."
#     )
#     profiles = "\n".join(f"  {u}: liked {n} movies" for u, n in counts.items())
#
#     prompt = (
#         f'Movie being discussed: "{top.get("title","?")} ({top.get("year","?")})\n'
#         f"Why it was recommended: {expl}\n\n"
#         f"User profiles:\n{profiles}\n\n"
#         "Simulate the group discussion."
#     )
#
#     try:
#         response = llm_creative.invoke([
#             SystemMessage(content=PERSONA_SYSTEM),
#             HumanMessage(content=prompt),
#         ])
#         parsed = _parse_json(response.content)
#         lines  = [f"  {d['speaker']}: {d['message']}"
#                   for d in parsed.get("discussion", [])]
#         lines.append(f"\n  Consensus: {parsed.get('consensus','')}")
#         lines.append(f"  Verdict: {parsed.get('recommendation','?').upper()}")
#         text = "\n".join(lines)
#     except Exception as e:
#         print(f"Persona error: {e}")
#         text = "Persona simulation unavailable."
#
#     return {"persona_discussion": text}
#
#
# # ================================================================
# # CYPHER WRITER — standalone
# # ================================================================
#
# def _build_cypher_system_prompt() -> str:
#     """Build Cypher prompt with live genre list and correct relationship directions."""
#     genre_list = ", ".join(_FILTER_OPTIONS["genres"]) or "Drama, Comedy, Action, Thriller"
#     # Only list properties that actually exist on Movie nodes
#     movie_props = "movieId, title, year, overview, popularity, poster_path"
#     if RATING_PROP:
#         movie_props += f", {RATING_PROP}"
#     rating_note = (
#         f"RATING: use m.{RATING_PROP} for rating filters."
#         if RATING_PROP else
#         "RATING: NO rating property exists. Do NOT use avg_rating, rating, or any rating field."
#     )
#     return f"""You are a Neo4j Cypher expert for a movie knowledge graph.
#
# SCHEMA — arrow directions are EXACT, do not reverse them:
#   (:Movie {{{movie_props}}})
#   (:Genre {{name}})  (:Director {{directorId, name}})  (:Actor {{actorId, name}})
#   (:Keyword {{name}})  (:Language {{name}})  (:Country {{code}})  (:Decade {{label}})
#
#   (Movie)-[:HAS_GENRE]    ->(Genre)
#   (Movie)-[:DIRECTED_BY]  ->(Director)
#   (Movie)-[:HAS_ACTOR]    ->(Actor)
#   (Movie)-[:HAS_KEYWORD]  ->(Keyword)
#   (Movie)-[:IN_LANGUAGE]  ->(Language)
#   (Movie)-[:FROM_COUNTRY] ->(Country)
#   (Movie)-[:IN_DECADE]    ->(Decade)
#
# GENRE NAMES (case-sensitive, use exactly):
#   {genre_list}
#
# {rating_note}
#
# RULES:
# - MATCH/RETURN only. NEVER CREATE/MERGE/DELETE/SET.
# - Always add LIMIT (max 20).
# - Never use properties not listed in the schema above.
# - Return raw Cypher ONLY — no explanation, no markdown, no backticks."""
#
#
# def cypher_writer(query: str) -> list[dict]:
#     """
#     Standalone: translate natural language → Cypher → execute → return results.
#     Strips all markdown fences from the LLM response before parsing.
#     """
#     if not SEARCH_AVAILABLE:
#         return []
#     try:
#         response = llm_explain.invoke([   # pro model — generates more accurate Cypher
#             SystemMessage(content=_build_cypher_system_prompt()),
#             HumanMessage(content=f"Write Cypher for: {query}"),
#         ])
#         # Strip ALL possible fence formats Gemini might use
#         cypher = _extract_text(response.content)
#         cypher = re.sub(r"```(?:cypher|sql|)[\s\S]*?```", "", cypher).strip()
#         cypher = re.sub(r"```", "", cypher).strip()
#
#         if not cypher:
#             print("Cypher writer returned empty query.")
#             return []
#
#         if any(f in cypher.upper() for f in ["DELETE","DETACH","SET ","CREATE ","MERGE "]):
#             print(f"Cypher writer blocked destructive query: {cypher[:60]}")
#             return []
#
#         print(f"  Executing Cypher:\n    {cypher[:200]}")
#         results = neo4j_run(cypher)
#         rating_prop = RATING_PROP  # None if property doesn't exist
#         return [
#             {"title":   r.get("title",   r.get("m.title",   "?")),
#              "year":    r.get("year",    r.get("m.year",    "?")),
#              "rating":  (r.get(rating_prop) or r.get(f"m.{rating_prop}")) if rating_prop else None,
#              "movieId": r.get("movieId", r.get("m.movieId"))}
#             for r in results
#         ]
#     except Exception as e:
#         print(f"Cypher writer error: {e}")
#         return []
#
#
# # ================================================================
# # LANGGRAPH WORKFLOW
# # ================================================================
#
# def _route_search(state: MovieSearchState) -> Literal["lookup", "explain"]:
#     return "lookup" if state.get("unknown_movie_query", False) else "explain"
#
#
# def _route_explain(state: MovieSearchState) -> Literal["group", "done"]:
#     return "group" if state.get("all_users_ready", False) else "done"
#
#
# def build_graph() -> StateGraph:
#     g = StateGraph(MovieSearchState)
#     g.add_node("reformulate",     reformulate_node)
#     g.add_node("suggest_filters", filter_suggester_node)
#     g.add_node("search",          search_node)
#     g.add_node("lookup_unknown",  unknown_movie_lookup_node)
#     g.add_node("explain",         explanation_generator_node)
#     g.add_node("group_summary",   group_summary_node)
#     g.add_node("persona",         persona_node)
#
#     g.set_entry_point("reformulate")
#     g.add_edge("reformulate",     "suggest_filters")
#     g.add_edge("suggest_filters", "search")
#     g.add_conditional_edges("search",  _route_search,
#                             {"lookup": "lookup_unknown", "explain": "explain"})
#     g.add_edge("lookup_unknown", "explain")
#     g.add_conditional_edges("explain", _route_explain,
#                             {"group": "group_summary", "done": END})
#     g.add_edge("group_summary", END)
#     return g
#
#
# search_graph = build_graph().compile()
#
#
# # ================================================================
# # PUBLIC API
# # ================================================================
#
# def run_search_pipeline(
#     raw_query: str,
#     user_id: str = "user_1",
#     session_id: str = "session_1",
#     user_preferences: dict = None,
#     all_users_ready: bool = False,
#     group_similarity: dict = None,
# ) -> dict:
#     """
#     Main entry point for FastAPI / Next.js backend.
#
#     user_preferences keys:
#       liked_ids      list[int]  — movieIds the user liked
#       liked_titles   list[str]  — for display / prompt context
#       disliked_ids   list[int]
#       disliked_titles list[str]
#       genre_weights  dict       — e.g. {"Action": 0.7, "Comedy": 0.3}
#       decade_hint    str        — e.g. "1990s" from era slider
#     """
#     initial = MovieSearchState(
#         raw_query=raw_query,
#         user_id=user_id,
#         session_id=session_id,
#         user_preferences=user_preferences or {},
#         reformulations=[],
#         active_query=raw_query,
#         suggested_filters={},
#         search_results=[],
#         top_score=0.0,
#         search_mode="",
#         pivot_spread=0.0,
#         unknown_movie_query=False,
#         lookup_results=[],
#         explanations={},
#         all_users_ready=all_users_ready,
#         group_similarity=group_similarity or {},
#         group_summary="",
#         group_recommendations=[],
#         persona_discussion="",
#         llm_thinking={},
#     )
#     return dict(search_graph.invoke(initial))
#
#
# # ================================================================
# # TEST HARNESS — realistic multi-step session simulation
# # ================================================================
#
# if __name__ == "__main__":
#     import textwrap, time
#
#     def header(t): print(f"\n{'='*70}\n  {t}\n{'='*70}")
#     def subheader(t): print(f"\n  ── {t} ──")
#     def show_results(state, n=5):
#         for r in state.get("search_results", [])[:n]:
#             mid  = str(r.get("movieId",""))
#             expl = state.get("explanations",{}).get(mid,"(no explanation)")
#             score = r.get("score", r.get("sem_score",0))
#             print(f"    {r.get('title','?'):<48} {str(r.get('year','')):<6}  {score:.3f}")
#             print(f"      → {textwrap.fill(expl, 90, subsequent_indent='        ')}")
#
#     # ── Test 1: Cold start — vibe query, no history ───────────────────────
#     header("TEST 1 — COLD START: VIBE QUERY")
#     print("  Simulating: Alice opens the app for the first time and types a mood.")
#     r1 = run_search_pipeline(
#         raw_query="something dark and psychological that keeps me guessing",
#         user_id="alice", session_id="night_1",
#     )
#     subheader("Reformulations (all 4, full rationale)")
#     for ref in r1.get("reformulations", []):
#         print(f"    [{ref.get('dimension','?')}] {ref.get('query','')}")
#         print(f"         {ref.get('rationale','')}")
#     subheader(f"Filters suggested: {r1.get('suggested_filters',{})}")
#     subheader(f"Search mode: {r1.get('search_mode','?')} | "
#               f"Top score: {r1.get('top_score',0):.3f} | "
#               f"Pivot spread: {r1.get('pivot_spread',0):.4f}")
#     subheader("Results + Explanations")
#     show_results(r1)
#     if r1.get("llm_thinking", {}).get("reformulator"):
#         subheader("LLM thinking (reformulator)")
#         print(textwrap.fill(
#             r1["llm_thinking"]["reformulator"][:500], 90,
#             initial_indent="    ", subsequent_indent="    "
#         ))
#
#     # ── Test 2: User liked some results, now queries again ────────────────
#     header("TEST 2 — SECOND QUERY WITH PREFERENCE HISTORY")
#     print("  Simulating: Alice liked Memento and Prisoners from test 1, now refines.")
#     if SEARCH_AVAILABLE:
#         liked_rows = neo4j_run("""
#             MATCH (m:Movie)
#             WHERE m.title IS NOT NULL
#               AND toLower(toString(m.title)) IN ['memento', 'prisoners', 'se7en']
#               AND m.embedding_graph IS NOT NULL
#             RETURN m.movieId AS movieId, m.title AS title
#         """)
#     else:
#         liked_rows = []
#
#     r2 = run_search_pipeline(
#         raw_query="a thriller where the detective is unreliable or obsessed",
#         user_id="alice", session_id="night_1",
#         user_preferences={
#             "liked_ids":    [r["movieId"] for r in liked_rows],
#             "liked_titles": [r["title"]   for r in liked_rows],
#         },
#     )
#     subheader(f"Liked context: {[r['title'] for r in liked_rows]}")
#     subheader(f"Mode: {r2.get('search_mode','?')} | Score: {r2.get('top_score',0):.3f}")
#     subheader("Results + KG-grounded Explanations")
#     show_results(r2)
#
#     # ── Test 3: Genre slider active ───────────────────────────────────────
#     header("TEST 3 — GENRE SLIDER: 60% CRIME, 40% DRAMA")
#     print("  Simulating: Alice drags the genre slider toward Crime.")
#     r3 = run_search_pipeline(
#         raw_query="a gripping story about justice and moral failure",
#         user_id="alice", session_id="night_1",
#         user_preferences={
#             "liked_ids":    [r["movieId"] for r in liked_rows],
#             "liked_titles": [r["title"]   for r in liked_rows],
#             "genre_weights": {"Crime": 0.6, "Drama": 0.4},
#         },
#     )
#     subheader(f"Mode: {r3.get('search_mode','?')}")
#     show_results(r3, n=5)
#
#     # ── Test 4: Era slider active ──────────────────────────────────────────
#     header("TEST 4 — ERA SLIDER: 1970s")
#     print("  Simulating: Alice sets the era slider to 1970s.")
#     r4 = run_search_pipeline(
#         raw_query="a dark crime drama with moral ambiguity",
#         user_id="alice", session_id="night_1",
#         user_preferences={
#             "decade_hint": "1970s",
#         },
#     )
#     subheader(f"Mode: {r4.get('search_mode','?')}")
#     show_results(r4, n=5)
#
#     # ── Test 5: Unknown movie — recent film not in DB ─────────────────────
#     header("TEST 5 — UNKNOWN MOVIE LOOKUP")
#     print("  Simulating: Alice types a recent film that might not be in the database.")
#     r5 = run_search_pipeline(
#         raw_query="Conclave 2024 movie",
#         user_id="alice", session_id="night_1",
#     )
#     subheader(f"Mode: {r5.get('search_mode','?')} | Score: {r5.get('top_score',0):.3f}")
#     subheader("Results after web lookup + re-embedding:")
#     for r in r5.get("search_results", [])[:4]:
#         print(f"    {r.get('title','?'):<48} {str(r.get('year',''))}")
#
#     # ── Test 6: Cypher writer — explicit fact queries ──────────────────────
#     header("TEST 6 — CYPHER WRITER")
#     print("  Simulating: user asks explicit factual questions about the graph.")
#     for q in [
#         "Movies directed by David Fincher ordered by rating",
#         "Top 5 Crime movies from the 1990s with rating above 3.8",
#         "Which actors appear in both Sci-Fi and Horror films",
#     ]:
#         subheader(f"Query: '{q}'")
#         rows = cypher_writer(q)
#         for row in rows[:5]:
#             rating = f" ★{row['rating']:.2f}" if row.get("rating") else ""
#             print(f"    {row.get('title','?'):<48} {str(row.get('year',''))}{rating}")
#
#     # ── Test 7: Group session — different taste profiles ──────────────────
#     header("TEST 7 — GROUP SESSION: ALICE (thriller) + BOB (family/comedy)")
#     print("  Simulating: movie night group, both users ready.")
#     result7, pref_map = {}, {}
#     if SEARCH_AVAILABLE:
#         alice_ids = [r["movieId"] for r in neo4j_run("""
#             MATCH (m:Movie)
#             WHERE m.title IS NOT NULL AND m.embedding_graph IS NOT NULL
#               AND toLower(toString(m.title)) IN
#                 ['zodiac', 'se7en', 'no country for old men', 'fargo',
#                  'gone girl', 'silence of the lambs, the']
#             RETURN m.movieId AS movieId, m.title AS title
#         """)]
#         bob_ids = [r["movieId"] for r in neo4j_run("""
#             MATCH (m:Movie)
#             WHERE m.title IS NOT NULL AND m.embedding_graph IS NOT NULL
#               AND toLower(toString(m.title)) IN
#                 ['toy story', 'finding nemo', 'shrek', 'the incredibles',
#                  'up', 'inside out']
#             RETURN m.movieId AS movieId, m.title AS title
#         """)]
#         if alice_ids and bob_ids:
#             pref_map = group_preference_map({
#                 "Alice (thriller)":  alice_ids,
#                 "Bob (family/comedy)": bob_ids,
#             })
#             connectors = connector_movie(
#                 list(set(alice_ids + bob_ids)), embedding_space="graph"
#             )
#             result7 = run_search_pipeline(
#                 raw_query="a movie everyone can enjoy tonight",
#                 user_id="group", session_id="night_1",
#                 all_users_ready=True,
#                 group_similarity=pref_map,
#                 user_preferences={"liked_ids": alice_ids + bob_ids},
#             )
#             subheader("Group similarity")
#             g_scal = pref_map.get("graph_similarity_scaled", {})
#             s_scal = pref_map.get("semantic_similarity_scaled", {})
#             users  = list(g_scal.keys())
#             for i, u1 in enumerate(users):
#                 for u2 in users[i+1:]:
#                     print(f"    {u1} ↔ {u2}:")
#                     print(f"      Graph (structural):  {g_scal.get(u1,{}).get(u2,0):.0f}%")
#                     print(f"      Semantic (thematic): {s_scal.get(u1,{}).get(u2,0):.0f}%")
#
#             subheader("Group summary (full)")
#             print(f"    {result7.get('group_summary','?')}")
#
#             subheader("Connector movie recommendations")
#             for r in result7.get("group_recommendations", [])[:3]:
#                 print(f"\n    {r.get('title','?')} ({r.get('year','?')})")
#                 rat = r.get("rationale","")
#                 if rat:
#                     print(textwrap.fill(rat, 80,
#                           initial_indent="      → ",
#                           subsequent_indent="        "))
#
#     # ── Test 8: Persona discussion about top result ────────────────────────
#     header("TEST 8 — PERSONA DISCUSSION (full output)")
#     if result7.get("search_results") and pref_map:
#         p = persona_node({**result7, "group_similarity": pref_map})
#         print(p.get("persona_discussion", "No discussion generated."))
#
#     # ── Test 9: Decade feel for specific movies ────────────────────────────
#     header("TEST 9 — WHAT DECADE DOES THIS FEEL LIKE?")
#     print("  Useful for displaying a 'feels like the Xs' tag next to each result.")
#     if SEARCH_AVAILABLE:
#         for title_q in ["Arrival", "Blade Runner 2049", "Toy Story", "The Matrix"]:
#             what_decade_does_this_feel_like(title_q)
#
#     print("\n✅ All realistic session tests complete")