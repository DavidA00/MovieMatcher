const API = '/api';

async function post<T>(url: string, body: any): Promise<T> {
  const res = await fetch(`${API}${url}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`API ${res.status}: ${await res.text()}`);
  return res.json();
}

async function get<T>(url: string): Promise<T> {
  const res = await fetch(`${API}${url}`);
  if (!res.ok) throw new Error(`API ${res.status}`);
  return res.json();
}

// ── Session ──────────────────────────────────────────────────

export const join = (user_name: string) =>
  post<{ session_id: string; user_name: string }>('/join', { user_name });

export const getSession = (sid: string) =>
  get<any>(`/session/${sid}`);

// ── Filters ──────────────────────────────────────────────────

export const getFilters = () =>
  get<{ genres: string[]; decades: string[] }>('/filters');

// ── Fast search (no LLM) ────────────────────────────────────

export interface SearchResult {
  movieId: number;
  title: string;
  year?: number;
  score?: number;
  sem_score?: number;
  poster_path?: string;
  poster_url?: string | null;
  overview?: string;
  genres?: string[];
  directors?: string[];
  popularity?: number;
}

export interface SearchResponse {
  search_results: SearchResult[];
  search_mode: string;
  top_score: number;
  elapsed_ms: number;
}

export const search = (params: {
  query: string;
  session_id: string;
  genre_weights?: Record<string, number>;
  decade_hint?: string | null;
  active_genres?: string[];
  active_decades?: string[];
}) => post<SearchResponse>('/search', {
  query: params.query,
  session_id: params.session_id,
  genre_weights: params.genre_weights || {},
  decade_hint: params.decade_hint || null,
  active_genres: params.active_genres || [],
  active_decades: params.active_decades || [],
});

// ── Enrich (batched LLM) ────────────────────────────────────

export interface EnrichResponse {
  reformulations: { query: string; dimension: string; rationale: string }[];
  explanations: Record<string, string>;
  filter_suggestions: { genres?: string[]; decades?: string[] };
}

export const enrich = (params: {
  query: string;
  session_id: string;
  movie_ids: number[];
}) => post<EnrichResponse>('/enrich', params);

// ── Feedback ─────────────────────────────────────────────────

export interface FeedbackItem { id: number; title: string; }

export const sendFeedback = (params: {
  session_id: string;
  movie_id: number;
  movie_title: string;
  action: 'like' | 'dislike' | 'clear';
}) => post<{ liked: FeedbackItem[]; disliked: FeedbackItem[] }>('/feedback', params);

// ── Detail / Neighborhood ────────────────────────────────────

export const getMovieDetail = (movieId: number) =>
  post<any>('/movie/detail', { movie_id: movieId });

export const getNeighborhood = (movieId: number, depth = 1) =>
  post<{ nodes: any[]; links: any[] }>('/movie/neighborhood', { movie_id: movieId, depth });

// ── Mixer ────────────────────────────────────────────────────

export const getMixerResults = (movieIds: number[], space = 'graph') =>
  post<{ connectors: SearchResult[] }>('/mixer', { movie_ids: movieIds, embedding_space: space });
