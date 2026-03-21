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

export const createParty = (user_name: string, party_secret: string) =>
  post<{ session_id: string; user_name: string; party_name: string; round: number; is_admin: boolean }>(
    '/party/create', { user_name, party_secret },
  );

export const joinParty = (user_name: string, party_secret: string) =>
  post<{ session_id: string; user_name: string; party_name: string; round: number; is_admin: boolean }>(
    '/party/join', { user_name, party_secret },
  );

export const getSession = (sid: string) => get<any>(`/session/${sid}`);

// ── Filters ──────────────────────────────────────────────────

export const getFilters = () =>
  get<{ genres: string[]; decades: string[] }>('/filters');

// ── Search ───────────────────────────────────────────────────

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
  imdb_rating?: number | null;
  imdb_votes?: number | null;
}

export interface SearchResponse {
  search_results: SearchResult[];
  search_mode: string;
  top_score: number;
  elapsed_ms: number;
}

export const search = (params: {
  query?: string;
  session_id: string;
  genre_weights?: Record<string, number>;
  decade_hints?: string[];
  active_genres?: string[];
  active_decades?: string[];
  mixer_weights?: Record<string, number>;
  lam?: number;
  pref_intensity?: number;
  imdb_min?: number | null;
  imdb_max?: number | null;
  sort_by?: string;
  sort_dir?: string;
  k_fetch?: number;
  steering_strength?: number;
}) => post<SearchResponse>('/search', {
  query: params.query || '',
  session_id: params.session_id,
  genre_weights: params.genre_weights || {},
  decade_hints: params.decade_hints || [],
  active_genres: params.active_genres || [],
  active_decades: params.active_decades || [],
  mixer_weights: params.mixer_weights || {},
  lam: params.lam ?? 0.6,
  pref_intensity: params.pref_intensity ?? 0,
  imdb_min: params.imdb_min ?? null,
  imdb_max: params.imdb_max ?? null,
  sort_by: params.sort_by || 'relevance',
  sort_dir: params.sort_dir || 'desc',
  k_fetch: params.k_fetch ?? 30,
  steering_strength: params.steering_strength ?? 0.6,
});

// ── Reformulate ──────────────────────────────────────────────

export const reformulate = (params: { query: string; session_id: string; party_name?: string }) =>
  post<{ reformulations: { query: string; dimension: string; rationale: string }[] }>(
    '/reformulate', params,
  );

// ── Enrich ───────────────────────────────────────────────────

export interface EnrichResponse {
  reformulations: { query: string; dimension: string; rationale: string }[];
  explanations: Record<string, string>;
  filter_suggestions: { genres?: string[]; decades?: string[] };
}

export const enrich = (params: {
  query: string;
  session_id: string;
  movie_ids: number[];
  party_name?: string;
}) => post<EnrichResponse>('/enrich', params);

// ── Feedback ─────────────────────────────────────────────────

export interface FeedbackItem { id: number; title: string; }

export const sendFeedback = (params: {
  session_id: string;
  movie_id: number;
  movie_title: string;
  action: 'like' | 'dislike' | 'clear';
}) => post<{ liked: FeedbackItem[]; disliked: FeedbackItem[] }>('/feedback', params);

// ── Movie Detail / Neighborhood ──────────────────────────────

export const getMovieDetail = (movieId: number) =>
  post<any>('/movie/detail', { movie_id: movieId });

export const getNeighborhood = (movieId: number, depth = 1) =>
  post<{ nodes: any[]; links: any[] }>('/movie/neighborhood', { movie_id: movieId, depth });

// ── Movie Autocomplete (for mixer) ───────────────────────────

export interface AutocompleteResult {
  movieId: number;
  title: string;
  year?: number;
  poster_url?: string | null;
  is_liked: boolean;
}

export const movieAutocomplete = (q: string, sessionId: string) =>
  post<{ results: AutocompleteResult[] }>('/movie/autocomplete', { q, session_id: sessionId });

// ── Fuse (Party multiplayer) ─────────────────────────────────

export interface PartyUser {
  session_id: string;
  name: string;
  ready: boolean;
  liked_count: number;
  disliked_count: number;
  search_count: number;
  is_admin: boolean;
}

export interface RoundSummary {
  round: number;
  summary: {
    similarities: string | string[];
    differences: string | string[];
    group_query: string;
    reasoning: string;
  };
  suggestions: {
    movieId: number;
    title: string;
    year?: number;
    poster_url?: string | null;
    genres: string[];
    imdb_rating?: number;
    overview?: string;
  }[];
}

export interface PartyStatus {
  party_name: string;
  round: number;
  users: PartyUser[];
  admin_sid: string | null;
  all_ready: boolean;
  fuse_in_progress: boolean;
  round_summaries: RoundSummary[];
}

export const partyStatus = (sessionId: string, partyName: string) =>
  post<PartyStatus>('/party/status', { session_id: sessionId, party_name: partyName });

export const partyReady = (sessionId: string, partyName: string, ready: boolean) =>
  post<PartyStatus>('/party/ready', { session_id: sessionId, party_name: partyName, ready });

export const partyFuse = (sessionId: string, partyName: string) =>
  post<{ status: string; round_data?: RoundSummary; new_round?: number; message?: string }>(
    '/party/fuse', { session_id: sessionId, party_name: partyName });

export const partyRemove = (sessionId: string, partyName: string, targetSid: string) =>
  post<PartyStatus>('/party/remove', { session_id: sessionId, party_name: partyName, target_sid: targetSid });

export const partyLeave = (sessionId: string, partyName: string) =>
  post<{ status: string }>('/party/leave', { session_id: sessionId, party_name: partyName });

export const partyLeaveAssign = (sessionId: string, partyName: string, newAdminSid: string) =>
  post<{ status: string; new_admin_sid: string }>(
    '/party/leave_assign', { session_id: sessionId, party_name: partyName, new_admin_sid: newAdminSid },
  );

export const partyCancel = (sessionId: string, partyName: string) =>
  post<{ status: string }>('/party/cancel', { session_id: sessionId, party_name: partyName });

export const groupPerspective = (partyName: string, movieIds: number[]) =>
  post<{ perspectives: Record<string, string> }>(
    '/party/group_perspective', { party_name: partyName, movie_ids: movieIds });