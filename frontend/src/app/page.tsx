'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import * as api from '@/lib/api';
import { loadUser, saveUser, deleteUser, deletePartyUsers } from '@/lib/firebase';
import type { SearchResult, FeedbackItem } from '@/lib/api';
import TopBar from '@/components/TopBar';
import SearchBar from '@/components/SearchBar';
import ReformulationSuggestions from '@/components/ReformulationSuggestions';
import FilterSidebar from '@/components/FilterSidebar';
import MovieGrid from '@/components/MovieGrid';
import MovieDetailPanel from '@/components/MovieDetailPanel';
import MixerBar from '@/components/MixerBar';
import StatusBar from '@/components/StatusBar';
import LikedDislikedPanel from '@/components/LikedDislikedPanel';
import FuseView from '@/components/FuseView';

// ── Welcome ──────────────────────────────────────────────────

function WelcomeScreen({ onJoin }: { onJoin: (name: string, party: string, sid: string, round: number, isAdmin: boolean) => void }) {
  const [name, setName] = useState('');
  const [partySecret, setPartySecret] = useState('');
  const [mode, setMode] = useState<'join' | 'create'>('join');
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const partyRef = useRef<HTMLInputElement>(null);
  useEffect(() => { partyRef.current?.focus(); }, []);

  const secret = partySecret.trim();
  const secretValid = secret.length >= 8;
  const canSubmit = name.trim().length > 0 && secret.length > 0;

  const handleSubmit = async () => {
    if (busy) return;
    if (!secretValid) { setError('Party secret must be at least 8 characters'); return; }
    if (!name.trim()) { setError('Please enter your name'); return; }
    setBusy(true); setError('');
    setStatus(mode === 'create' ? 'Creating party...' : 'Joining party...');
    try {
      const r = mode === 'create'
        ? await api.createParty(name.trim(), secret)
        : await api.joinParty(name.trim(), secret);
      setStatus('Loading profile...');
      onJoin(r.user_name, r.party_name, r.session_id, r.round, r.is_admin);
    } catch (e: any) {
      setBusy(false);
      setStatus('');
      const msg = e?.message || '';
      if (msg.includes('404') || msg.includes('not found')) setError('Party not found. Check your secret or create a new party.');
      else if (msg.includes('409')) setError('A party with this secret already exists. Try joining instead.');
      else setError(msg || 'Could not join party');
    }
  };
  return (
    <div className="min-h-screen flex items-center justify-center bg-surface-0">
      <div className="w-full max-w-md px-6 animate-fade-in">
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold tracking-tight mb-2">
            <span className="text-accent">Movie</span><span className="text-text-primary">Matcher</span>
          </h1>
          <p className="text-text-secondary text-sm mb-4">Find the perfect movie for everyone</p>
          <div className="bg-surface-1/60 border border-white/[0.04] rounded-xl p-4 text-left space-y-2">
            <p className="text-[12px] text-text-secondary leading-relaxed">
              <strong className="text-text-primary">How it works:</strong> Search for movies by mood, vibe, or genre.
              Like and dislike movies to teach the AI your taste. Use the Mixer to blend movies together,
              or steer results with genre and era controls.
            </p>
            <p className="text-[12px] text-text-secondary leading-relaxed">
              <strong className="text-text-primary">With friends:</strong> Everyone joins the same party, browses independently,
              then hits <span className="text-purple-400 font-medium">Fuse</span> — the AI finds what everyone will enjoy together.
            </p>
          </div>
        </div>
        <div className="bg-surface-1 border border-white/[0.06] rounded-2xl p-8 shadow-2xl space-y-4">
          <div className="grid grid-cols-2 gap-2 bg-surface-2 p-1 rounded-xl">
            <button
              onClick={() => setMode('join')}
              className={`py-2 rounded-lg text-sm font-medium transition-colors ${mode === 'join' ? 'bg-accent/20 text-accent-light' : 'text-text-dim hover:text-text-secondary'}`}
            >
              Join Party
            </button>
            <button
              onClick={() => setMode('create')}
              className={`py-2 rounded-lg text-sm font-medium transition-colors ${mode === 'create' ? 'bg-accent/20 text-accent-light' : 'text-text-dim hover:text-text-secondary'}`}
            >
              Create Party
            </button>
          </div>
          <div>
            <label className="block text-sm font-medium text-text-secondary mb-1.5">Party secret</label>
            <input ref={partyRef} type="password" value={partySecret} onChange={e => setPartySecret(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter' && secret) (document.getElementById('name-input') as HTMLInputElement)?.focus(); }}
              placeholder="Minimum 8 characters" maxLength={64}
              className="w-full bg-surface-2 border border-white/[0.08] rounded-xl px-4 py-3 text-base text-text-primary placeholder:text-text-dim/50 outline-none focus:border-accent/50 transition-colors" />
            <p className="text-[11px] text-text-dim mt-1">
              Must be at least 8 characters. Share this with your friends to join.
            </p>
          </div>
          <div>
            <label className="block text-sm font-medium text-text-secondary mb-1.5">Your name</label>
            <input id="name-input" type="text" value={name} onChange={e => setName(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') handleSubmit(); }} placeholder="Enter your name..." maxLength={24}
              className="w-full bg-surface-2 border border-white/[0.08] rounded-xl px-4 py-3 text-base text-text-primary placeholder:text-text-dim/50 outline-none focus:border-accent/50 transition-colors" />
          </div>
          <button onClick={handleSubmit} disabled={!canSubmit || busy}
            className="w-full py-3.5 bg-accent hover:bg-accent-light disabled:bg-surface-3 disabled:text-text-dim text-white text-base font-semibold rounded-xl transition-all mt-2">
            {status || (mode === 'create' ? 'Create Party' : 'Join Party')}
          </button>
          {error && <p className="text-sm text-red-400">{error}</p>}
        </div>
        <p className="text-center text-[11px] text-text-dim mt-6">
          Creator becomes admin. Members join with the same party secret.
        </p>
      </div>
    </div>
  );
}

// ── Types ────────────────────────────────────────────────────

interface Reformulation { query: string; dimension: string; rationale: string; }
interface MixerMovie { movieId: number; title: string; year?: number; poster_url?: string | null; weight: number; }
interface ViewState {
  results: SearchResult[];
  explanations: Record<string, string>;
  reformulations: Reformulation[];
  searchMode: string;
  topScore: number;
  elapsedMs: number;
}
const emptyView: ViewState = { results: [], explanations: {}, reformulations: [], searchMode: '', topScore: 0, elapsedMs: 0 };

type AppView = 'search' | 'mixer' | 'fuse';

// ── Main App ─────────────────────────────────────────────────

export default function HomePage() {
  const [userName, setUserName] = useState<string | null>(null);
  const [partyName, setPartyName] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [partyMembers, setPartyMembers] = useState<{ session_id: string; name: string }[]>([]);
  const [currentRound, setCurrentRound] = useState(1);

  const [searchView, setSearchView] = useState<ViewState>({ ...emptyView });
  const [mixerView, setMixerView] = useState<ViewState>({ ...emptyView });
  const [activeView, setActiveView] = useState<AppView>('search');

  const isMixer = activeView === 'mixer';
  const currentView = isMixer ? mixerView : searchView;

  const [query, setQuery] = useState('');
  const [isSearching, setIsSearching] = useState(false);
  const [isEnriching, setIsEnriching] = useState(false);
  const [isReformLoading, setIsReformLoading] = useState(false);
  const [refsCollapsed, setRefsCollapsed] = useState(false);
  const [suggestedFilters, setSuggestedFilters] = useState<{ genres?: string[]; decades?: string[] }>({});

  const [availableGenres, setAvailableGenres] = useState<string[]>([]);
  const [availableDecades, setAvailableDecades] = useState<string[]>([]);
  const [activeGenres, setActiveGenres] = useState<string[]>([]);
  const [activeDecades, setActiveDecades] = useState<string[]>([]);
  const [genreWeights, setGenreWeights] = useState<Record<string, number>>({});
  const [decadeHints, setDecadeHints] = useState<string[]>([]);

  const [lam, setLam] = useState(0.5);
  const [prefIntensity, setPrefIntensity] = useState(0.5);
  const [sortBy, setSortBy] = useState('relevance');
  const [sortDir, setSortDir] = useState('desc');
  const [imdbMin, setImdbMin] = useState<number | null>(6);
  const [searchPage, setSearchPage] = useState(0);
  const [mixerPage, setMixerPage] = useState(0);
  const [imdbMax] = useState<number | null>(null);
  const [steeringStrength, setSteeringStrength] = useState(0.6);

  const [liked, setLiked] = useState<FeedbackItem[]>([]);
  const [disliked, setDisliked] = useState<FeedbackItem[]>([]);
  const [showLikedPanel, setShowLikedPanel] = useState(false);

  const [selectedMovie, setSelectedMovie] = useState<SearchResult | null>(null);
  const [movieDetail, setMovieDetail] = useState<any>(null);
  const [neighborhood, setNeighborhood] = useState<any>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const [mixerMovies, setMixerMovies] = useState<MixerMovie[]>([]);
  const [groupPerspectives, setGroupPerspectives] = useState<Record<string, string>>({});

  // Refs
  const queryRef = useRef(query); queryRef.current = query;
  const sessionRef = useRef(sessionId); sessionRef.current = sessionId;
  const partyRef = useRef(partyName); partyRef.current = partyName;
  const mixerRef = useRef(mixerMovies); mixerRef.current = mixerMovies;
  const enrichAbort = useRef<AbortController | null>(null);
  const lastReformQuery = useRef('');
  const lastReformWordCount = useRef(0);
  const reformTimer = useRef<ReturnType<typeof setTimeout>>();
  const reformInFlight = useRef(false);
  const searchTimerRef = useRef<ReturnType<typeof setTimeout>>();
  const mixerTimerRef = useRef<ReturnType<typeof setTimeout>>();
  const saveTimer = useRef<ReturnType<typeof setTimeout>>();
  const countWords = (s: string) => s.trim().split(/\s+/).filter(Boolean).length;

  useEffect(() => { api.getFilters().then(f => { setAvailableGenres(f.genres); setAvailableDecades(f.decades); }).catch(() => {}); }, []);

  // ── Firebase load ──────────────────────────────────────────
  const handleJoin = useCallback(async (name: string, party: string, sid: string, round: number, admin: boolean) => {
    setUserName(name); setPartyName(party); setSessionId(sid); setCurrentRound(round); setIsAdmin(admin);
    const saved = await loadUser(party, name);
    if (saved) {
      console.log('[Firebase] Loaded', name, 'in', party);
      if (saved.liked?.length) setLiked(saved.liked);
      if (saved.disliked?.length) setDisliked(saved.disliked);
      if (saved.activeGenres?.length) setActiveGenres(saved.activeGenres);
      if (saved.activeDecades?.length) setActiveDecades(saved.activeDecades);
      if (saved.genreWeights && Object.keys(saved.genreWeights).length) setGenreWeights(saved.genreWeights);
      if (saved.decadeHints?.length) setDecadeHints(saved.decadeHints);
      if (saved.lam != null) setLam(saved.lam);
      if (saved.prefIntensity != null) setPrefIntensity(saved.prefIntensity);
      if (saved.sortBy) setSortBy(saved.sortBy);
      if (saved.sortDir) setSortDir(saved.sortDir);
      if (saved.imdbMin != null) setImdbMin(saved.imdbMin);
      if (saved.imdbMax != null) setImdbMax(saved.imdbMax);
      if (saved.steeringStrength != null) setSteeringStrength(saved.steeringStrength);
      if (saved.mixerMovies?.length) setMixerMovies(saved.mixerMovies);
      if (saved.lastQuery) setQuery(saved.lastQuery);
      for (const item of (saved.liked || [])) api.sendFeedback({ session_id: sid, movie_id: item.id, movie_title: item.title, action: 'like' }).catch(() => {});
      for (const item of (saved.disliked || [])) api.sendFeedback({ session_id: sid, movie_id: item.id, movie_title: item.title, action: 'dislike' }).catch(() => {});
    }
  }, []);

  // ── Firebase save ──────────────────────────────────────────
  useEffect(() => {
    if (!userName || !partyName) return;
    clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      saveUser(partyName, userName, {
        liked, disliked, activeGenres, activeDecades, genreWeights, decadeHints,
        lam, prefIntensity, sortBy, sortDir, imdbMin, imdbMax, steeringStrength,
        mixerMovies, lastQuery: query,
      });
    }, 2000);
  }, [userName, partyName, liked, disliked, activeGenres, activeDecades, genreWeights, decadeHints,
      lam, prefIntensity, sortBy, sortDir, imdbMin, imdbMax, steeringStrength, mixerMovies, query]);

  useEffect(() => {
    if (!sessionId || !partyName) return;
    let cancelled = false;
    const refresh = async () => {
      try {
        const s = await api.partyStatus(sessionId, partyName);
        if (cancelled) return;
        setIsAdmin(s.admin_sid === sessionId);
        setPartyMembers((s.users || []).map(u => ({ session_id: u.session_id, name: u.name })));
      } catch { }
    };
    refresh();
    const timer = setInterval(refresh, 4000);
    return () => { cancelled = true; clearInterval(timer); };
  }, [sessionId, partyName]);

  const resetToWelcome = useCallback(() => {
    setUserName(null); setPartyName(null); setSessionId(null); setCurrentRound(1); setIsAdmin(false); setPartyMembers([]);
    setActiveView('search'); setSearchView({ ...emptyView }); setMixerView({ ...emptyView });
    setQuery(''); setLiked([]); setDisliked([]); setActiveGenres([]); setActiveDecades([]);
    setGenreWeights({}); setDecadeHints([]); setMixerMovies([]); setGroupPerspectives({});
    setSelectedMovie(null); setMovieDetail(null); setNeighborhood(null);
  }, []);

  const handleLeaveParty = useCallback(async () => {
    if (!sessionId || !partyName || !userName) return;
    await api.partyLeave(sessionId, partyName);
    await deleteUser(partyName, userName);
    resetToWelcome();
  }, [sessionId, partyName, userName, resetToWelcome]);

  const handleLeaveAssign = useCallback(async (newAdminSid: string) => {
    if (!sessionId || !partyName || !userName) return;
    await api.partyLeaveAssign(sessionId, partyName, newAdminSid);
    await deleteUser(partyName, userName);
    resetToWelcome();
  }, [sessionId, partyName, userName, resetToWelcome]);

  const handleCancelParty = useCallback(async () => {
    if (!sessionId || !partyName) return;
    await api.partyCancel(sessionId, partyName);
    await deletePartyUsers(partyName);
    resetToWelcome();
  }, [sessionId, partyName, resetToWelcome]);

  // ── Clear results when query emptied / mixer cleared ───────
  useEffect(() => {
    if (!query.trim() && activeView === 'search') setSearchView({ ...emptyView });
  }, [query, activeView]);
  useEffect(() => {
    if (activeView === 'mixer' && mixerMovies.length === 0) setMixerView({ ...emptyView });
  }, [activeView, mixerMovies.length]);

  // ── Core search ────────────────────────────────────────────
  const doSearchForView = useCallback(async (
    target: 'search' | 'mixer', q: string, mixerW?: Record<string, number>, page?: number,
  ) => {
    if (!sessionRef.current) return;
    const isMixerSearch = !!mixerW && Object.keys(mixerW).length >= 2;
    if (!q.trim() && !isMixerSearch) return;

    enrichAbort.current?.abort();
    setIsSearching(true);
    setGroupPerspectives({});
    const viewSetter = target === 'mixer' ? setMixerView : setSearchView;
    const p = page ?? (target === 'mixer' ? mixerPage : searchPage);

    try {
      const resp = await api.search({
        query: q || '', session_id: sessionRef.current,
        genre_weights: genreWeights, decade_hints: decadeHints,
        active_genres: activeGenres, active_decades: activeDecades,
        mixer_weights: mixerW || {}, lam,
        pref_intensity: prefIntensity,
        imdb_min: imdbMin, imdb_max: imdbMax, sort_by: sortBy, sort_dir: sortDir,
        steering_strength: steeringStrength,
        min_results: 10 * (p + 1),
      });

      viewSetter(prev => ({ ...prev, results: resp.search_results, searchMode: resp.search_mode,
        topScore: resp.top_score, elapsedMs: resp.elapsed_ms, explanations: {} }));
      setIsSearching(false);

      const movieIds = resp.search_results.map(r => r.movieId).filter(Boolean);
      const enrichQuery = q.trim() ? q : (isMixerSearch ? `Movies similar to ${mixerRef.current.map(m => m.title).join(' + ')}` : '');
      if (movieIds.length > 0 && enrichQuery) {
        setIsEnriching(true);
        const ac = new AbortController(); enrichAbort.current = ac;
        try {
          const e = await api.enrich({ query: enrichQuery, session_id: sessionRef.current!,
            movie_ids: movieIds, party_name: partyRef.current || '' });
          if (ac.signal.aborted) return;
          viewSetter(prev => ({ ...prev, explanations: e.explanations || {}, reformulations: e.reformulations || [] }));
          setSuggestedFilters(e.filter_suggestions || {}); setRefsCollapsed(false);
        } catch { } finally { if (!ac.signal.aborted) setIsEnriching(false); }

        // Fetch group perspectives in round 2+ (after enrich, non-blocking)
        if (currentRound > 1 && partyRef.current && movieIds.length > 0) {
          api.groupPerspective(partyRef.current, movieIds.slice(0, 8))
            .then(gp => { if (!ac.signal.aborted) setGroupPerspectives(gp.perspectives || {}); })
            .catch(() => {});
        }
      }
    } catch (err) { console.error('Search error:', err); setIsSearching(false); }
  }, [genreWeights, decadeHints, activeGenres, activeDecades, lam, prefIntensity, sortBy, sortDir, imdbMin, imdbMax, searchPage, mixerPage, steeringStrength, currentRound]);

  const handleSubmit = useCallback(() => {
    if (activeView === 'mixer' && mixerRef.current.length >= 2) {
      const w: Record<string, number> = {}; mixerRef.current.forEach(m => { w[String(m.movieId)] = m.weight; }); doSearchForView('mixer', '', w);
    } else {
      doSearchForView('search', queryRef.current);
    }
  }, [doSearchForView, activeView]);

  const handleReformulationClick = useCallback((ref: Reformulation) => {
    setQuery(ref.query); queryRef.current = ref.query; doSearchForView('search', ref.query);
  }, [doSearchForView]);

  // ── Auto-search: mixer ─────────────────────────────────────
  useEffect(() => {
    if (activeView !== 'mixer' || mixerMovies.length < 2) return;
    clearTimeout(mixerTimerRef.current);
    mixerTimerRef.current = setTimeout(() => {
      const w: Record<string, number> = {}; mixerMovies.forEach(m => { w[String(m.movieId)] = m.weight; }); doSearchForView('mixer', '', w);
    }, 600);
    return () => clearTimeout(mixerTimerRef.current);
  }, [mixerMovies, activeView, doSearchForView]);

  // ── Auto-search: filter/settings ───────────────────────────
  // Reset pages when filters change (not when pages themselves change)
  useEffect(() => {
    setSearchPage(0);
    setMixerPage(0);
  }, [activeGenres, activeDecades, genreWeights, decadeHints, sortBy, sortDir, imdbMin, imdbMax, steeringStrength]);

  useEffect(() => {
    if (activeView === 'fuse') return; // don't auto-search in fuse view
    clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => {
      if (activeView === 'mixer' && mixerRef.current.length >= 2) {
        const w: Record<string, number> = {}; mixerRef.current.forEach(m => { w[String(m.movieId)] = m.weight; }); doSearchForView('mixer', '', w);
      } else if (queryRef.current.trim()) doSearchForView('search', queryRef.current);
    }, 400);
    return () => clearTimeout(searchTimerRef.current);
  }, [activeGenres, activeDecades, genreWeights, decadeHints, lam, prefIntensity, sortBy, sortDir,
      imdbMin, imdbMax, steeringStrength, activeView, doSearchForView]);

  // ── Typing reformulations ──────────────────────────────────
  const fetchReformulations = useCallback(async (q: string) => {
    if (!q.trim() || q.length < 5 || !sessionRef.current) return;
    if (reformInFlight.current) {
      console.log('[REFORM] Skipped — already in flight');
      return;
    }
    reformInFlight.current = true; setIsReformLoading(true);
    console.log('[REFORM] Fetching for:', q);
    try {
      const resp = await api.reformulate({
        query: q, session_id: sessionRef.current,
        party_name: partyRef.current || '',
      });
      if (resp.reformulations?.length > 0) {
        console.log('[REFORM] Got', resp.reformulations.length, 'reformulations');
        setSearchView(prev => ({ ...prev, reformulations: resp.reformulations }));
        setRefsCollapsed(false);
      }
      lastReformQuery.current = q; lastReformWordCount.current = countWords(q);
    } catch (e) {
      console.error('[REFORM] Error:', e);
    } finally {
      reformInFlight.current = false; setIsReformLoading(false);
    }
  }, []);

  // Watch query changes for typing-triggered reformulations
  // Uses its OWN timer (reformTimer) separate from search timers
  useEffect(() => {
    if (!query.trim() || !sessionId || activeView !== 'search') return;

    const wc = countWords(query);
    const wordDiff = wc - lastReformWordCount.current;

    // Trigger immediately if 3+ new words typed
    if (wordDiff >= 3 && query !== lastReformQuery.current) {
      clearTimeout(reformTimer.current);
      fetchReformulations(query);
      return;
    }

    // Otherwise set a 5-second timer
    clearTimeout(reformTimer.current);
    if (query !== lastReformQuery.current && wc >= 2) {
      reformTimer.current = setTimeout(() => {
        console.log('[REFORM] 5s timer fired');
        fetchReformulations(queryRef.current);
      }, 5000);
    }

    return () => clearTimeout(reformTimer.current);
  }, [query, sessionId, activeView, fetchReformulations]);

  // ── Feedback ───────────────────────────────────────────────
  const handleFeedback = useCallback(async (movie: SearchResult, action: 'like' | 'dislike' | 'clear') => {
    if (!sessionRef.current) return;
    try { const r = await api.sendFeedback({ session_id: sessionRef.current, movie_id: movie.movieId, movie_title: movie.title, action }); setLiked(r.liked); setDisliked(r.disliked); } catch { }
  }, []);

  const handleMovieClick = useCallback(async (movie: SearchResult) => {
    setSelectedMovie(movie); setDetailLoading(true); setMovieDetail(null); setNeighborhood(null);
    try { const [d, n] = await Promise.all([api.getMovieDetail(movie.movieId), api.getNeighborhood(movie.movieId)]); setMovieDetail(d); setNeighborhood(n); } catch { } finally { setDetailLoading(false); }
  }, []);

  // ── Graph-triggered actions ────────────────────────────────
  const handleAddGenreFilterFromGraph = useCallback((genre: string) => {
    setActiveGenres(p => p.includes(genre) ? p : [...p, genre]);
    setGenreWeights({});
  }, []);

  const handleAddGenreMixerFromGraph = useCallback((genre: string) => {
    setGenreWeights(prev => {
      const keys = Object.keys(prev);
      if (keys.length >= 5 || prev[genre]) return prev;
      const next = { ...prev, [genre]: 1 };
      const total = Object.values(next).reduce((a, b) => a + b, 0);
      for (const k of Object.keys(next)) next[k] = next[k] / total;
      return next;
    });
    setActiveGenres([]);
  }, []);

  const handleGraphSearch = useCallback(async (nodeType: string, nodeName: string, sourceMovie: string) => {
    if (!sessionRef.current) return;
    setSelectedMovie(null); setMovieDetail(null); setNeighborhood(null);
    setActiveView('search');
    setIsSearching(true);
    try {
      const resp = await api.graphSearch({
        session_id: sessionRef.current,
        party_name: partyRef.current || '',
        node_type: nodeType, node_name: nodeName, source_movie: sourceMovie,
      });
      setQuery(resp.query); queryRef.current = resp.query;
      setSearchView(prev => ({
        ...prev, results: resp.search_results, searchMode: resp.search_mode,
        topScore: Math.max(...resp.search_results.map(r => r.score || r.sem_score || 0), 0),
        elapsedMs: resp.elapsed_ms, explanations: {},
      }));
      setIsSearching(false);
      const movieIds = resp.search_results.map(r => r.movieId).filter(Boolean);
      if (movieIds.length > 0 && resp.query) {
        setIsEnriching(true);
        try {
          const e = await api.enrich({ query: resp.query, session_id: sessionRef.current!, movie_ids: movieIds, party_name: partyRef.current || '' });
          setSearchView(prev => ({ ...prev, explanations: e.explanations || {}, reformulations: e.reformulations || [] }));
          setSuggestedFilters(e.filter_suggestions || {});
        } catch { } finally { setIsEnriching(false); }
      }
    } catch (err) { console.error('Graph search error:', err); setIsSearching(false); }
  }, []);

  // ── Filter toggles ────────────────────────────────────────
  const toggleGenre = useCallback((g: string) => { setActiveGenres(p => p.includes(g) ? p.filter(x => x !== g) : [...p, g]); setGenreWeights({}); }, []);
  const toggleDecade = useCallback((d: string) => { setActiveDecades(p => p.includes(d) ? p.filter(x => x !== d) : [...p, d]); setDecadeHints([]); }, []);
  const handleGenreWeightsChange = useCallback((w: Record<string, number>) => { setGenreWeights(w); if (Object.keys(w).length > 0) setActiveGenres([]); }, []);
  const handleDecadeHintsChange = useCallback((h: string[]) => { setDecadeHints(h); if (h.length > 0) setActiveDecades([]); }, []);
  const handleClearAll = useCallback(() => {
    setActiveGenres([]); setActiveDecades([]); setGenreWeights({}); setDecadeHints([]);
    setImdbMin(6); setImdbMax(null); setSortBy('relevance'); setSortDir('desc');
    setSearchPage(0); setMixerPage(0);
  }, []);

  const handleMoreResults = useCallback(() => {
    if (activeView === 'mixer') setMixerPage(p => p + 1);
    else setSearchPage(p => p + 1);
  }, [activeView]);

  // Reset pages when query changes
  useEffect(() => { setSearchPage(0); }, [query]);
  useEffect(() => { setMixerPage(0); }, [mixerMovies]);

  const likedIds = new Set(liked.map(l => l.id));
  const dislikedIds = new Set(disliked.map(d => d.id));

  if (!userName || !partyName || !sessionId) return <WelcomeScreen onJoin={handleJoin} />;

  return (
    <div className="min-h-screen flex flex-col">
      <TopBar userName={userName} partyName={partyName}
        isAdmin={isAdmin} partyMembers={partyMembers} sessionId={sessionId}
        view={activeView} onViewChange={setActiveView} currentRound={currentRound}
        steeringStrength={steeringStrength} onSteeringStrengthChange={setSteeringStrength}
        likedCount={liked.length} dislikedCount={disliked.length}
        mixerCount={mixerMovies.length} onShowLikedPanel={() => setShowLikedPanel(true)}
        onLeaveParty={handleLeaveParty} onLeaveAssign={handleLeaveAssign} onCancelParty={handleCancelParty} />

      <main className="flex-1 flex flex-col">
        {/* ═══ FUSE VIEW ═══ */}
        {activeView === 'fuse' ? (
          <FuseView sessionId={sessionId} partyName={partyName}
            currentRound={currentRound} onRoundChange={setCurrentRound}
            likedIds={likedIds} dislikedIds={dislikedIds}
            onFeedback={handleFeedback} onMovieClick={handleMovieClick} />
        ) : (
          <>
            {/* Top area: search bar or mixer bar */}
            <div className="px-6 pt-4 pb-2 max-w-[1400px] mx-auto w-full">
              {activeView === 'mixer' ? (
                <MixerBar selected={mixerMovies} onUpdate={setMixerMovies} sessionId={sessionId} />
              ) : (
                <>
                  <SearchBar value={query} onChange={setQuery} onSubmit={handleSubmit} isSearching={isSearching} />
                  <ReformulationSuggestions reformulations={currentView.reformulations} onClick={handleReformulationClick}
                    isCollapsed={refsCollapsed} onToggleCollapse={() => setRefsCollapsed(p => !p)} isLoading={isEnriching || isReformLoading} />
                </>
              )}
            </div>

            {/* Filters + results */}
            <div className="flex-1 flex max-w-[1400px] mx-auto w-full px-4">
              <FilterSidebar availableGenres={availableGenres} availableDecades={availableDecades}
                activeGenres={activeGenres} activeDecades={activeDecades}
                suggestedFilters={suggestedFilters} genreWeights={genreWeights} decadeHints={decadeHints}
                onToggleGenre={toggleGenre} onToggleDecade={toggleDecade}
                onGenreWeightsChange={handleGenreWeightsChange} onDecadeHintsChange={handleDecadeHintsChange}
                onClearAll={handleClearAll}
                liked={liked} disliked={disliked} onShowLikedPanel={() => setShowLikedPanel(true)}
                sortBy={sortBy} sortDir={sortDir} onSortChange={(by, dir) => { setSortBy(by); setSortDir(dir); }}
                imdbMin={imdbMin} onImdbMinChange={setImdbMin} />

              <div className="flex-1 min-w-0 pl-4 relative">
                <StatusBar mode={currentView.searchMode} count={currentView.results.length}
                  isSearching={isSearching} topScore={currentView.topScore} elapsedMs={currentView.elapsedMs} isEnriching={isEnriching} />

                {isSearching && currentView.results.length > 0 && (
                  <div className="absolute inset-0 z-10 bg-surface-0/60 backdrop-blur-[2px] flex items-start justify-center pt-24 pointer-events-none rounded-lg">
                    <div className="flex items-center gap-3 bg-surface-2/90 border border-white/[0.08] rounded-xl px-5 py-3 shadow-xl">
                      <svg className="w-5 h-5 text-accent animate-spin" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                      </svg>
                      <span className="text-sm text-text-secondary">Searching...</span>
                    </div>
                  </div>
                )}

                <MovieGrid movies={currentView.results} explanations={currentView.explanations}
                  groupPerspectives={currentRound > 1 ? groupPerspectives : undefined}
                  likedIds={likedIds} dislikedIds={dislikedIds} onMovieClick={handleMovieClick} onFeedback={handleFeedback}
                  isLoading={isSearching && currentView.results.length === 0} isEnriching={isEnriching} />

                {currentView.results.length > 0 && !isSearching && (
                  <button onClick={handleMoreResults}
                    className="w-full py-3 text-sm text-accent hover:text-accent-light border border-accent/20 hover:border-accent/40 rounded-xl mt-2 mb-6 transition-colors">
                    Load more results
                  </button>
                )}
              </div>
            </div>
          </>
        )}
      </main>

      {selectedMovie && (
        <MovieDetailPanel movie={selectedMovie} detail={movieDetail} neighborhood={neighborhood}
          explanation={currentView.explanations[String(selectedMovie.movieId)]}
          groupPerspective={currentRound > 1 ? groupPerspectives[String(selectedMovie.movieId)] : undefined}
          isLoading={detailLoading} isLiked={likedIds.has(selectedMovie.movieId)} isDisliked={dislikedIds.has(selectedMovie.movieId)}
          onClose={() => { setSelectedMovie(null); setMovieDetail(null); setNeighborhood(null); }}
          onFeedback={handleFeedback}
          onAddGenreFilter={handleAddGenreFilterFromGraph}
          onAddGenreMixer={handleAddGenreMixerFromGraph}
          onGraphSearch={handleGraphSearch} />
      )}

      {showLikedPanel && (
        <LikedDislikedPanel liked={liked} disliked={disliked} onClose={() => setShowLikedPanel(false)}
          onRemoveLike={(id, title) => handleFeedback({ movieId: id, title } as SearchResult, 'clear')}
          onRemoveDislike={(id, title) => handleFeedback({ movieId: id, title } as SearchResult, 'clear')} />
      )}
    </div>
  );
}