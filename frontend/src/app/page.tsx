'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import * as api from '@/lib/api';
import type { SearchResult, EnrichResponse, FeedbackItem } from '@/lib/api';
import TopBar from '@/components/TopBar';
import SearchBar from '@/components/SearchBar';
import ReformulationSuggestions from '@/components/ReformulationSuggestions';
import FilterSidebar from '@/components/FilterSidebar';
import MovieGrid from '@/components/MovieGrid';
import MovieDetailPanel from '@/components/MovieDetailPanel';
import MovieMixer from '@/components/MovieMixer';
import StatusBar from '@/components/StatusBar';
import LikedDislikedPanel from '@/components/LikedDislikedPanel';

// ================================================================
// KAHOOT-STYLE WELCOME SCREEN
// ================================================================

function WelcomeScreen({ onJoin }: { onJoin: (name: string, sid: string) => void }) {
  const [name, setName] = useState('');
  const [joining, setJoining] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => { inputRef.current?.focus(); }, []);

  const handleJoin = async () => {
    const trimmed = name.trim();
    if (!trimmed || joining) return;
    setJoining(true);
    try {
      const resp = await api.join(trimmed);
      onJoin(resp.user_name, resp.session_id);
    } catch {
      setJoining(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-surface-0">
      <div className="w-full max-w-md px-6 animate-fade-in">
        {/* Logo */}
        <div className="text-center mb-10">
          <h1 className="text-4xl font-bold tracking-tight mb-2">
            <span className="text-accent">Movie</span>
            <span className="text-text-primary">Matcher</span>
          </h1>
          <p className="text-text-secondary text-sm">Find the perfect movie for everyone</p>
        </div>

        {/* Join card */}
        <div className="bg-surface-1 border border-white/[0.06] rounded-2xl p-8 shadow-2xl">
          <label className="block text-sm font-medium text-text-secondary mb-2">
            What&apos;s your name?
          </label>
          <input
            ref={inputRef}
            type="text"
            value={name}
            onChange={e => setName(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') handleJoin(); }}
            placeholder="Enter your name..."
            maxLength={24}
            className="w-full bg-surface-2 border border-white/[0.08] rounded-xl px-4 py-3.5 text-lg text-text-primary placeholder:text-text-dim/50 outline-none focus:border-accent/50 transition-colors mb-4"
          />
          <button
            onClick={handleJoin}
            disabled={!name.trim() || joining}
            className="w-full py-3.5 bg-accent hover:bg-accent-light disabled:bg-surface-3 disabled:text-text-dim text-white text-base font-semibold rounded-xl transition-all"
          >
            {joining ? 'Joining...' : 'Join Movie Night'}
          </button>
        </div>

        <p className="text-center text-[11px] text-text-dim mt-6">
          AI-powered search · Knowledge graph · Group recommendations
        </p>
      </div>
    </div>
  );
}


// ================================================================
// MAIN APP (after login)
// ================================================================

interface Reformulation { query: string; dimension: string; rationale: string; }

export default function HomePage() {
  // ── Auth ────────────────────────────────────────────────────
  const [userName, setUserName] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);

  // ── Search ─────────────────────────────────────────────────
  const [query, setQuery] = useState('');
  const [isSearching, setIsSearching] = useState(false);
  const [isEnriching, setIsEnriching] = useState(false);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [searchMode, setSearchMode] = useState('');
  const [topScore, setTopScore] = useState(0);
  const [elapsedMs, setElapsedMs] = useState(0);

  // ── Enrichment (from LLM, arrives after results) ───────────
  const [reformulations, setReformulations] = useState<Reformulation[]>([]);
  const [refsCollapsed, setRefsCollapsed] = useState(false);
  const [explanations, setExplanations] = useState<Record<string, string>>({});
  const [suggestedFilters, setSuggestedFilters] = useState<{ genres?: string[]; decades?: string[] }>({});

  // ── Filters ────────────────────────────────────────────────
  const [availableGenres, setAvailableGenres] = useState<string[]>([]);
  const [availableDecades, setAvailableDecades] = useState<string[]>([]);
  const [activeGenres, setActiveGenres] = useState<string[]>([]);
  const [activeDecades, setActiveDecades] = useState<string[]>([]);
  const [genreWeights, setGenreWeights] = useState<Record<string, number>>({});
  const [decadeHint, setDecadeHint] = useState<string | null>(null);

  // ── Preferences ────────────────────────────────────────────
  const [liked, setLiked] = useState<FeedbackItem[]>([]);
  const [disliked, setDisliked] = useState<FeedbackItem[]>([]);
  const [showLikedPanel, setShowLikedPanel] = useState(false);
  const [prefIntensity, setPrefIntensity] = useState(0.5);

  // ── Detail panel ───────────────────────────────────────────
  const [selectedMovie, setSelectedMovie] = useState<SearchResult | null>(null);
  const [movieDetail, setMovieDetail] = useState<any>(null);
  const [neighborhood, setNeighborhood] = useState<any>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // ── Views ──────────────────────────────────────────────────
  const [view, setView] = useState<'search' | 'mixer'>('search');
  const [mixerMovies, setMixerMovies] = useState<SearchResult[]>([]);

  // Refs for stable access
  const queryRef = useRef(query);
  queryRef.current = query;
  const sessionRef = useRef(sessionId);
  sessionRef.current = sessionId;
  const enrichAbort = useRef<AbortController | null>(null);

  // ── Load filters ───────────────────────────────────────────
  useEffect(() => {
    api.getFilters().then(f => {
      setAvailableGenres(f.genres);
      setAvailableDecades(f.decades);
    }).catch(() => {});
  }, []);

  // ── TWO-PHASE SEARCH ──────────────────────────────────────
  // Phase 1: Fast search (Neo4j only, <1s) → show results
  // Phase 2: Enrich (batched LLM, 3-8s) → add AI reasoning

  const doSearch = useCallback(async (q: string) => {
    if (!q.trim() || !sessionRef.current) return;

    // Abort any in-flight enrichment
    enrichAbort.current?.abort();

    setIsSearching(true);
    try {
      // ── Phase 1: Fast search ────────────────────────────
      const resp = await api.search({
        query: q,
        session_id: sessionRef.current,
        genre_weights: genreWeights,
        decade_hint: decadeHint,
        active_genres: activeGenres,
        active_decades: activeDecades,
      });

      setResults(resp.search_results);
      setSearchMode(resp.search_mode);
      setTopScore(resp.top_score);
      setElapsedMs(resp.elapsed_ms);
      setIsSearching(false);
      setRefsCollapsed(false);

      // ── Phase 2: Enrich in background ──────────────────
      const movieIds = resp.search_results.map(r => r.movieId).filter(Boolean);
      if (movieIds.length > 0) {
        setIsEnriching(true);
        const ac = new AbortController();
        enrichAbort.current = ac;

        api.enrich({
          query: q,
          session_id: sessionRef.current,
          movie_ids: movieIds,
        }).then(enrichResp => {
          if (ac.signal.aborted) return;
          setReformulations(enrichResp.reformulations || []);
          setExplanations(enrichResp.explanations || {});
          setSuggestedFilters(enrichResp.filter_suggestions || {});
        }).catch(() => {}).finally(() => {
          if (!ac.signal.aborted) setIsEnriching(false);
        });
      }
    } catch (e) {
      console.error('Search error:', e);
      setIsSearching(false);
    }
  }, [genreWeights, decadeHint, activeGenres, activeDecades]);

  // ── Search triggers ────────────────────────────────────────
  const handleSubmit = useCallback(() => doSearch(queryRef.current), [doSearch]);

  const handleReformulationClick = useCallback((ref: Reformulation) => {
    setQuery(ref.query);
    queryRef.current = ref.query;
    doSearch(ref.query);
  }, [doSearch]);

  // ── Filter changes trigger search ──────────────────────────
  const filterTimer = useRef<ReturnType<typeof setTimeout>>();
  useEffect(() => {
    if (!queryRef.current.trim()) return;
    clearTimeout(filterTimer.current);
    filterTimer.current = setTimeout(() => doSearch(queryRef.current), 400);
    return () => clearTimeout(filterTimer.current);
  }, [activeGenres, activeDecades, genreWeights, decadeHint, doSearch]);

  // ── Feedback ───────────────────────────────────────────────
  const handleFeedback = useCallback(async (
    movie: SearchResult, action: 'like' | 'dislike' | 'clear',
  ) => {
    if (!sessionRef.current) return;
    try {
      const resp = await api.sendFeedback({
        session_id: sessionRef.current,
        movie_id: movie.movieId,
        movie_title: movie.title,
        action,
      });
      setLiked(resp.liked);
      setDisliked(resp.disliked);
    } catch (e) {
      console.error('Feedback error:', e);
    }
  }, []);

  // ── Detail panel ───────────────────────────────────────────
  const handleMovieClick = useCallback(async (movie: SearchResult) => {
    setSelectedMovie(movie);
    setDetailLoading(true);
    setMovieDetail(null);
    setNeighborhood(null);
    try {
      const [det, nbr] = await Promise.all([
        api.getMovieDetail(movie.movieId),
        api.getNeighborhood(movie.movieId),
      ]);
      setMovieDetail(det);
      setNeighborhood(nbr);
    } catch (e) {
      console.error('Detail error:', e);
    } finally {
      setDetailLoading(false);
    }
  }, []);

  // ── Mixer ──────────────────────────────────────────────────
  const addToMixer = useCallback((movie: SearchResult) => {
    setMixerMovies(prev => prev.find(m => m.movieId === movie.movieId) ? prev : [...prev, movie]);
  }, []);

  // ── Filter toggles ────────────────────────────────────────
  const toggleGenre = useCallback((g: string) => {
    setActiveGenres(prev => prev.includes(g) ? prev.filter(x => x !== g) : [...prev, g]);
  }, []);
  const toggleDecade = useCallback((d: string) => {
    setActiveDecades(prev => prev.includes(d) ? prev.filter(x => x !== d) : [d]);
  }, []);

  const likedIds = new Set(liked.map(l => l.id));
  const dislikedIds = new Set(disliked.map(d => d.id));

  // ── Welcome screen ─────────────────────────────────────────
  if (!userName || !sessionId) {
    return (
      <WelcomeScreen onJoin={(name, sid) => {
        setUserName(name);
        setSessionId(sid);
      }} />
    );
  }

  // ── Main app ───────────────────────────────────────────────
  return (
    <div className="min-h-screen flex flex-col">
      <TopBar
        userName={userName}
        view={view}
        onViewChange={setView}
        preferenceIntensity={prefIntensity}
        onPreferenceIntensityChange={setPrefIntensity}
        likedCount={liked.length}
        dislikedCount={disliked.length}
        mixerCount={mixerMovies.length}
        onShowLikedPanel={() => setShowLikedPanel(true)}
      />

      <main className="flex-1 flex flex-col">
        <div className="px-6 pt-4 pb-2 max-w-[1400px] mx-auto w-full">
          <SearchBar
            value={query}
            onChange={setQuery}
            onSubmit={handleSubmit}
            isSearching={isSearching}
          />
          <ReformulationSuggestions
            reformulations={reformulations}
            onClick={handleReformulationClick}
            isVisible={reformulations.length > 0 || isEnriching}
            isCollapsed={refsCollapsed}
            onToggleCollapse={() => setRefsCollapsed(p => !p)}
            isLoading={isEnriching && reformulations.length === 0}
          />
        </div>

        {view === 'search' ? (
          <div className="flex-1 flex max-w-[1400px] mx-auto w-full px-4">
            <FilterSidebar
              availableGenres={availableGenres}
              availableDecades={availableDecades}
              activeGenres={activeGenres}
              activeDecades={activeDecades}
              suggestedFilters={suggestedFilters}
              genreWeights={genreWeights}
              decadeHint={decadeHint}
              onToggleGenre={toggleGenre}
              onToggleDecade={toggleDecade}
              onGenreWeightsChange={setGenreWeights}
              onDecadeHintChange={setDecadeHint}
              liked={liked}
              disliked={disliked}
              onShowLikedPanel={() => setShowLikedPanel(true)}
            />

            <div className="flex-1 min-w-0 pl-4 relative">
              <StatusBar
                mode={searchMode}
                count={results.length}
                isSearching={isSearching}
                topScore={topScore}
                elapsedMs={elapsedMs}
                isEnriching={isEnriching}
              />

              {/* Loading overlay on existing results */}
              {isSearching && results.length > 0 && (
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

              <MovieGrid
                movies={results}
                explanations={explanations}
                likedIds={likedIds}
                dislikedIds={dislikedIds}
                onMovieClick={handleMovieClick}
                onFeedback={handleFeedback}
                onAddToMixer={addToMixer}
                isLoading={isSearching && results.length === 0}
                isEnriching={isEnriching}
              />
            </div>
          </div>
        ) : (
          <div className="flex-1 max-w-[1400px] mx-auto w-full px-6">
            <MovieMixer
              movies={mixerMovies}
              onRemove={(id) => setMixerMovies(prev => prev.filter(m => m.movieId !== id))}
              sessionId={sessionId}
            />
          </div>
        )}
      </main>

      {selectedMovie && (
        <MovieDetailPanel
          movie={selectedMovie}
          detail={movieDetail}
          neighborhood={neighborhood}
          explanation={explanations[String(selectedMovie.movieId)]}
          isLoading={detailLoading}
          isLiked={likedIds.has(selectedMovie.movieId)}
          isDisliked={dislikedIds.has(selectedMovie.movieId)}
          onClose={() => { setSelectedMovie(null); setMovieDetail(null); setNeighborhood(null); }}
          onFeedback={handleFeedback}
          onAddToMixer={addToMixer}
        />
      )}

      {showLikedPanel && (
        <LikedDislikedPanel
          liked={liked}
          disliked={disliked}
          onClose={() => setShowLikedPanel(false)}
          onRemoveLike={(id, title) => handleFeedback({ movieId: id, title } as SearchResult, 'clear')}
          onRemoveDislike={(id, title) => handleFeedback({ movieId: id, title } as SearchResult, 'clear')}
        />
      )}
    </div>
  );
}
