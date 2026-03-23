'use client';

import { useState, useMemo, useCallback, useRef } from 'react';
import type { FeedbackItem } from '@/lib/api';

interface Props {
  availableGenres: string[];
  availableDecades: string[];
  activeGenres: string[];
  activeDecades: string[];
  suggestedFilters: { genres?: string[]; decades?: string[] };
  genreWeights: Record<string, number>;
  decadeHints: string[];
  onToggleGenre: (g: string) => void;
  onToggleDecade: (d: string) => void;
  onGenreWeightsChange: (w: Record<string, number>) => void;
  onDecadeHintsChange: (d: string[]) => void;
  onClearAll: () => void;
  liked: FeedbackItem[];
  disliked: FeedbackItem[];
  onShowLikedPanel: () => void;
  sortBy: string;
  sortDir: string;
  onSortChange: (by: string, dir: string) => void;
  imdbMin: number | null;
  onImdbMinChange: (min: number | null) => void;
}

const SORT_OPTIONS = [
  { value: 'relevance', label: 'Relevance' },
  { value: 'imdb_rating', label: 'IMDB' },
  { value: 'year', label: 'Year' },
  { value: 'title', label: 'Title' },
];

export default function FilterSidebar({
  availableGenres, availableDecades, activeGenres, activeDecades,
  suggestedFilters, genreWeights, decadeHints,
  onToggleGenre, onToggleDecade, onGenreWeightsChange, onDecadeHintsChange,
  onClearAll,
  liked, disliked, onShowLikedPanel,
  sortBy, sortDir, onSortChange,
  imdbMin, onImdbMinChange,
}: Props) {
  const [showAllGenres, setShowAllGenres] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);

  const isGenreSoftActive = Object.keys(genreWeights).length > 0;
  const isDecadeSoftActive = decadeHints.length > 0;
  const isGenreHardActive = activeGenres.length > 0;
  const isDecadeHardActive = activeDecades.length > 0;
  const hasAnyFilter = isGenreSoftActive || isDecadeSoftActive || isGenreHardActive || isDecadeHardActive
    || imdbMin !== null || sortBy !== 'relevance';

  const sortedGenres = useMemo(() => {
    const suggested = new Set(suggestedFilters.genres || []);
    return [...availableGenres.filter(g => suggested.has(g)), ...availableGenres.filter(g => !suggested.has(g))];
  }, [availableGenres, suggestedFilters.genres]);

  const displayGenres = showAllGenres ? sortedGenres : sortedGenres.slice(0, 12);

  // ── Multi-genre steering (1-5 genres with normalized weights) ──

  const steeringGenres = Object.keys(genreWeights);
  const availableForSteering = availableGenres.filter(g => !genreWeights[g]);

  const addSteeringGenre = useCallback((genre: string) => {
    if (!genre || steeringGenres.length >= 5) return;
    const next = { ...genreWeights, [genre]: 1 };
    // Normalize
    const total = Object.values(next).reduce((a, b) => a + b, 0);
    for (const k of Object.keys(next)) next[k] = next[k] / total;
    onGenreWeightsChange(next);
  }, [genreWeights, steeringGenres.length, onGenreWeightsChange]);

  const removeSteeringGenre = useCallback((genre: string) => {
    const next = { ...genreWeights };
    delete next[genre];
    if (Object.keys(next).length === 0) { onGenreWeightsChange({}); return; }
    const total = Object.values(next).reduce((a, b) => a + b, 0);
    for (const k of Object.keys(next)) next[k] = next[k] / total;
    onGenreWeightsChange(next);
  }, [genreWeights, onGenreWeightsChange]);

  const setSteeringWeight = useCallback((genre: string, value: number) => {
    const next = { ...genreWeights, [genre]: Math.max(0.05, value) };
    const total = Object.values(next).reduce((a, b) => a + b, 0);
    for (const k of Object.keys(next)) next[k] = next[k] / total;
    onGenreWeightsChange(next);
  }, [genreWeights, onGenreWeightsChange]);

  const toggleDecadeHint = (d: string) => {
    onDecadeHintsChange(decadeHints.includes(d) ? decadeHints.filter(x => x !== d) : [...decadeHints, d]);
  };

  const [sidebarWidth, setSidebarWidth] = useState(224); // 14rem = 224px
  const dragging = useRef(false);
  const startX = useRef(0);
  const startW = useRef(224);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    dragging.current = true; startX.current = e.clientX; startW.current = sidebarWidth;
    const onMove = (ev: MouseEvent) => { if (dragging.current) setSidebarWidth(Math.max(180, Math.min(400, startW.current + ev.clientX - startX.current))); };
    const onUp = () => { dragging.current = false; window.removeEventListener('mousemove', onMove); window.removeEventListener('mouseup', onUp); };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  }, [sidebarWidth]);

  return (
    <aside className="flex-shrink-0 pr-2 py-3 space-y-5 overflow-y-auto max-h-[calc(100vh-180px)] sticky top-[72px] relative"
      style={{ width: sidebarWidth }}>

      {hasAnyFilter && (
        <button onClick={onClearAll}
          className="w-full text-[11px] text-red-400/80 hover:text-red-400 border border-red-400/20 hover:border-red-400/40 rounded-lg py-1.5 transition-colors">
          Clear all filters
        </button>
      )}

      {/* ═══ FILTERS SECTION ═══ */}
      <div className="space-y-2">
        <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Sort by</h3>
        <div className="flex gap-1 flex-wrap">
          {SORT_OPTIONS.map(opt => (
            <button key={opt.value} onClick={() => onSortChange(opt.value, sortBy === opt.value && sortDir === 'desc' ? 'asc' : 'desc')}
              className={`tag text-[10px] flex items-center gap-0.5 ${sortBy === opt.value ? 'tag-active' : ''}`}>
              {opt.label}
              {sortBy === opt.value && <span className="text-[9px] opacity-70">{sortDir === 'desc' ? '↓' : '↑'}</span>}
            </button>
          ))}
        </div>
      </div>

      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Min IMDB</h3>
          {imdbMin !== null && imdbMin !== 6 && (
            <button onClick={() => onImdbMinChange(6)} className="text-[10px] text-red-400/70 hover:text-red-400">Reset</button>
          )}
        </div>
        <div className="flex items-center gap-2">
          <input type="range" min={0} max={9} step={0.5} value={imdbMin ?? 6}
            onChange={e => onImdbMinChange(parseFloat(e.target.value))}
            className="flex-1" />
          <span className="text-sm text-text-primary font-mono w-8 text-right">{(imdbMin ?? 6).toFixed(1)}</span>
        </div>
      </div>

      {/* Genres */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
            Genres
            {isGenreSoftActive && <span className="text-amber-400/60 text-[9px] font-normal normal-case">(using steering)</span>}
          </h3>
          {isGenreHardActive && (
            <button onClick={() => activeGenres.forEach(g => onToggleGenre(g))} className="text-[10px] text-red-400/70 hover:text-red-400">Clear</button>
          )}
        </div>
        <div className={`flex flex-wrap gap-1 ${isGenreSoftActive ? 'opacity-40 pointer-events-none' : ''}`}>
          {displayGenres.map(g => (
            <button key={g} onClick={() => onToggleGenre(g)}
              className={`tag text-[11px] ${activeGenres.includes(g) ? 'tag-active' : suggestedFilters.genres?.includes(g) ? 'border-accent/30 text-accent/70' : ''}`}>
              {g}
            </button>
          ))}
        </div>
        {sortedGenres.length > 12 && (
          <button onClick={() => setShowAllGenres(!showAllGenres)}
            className="text-[11px] text-accent hover:text-accent-light">
            {showAllGenres ? 'Show less' : `Show all ${sortedGenres.length}`}
          </button>
        )}
      </div>

      {/* Decades */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
            Decades
            {isDecadeSoftActive && <span className="text-amber-400/60 text-[9px] font-normal normal-case">(using era feel)</span>}
          </h3>
          {isDecadeHardActive && (
            <button onClick={() => activeDecades.forEach(d => onToggleDecade(d))} className="text-[10px] text-red-400/70 hover:text-red-400">Clear</button>
          )}
        </div>
        <div className={`flex flex-wrap gap-1 ${isDecadeSoftActive ? 'opacity-40 pointer-events-none' : ''}`}>
          {availableDecades.map(d => (
            <button key={d} onClick={() => onToggleDecade(d)}
              className={`tag text-[11px] ${activeDecades.includes(d) ? 'tag-active' : ''}`}>{d}</button>
          ))}
        </div>
      </div>

      {/* ═══ ADVANCED FILTERS ═══ */}
      <div className="pt-3 border-t border-white/[0.06]">
        <button onClick={() => setShowAdvanced(!showAdvanced)}
          className="w-full flex items-center justify-between text-[11px] font-semibold text-text-dim uppercase tracking-wider hover:text-text-secondary transition-colors">
          <span>Advanced steering</span>
          <svg className={`w-3 h-3 transition-transform ${showAdvanced ? 'rotate-180' : ''}`}
            fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        </button>
      </div>

      {showAdvanced && (
        <>
          {/* ═══ GENRE STEERING (multi-genre, 1-5) ═══ */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Genre steering</h3>
              {isGenreSoftActive && (
                <button onClick={() => onGenreWeightsChange({})} className="text-[10px] text-red-400/70 hover:text-red-400">Clear</button>
              )}
            </div>
            <p className="text-[10px] text-text-dim">Blend 1-5 genres to steer results in graph space</p>

            <div className={isGenreHardActive ? 'opacity-40 pointer-events-none' : ''}>
              {/* Current steering genres with weight sliders */}
              {steeringGenres.map(g => (
                <div key={g} className="mb-2.5">
                  <div className="flex items-center gap-1.5 mb-1">
                    <button onClick={() => removeSteeringGenre(g)}
                      className="w-4 h-4 rounded-full bg-surface-2 hover:bg-red-500/30 text-text-dim hover:text-red-400 flex items-center justify-center text-[8px] flex-shrink-0">
                      ✕
                    </button>
                    <span className="text-[11px] text-text-secondary flex-1">{g}</span>
                    <span className="text-[10px] text-accent font-mono flex-shrink-0">
                      {Math.round((genreWeights[g] || 0) * 100)}%
                    </span>
                  </div>
                  <input type="range" min={0.05} max={1} step={0.05}
                    value={genreWeights[g] || 0}
                    onChange={e => setSteeringWeight(g, parseFloat(e.target.value))}
                    className="w-full h-1 ml-5" style={{ width: 'calc(100% - 20px)' }} />
                </div>
              ))}

              {/* Add genre dropdown */}
              {steeringGenres.length < 5 && (
                <select value=""
                  onChange={e => { if (e.target.value) addSteeringGenre(e.target.value); }}
                  className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40">
                  <option value="">+ Add genre ({steeringGenres.length}/5)</option>
                  {availableForSteering.map(g => <option key={g} value={g}>{g}</option>)}
                </select>
              )}
            </div>
          </div>

          {/* ═══ ERA FEEL ═══ */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Era feel</h3>
              {isDecadeSoftActive && (
                <button onClick={() => onDecadeHintsChange([])} className="text-[10px] text-red-400/70 hover:text-red-400">Clear</button>
              )}
            </div>
            <p className="text-[10px] text-text-dim">Steer toward a decade&apos;s vibe in graph space</p>
            <div className={`flex flex-wrap gap-1 ${isDecadeHardActive ? 'opacity-40 pointer-events-none' : ''}`}>
              {availableDecades.map(d => (
                <button key={d} onClick={() => toggleDecadeHint(d)}
                  className={`tag text-[11px] ${decadeHints.includes(d) ? 'tag-active' : ''}`}>{d}</button>
              ))}
            </div>
          </div>
        </>
      )}

      {/* Liked/disliked */}
      {(liked.length > 0 || disliked.length > 0) && (
        <div className="space-y-2 pt-2 border-t border-white/[0.04]">
          <button onClick={onShowLikedPanel}
            className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1 hover:text-text-secondary transition-colors w-full text-left">
            {liked.length > 0 && <><span className="text-emerald-400">♥</span> {liked.length}</>}
            {disliked.length > 0 && <span className="ml-2"><span className="text-red-400">✕</span> {disliked.length}</span>}
            <svg className="w-3 h-3 ml-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
            </svg>
          </button>
        </div>
      )}
      {/* Resize handle */}
      <div onMouseDown={onMouseDown}
        className="absolute right-0 top-0 bottom-0 w-1.5 cursor-col-resize hover:bg-accent/30 active:bg-accent/50 transition-colors rounded-full" />
    </aside>
  );
}