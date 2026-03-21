'use client';

import { useState, useMemo } from 'react';
import type { FeedbackItem } from '@/lib/api';

interface Props {
  availableGenres: string[];
  availableDecades: string[];
  activeGenres: string[];          // hard genre filters
  activeDecades: string[];         // hard decade filters
  suggestedFilters: { genres?: string[]; decades?: string[] };
  genreWeights: Record<string, number>;  // genre soft steering (mutually exclusive with activeGenres)
  decadeHints: string[];           // era soft steering (mutually exclusive with activeDecades)
  onToggleGenre: (g: string) => void;    // toggles hard genre → auto-clears genreWeights
  onToggleDecade: (d: string) => void;   // toggles hard decade → auto-clears decadeHints
  onGenreWeightsChange: (w: Record<string, number>) => void;  // sets soft genre → auto-clears activeGenres
  onDecadeHintsChange: (d: string[]) => void;                 // sets soft decade → auto-clears activeDecades
  onClearAll: () => void;
  liked: FeedbackItem[];
  disliked: FeedbackItem[];
  onShowLikedPanel: () => void;
  sortBy: string;
  sortDir: string;
  onSortChange: (by: string, dir: string) => void;
  imdbMin: number | null;
  imdbMax: number | null;
  onImdbRangeChange: (min: number | null, max: number | null) => void;
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
  imdbMin, imdbMax, onImdbRangeChange,
}: Props) {
  const [showAllGenres, setShowAllGenres] = useState(false);
  const [mixGenreA, setMixGenreA] = useState('');
  const [mixGenreB, setMixGenreB] = useState('');
  const [mixRatio, setMixRatio] = useState(0.5);

  // Within-dimension mutual exclusion state
  const isGenreSoftActive = Object.keys(genreWeights).length > 0;
  const isDecadeSoftActive = decadeHints.length > 0;
  const isGenreHardActive = activeGenres.length > 0;
  const isDecadeHardActive = activeDecades.length > 0;
  const hasAnyFilter = isGenreSoftActive || isDecadeSoftActive || isGenreHardActive || isDecadeHardActive
    || imdbMin !== null || imdbMax !== null || sortBy !== 'relevance';

  const sortedGenres = useMemo(() => {
    const suggested = new Set(suggestedFilters.genres || []);
    return [...availableGenres.filter(g => suggested.has(g)), ...availableGenres.filter(g => !suggested.has(g))];
  }, [availableGenres, suggestedFilters.genres]);

  const displayGenres = showAllGenres ? sortedGenres : sortedGenres.slice(0, 12);

  const handleMixChange = (ratio: number) => {
    setMixRatio(ratio);
    if (mixGenreA && mixGenreB) onGenreWeightsChange({ [mixGenreA]: 1 - ratio, [mixGenreB]: ratio });
  };

  const clearSteering = () => {
    setMixGenreA(''); setMixGenreB(''); setMixRatio(0.5); onGenreWeightsChange({});
  };

  const toggleDecadeHint = (d: string) => {
    onDecadeHintsChange(decadeHints.includes(d) ? decadeHints.filter(x => x !== d) : [...decadeHints, d]);
  };

  return (
    <aside className="w-56 flex-shrink-0 pr-2 py-3 space-y-5 overflow-y-auto max-h-[calc(100vh-180px)] sticky top-[72px]">

      {/* Clear All */}
      {hasAnyFilter && (
        <button onClick={onClearAll}
          className="w-full text-[11px] text-red-400/80 hover:text-red-400 border border-red-400/20 hover:border-red-400/40 rounded-lg py-1.5 transition-colors">
          Clear all filters
        </button>
      )}

      {/* Sort */}
      <div className="space-y-2">
        <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Sort by</h3>
        <div className="flex gap-1 flex-wrap">
          {SORT_OPTIONS.map(opt => (
            <button key={opt.value} onClick={() => {
              onSortChange(opt.value, sortBy === opt.value && sortDir === 'desc' ? 'asc' : 'desc');
            }}
              className={`tag text-[10px] flex items-center gap-0.5 ${sortBy === opt.value ? 'tag-active' : ''}`}>
              {opt.label}
              {sortBy === opt.value && <span className="text-[9px] opacity-70">{sortDir === 'desc' ? '↓' : '↑'}</span>}
            </button>
          ))}
        </div>
      </div>

      {/* IMDB Range */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">IMDB Rating</h3>
          {(imdbMin !== null || imdbMax !== null) && (
            <button onClick={() => onImdbRangeChange(null, null)} className="text-[10px] text-red-400/70 hover:text-red-400 transition-colors">Clear</button>
          )}
        </div>
        <div className="flex items-center gap-2">
          <input type="number" min={0} max={10} step={0.5} value={imdbMin ?? ''} placeholder="Min"
            onChange={e => onImdbRangeChange(e.target.value ? parseFloat(e.target.value) : null, imdbMax)}
            className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40 placeholder:text-text-dim/40" />
          <span className="text-text-dim text-xs">—</span>
          <input type="number" min={0} max={10} step={0.5} value={imdbMax ?? ''} placeholder="Max"
            onChange={e => onImdbRangeChange(imdbMin, e.target.value ? parseFloat(e.target.value) : null)}
            className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40 placeholder:text-text-dim/40" />
        </div>
      </div>

      {/* ═══ GENRE: hard filter ═══ */}
      {/* Disabled when genre soft steering is active */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
            Genres
            {isGenreSoftActive && <span className="text-amber-400/60 text-[9px] font-normal normal-case">(using mixer)</span>}
          </h3>
          {isGenreHardActive && (
            <button onClick={() => { activeGenres.forEach(g => onToggleGenre(g)); }}
              className="text-[10px] text-red-400/70 hover:text-red-400 transition-colors">Clear</button>
          )}
        </div>
        <div className={`flex flex-wrap gap-1 ${isGenreSoftActive ? 'opacity-40 pointer-events-none' : ''}`}>
          {displayGenres.map(g => {
            const isActive = activeGenres.includes(g);
            const isSugg = suggestedFilters.genres?.includes(g);
            return (
              <button key={g} onClick={() => onToggleGenre(g)}
                className={`tag text-[11px] ${isActive ? 'tag-active' : isSugg ? 'border-accent/30 text-accent/70' : ''}`}>
                {g}
              </button>
            );
          })}
        </div>
        {sortedGenres.length > 12 && (
          <button onClick={() => setShowAllGenres(!showAllGenres)}
            className="text-[11px] text-accent hover:text-accent-light transition-colors">
            {showAllGenres ? 'Show less' : `Show all ${sortedGenres.length}`}
          </button>
        )}
      </div>

      {/* ═══ DECADE: hard filter ═══ */}
      {/* Disabled when decade soft steering is active */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
            Decades
            {isDecadeSoftActive && <span className="text-amber-400/60 text-[9px] font-normal normal-case">(using era feel)</span>}
          </h3>
          {isDecadeHardActive && (
            <button onClick={() => { activeDecades.forEach(d => onToggleDecade(d)); }}
              className="text-[10px] text-red-400/70 hover:text-red-400 transition-colors">Clear</button>
          )}
        </div>
        <div className={`flex flex-wrap gap-1 ${isDecadeSoftActive ? 'opacity-40 pointer-events-none' : ''}`}>
          {availableDecades.map(d => (
            <button key={d} onClick={() => onToggleDecade(d)}
              className={`tag text-[11px] ${activeDecades.includes(d) ? 'tag-active' : ''}`}>{d}</button>
          ))}
        </div>
      </div>

      {/* ═══ GENRE: soft steering (mixer) ═══ */}
      {/* Disabled when genre hard filters are active */}
      <div className="space-y-2 pt-2 border-t border-white/[0.04]">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
            Genre mixer
            {isGenreHardActive && <span className="text-amber-400/60 text-[9px] font-normal normal-case">(using hard filter)</span>}
          </h3>
          {isGenreSoftActive && (
            <button onClick={clearSteering} className="text-[10px] text-red-400/70 hover:text-red-400 transition-colors">Clear</button>
          )}
        </div>
        <p className="text-[10px] text-text-dim">Blend genres to steer results in graph space</p>
        <div className={`space-y-2 ${isGenreHardActive ? 'opacity-40 pointer-events-none' : ''}`}>
          <select value={mixGenreA}
            onChange={e => { setMixGenreA(e.target.value); if (e.target.value && mixGenreB) onGenreWeightsChange({ [e.target.value]: 1 - mixRatio, [mixGenreB]: mixRatio }); }}
            className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40">
            <option value="">Genre A</option>
            {availableGenres.map(g => <option key={g} value={g}>{g}</option>)}
          </select>
          <select value={mixGenreB}
            onChange={e => { setMixGenreB(e.target.value); if (mixGenreA && e.target.value) onGenreWeightsChange({ [mixGenreA]: 1 - mixRatio, [e.target.value]: mixRatio }); }}
            className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40">
            <option value="">Genre B</option>
            {availableGenres.map(g => <option key={g} value={g}>{g}</option>)}
          </select>
          {mixGenreA && mixGenreB && (
            <div>
              <input type="range" min={0} max={1} step={0.05} value={mixRatio}
                onChange={e => handleMixChange(parseFloat(e.target.value))} className="w-full" />
              <div className="flex justify-between text-[10px] text-text-dim">
                <span>{mixGenreA} {Math.round((1 - mixRatio) * 100)}%</span>
                <span>{Math.round(mixRatio * 100)}% {mixGenreB}</span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ═══ DECADE: soft steering (era feel) ═══ */}
      {/* Disabled when decade hard filters are active */}
      <div className="space-y-2 pt-2 border-t border-white/[0.04]">
        <div className="flex items-center justify-between">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
            Era feel
            {isDecadeHardActive && <span className="text-amber-400/60 text-[9px] font-normal normal-case">(using hard filter)</span>}
          </h3>
          {isDecadeSoftActive && (
            <button onClick={() => onDecadeHintsChange([])} className="text-[10px] text-red-400/70 hover:text-red-400 transition-colors">Clear</button>
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
    </aside>
  );
}