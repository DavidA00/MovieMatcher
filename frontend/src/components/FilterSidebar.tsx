'use client';

import { useState, useMemo } from 'react';
import type { SuggestedFilters, FeedbackItem } from '@/types';

interface Props {
  availableGenres: string[];
  availableDecades: string[];
  activeGenres: string[];
  activeDecades: string[];
  suggestedFilters: SuggestedFilters;
  genreWeights: Record<string, number>;
  decadeHint: string | null;
  onToggleGenre: (g: string) => void;
  onToggleDecade: (d: string) => void;
  onGenreWeightsChange: (w: Record<string, number>) => void;
  onDecadeHintChange: (d: string | null) => void;
  liked: FeedbackItem[];
  disliked: FeedbackItem[];
  onShowLikedPanel: () => void;
}

export default function FilterSidebar({
  availableGenres, availableDecades, activeGenres, activeDecades,
  suggestedFilters, genreWeights, decadeHint,
  onToggleGenre, onToggleDecade, onGenreWeightsChange, onDecadeHintChange,
  liked, disliked, onShowLikedPanel,
}: Props) {
  const [showAllGenres, setShowAllGenres] = useState(false);
  const [mixGenreA, setMixGenreA] = useState('');
  const [mixGenreB, setMixGenreB] = useState('');
  const [mixRatio, setMixRatio] = useState(0.5);

  // Suggested genres bubble to top
  const sortedGenres = useMemo(() => {
    const suggested = new Set(suggestedFilters.genres || []);
    const top = availableGenres.filter(g => suggested.has(g));
    const rest = availableGenres.filter(g => !suggested.has(g));
    return [...top, ...rest];
  }, [availableGenres, suggestedFilters.genres]);

  const displayGenres = showAllGenres ? sortedGenres : sortedGenres.slice(0, 12);

  // Genre mix handler
  const handleMixChange = (ratio: number) => {
    setMixRatio(ratio);
    if (mixGenreA && mixGenreB) {
      onGenreWeightsChange({
        [mixGenreA]: 1 - ratio,
        [mixGenreB]: ratio,
      });
    }
  };

  return (
    <aside className="w-56 flex-shrink-0 pr-2 py-3 space-y-5 overflow-y-auto max-h-[calc(100vh-180px)] sticky top-[72px]">

      {/* Active filters summary */}
      {(activeGenres.length > 0 || activeDecades.length > 0) && (
        <div className="space-y-1.5">
          <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Active filters</h3>
          <div className="flex flex-wrap gap-1">
            {activeGenres.map(g => (
              <button key={g} onClick={() => onToggleGenre(g)}
                className="tag tag-active text-[11px] flex items-center gap-1">
                {g}
                <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            ))}
            {activeDecades.map(d => (
              <button key={d} onClick={() => onToggleDecade(d)}
                className="tag tag-active text-[11px] flex items-center gap-1">
                {d}
                <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Genre filters */}
      <div className="space-y-2">
        <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5">
          <span>Genres</span>
          {suggestedFilters.genres && suggestedFilters.genres.length > 0 && (
            <span className="text-accent text-[10px] font-normal normal-case">• suggested</span>
          )}
        </h3>
        <div className="flex flex-wrap gap-1">
          {displayGenres.map(g => {
            const isActive = activeGenres.includes(g);
            const isSuggested = suggestedFilters.genres?.includes(g);
            return (
              <button
                key={g}
                onClick={() => onToggleGenre(g)}
                className={`tag text-[11px] ${
                  isActive ? 'tag-active' :
                  isSuggested ? 'border-accent/30 text-accent/70' : ''
                }`}
              >
                {g}
              </button>
            );
          })}
        </div>
        {sortedGenres.length > 12 && (
          <button
            onClick={() => setShowAllGenres(!showAllGenres)}
            className="text-[11px] text-accent hover:text-accent-light transition-colors"
          >
            {showAllGenres ? 'Show less' : `Show all ${sortedGenres.length}`}
          </button>
        )}
      </div>

      {/* Decade filters */}
      <div className="space-y-2">
        <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Decades</h3>
        <div className="flex flex-wrap gap-1">
          {availableDecades.map(d => {
            const isActive = activeDecades.includes(d);
            const isSuggested = suggestedFilters.decades?.includes(d);
            return (
              <button
                key={d}
                onClick={() => onToggleDecade(d)}
                className={`tag text-[11px] ${
                  isActive ? 'tag-active' :
                  isSuggested ? 'border-amber-500/30 text-amber-400/70' : ''
                }`}
              >
                {d}
              </button>
            );
          })}
        </div>
      </div>

      {/* Genre Mixer slider */}
      <div className="space-y-2 pt-2 border-t border-white/[0.04]">
        <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Genre mixer</h3>
        <p className="text-[10px] text-text-dim">Blend two genres to steer results</p>

        <div className="space-y-2">
          <select
            value={mixGenreA}
            onChange={e => { setMixGenreA(e.target.value); handleMixChange(mixRatio); }}
            className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40"
          >
            <option value="">Genre A</option>
            {availableGenres.map(g => <option key={g} value={g}>{g}</option>)}
          </select>
          <select
            value={mixGenreB}
            onChange={e => { setMixGenreB(e.target.value); handleMixChange(mixRatio); }}
            className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40"
          >
            <option value="">Genre B</option>
            {availableGenres.map(g => <option key={g} value={g}>{g}</option>)}
          </select>

          {mixGenreA && mixGenreB && (
            <div>
              <input
                type="range"
                min={0} max={1} step={0.05}
                value={mixRatio}
                onChange={e => handleMixChange(parseFloat(e.target.value))}
                className="w-full"
              />
              <div className="flex justify-between text-[10px] text-text-dim">
                <span>{mixGenreA} {Math.round((1 - mixRatio) * 100)}%</span>
                <span>{Math.round(mixRatio * 100)}% {mixGenreB}</span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Era slider */}
      <div className="space-y-2 pt-2 border-t border-white/[0.04]">
        <h3 className="text-[11px] font-semibold text-text-dim uppercase tracking-wider">Era feel</h3>
        <p className="text-[10px] text-text-dim">Steer toward a decade&apos;s vibe</p>
        <select
          value={decadeHint || ''}
          onChange={e => onDecadeHintChange(e.target.value || null)}
          className="w-full bg-surface-2 border border-white/[0.06] rounded-md px-2 py-1.5 text-xs text-text-primary outline-none focus:border-accent/40"
        >
          <option value="">Any era</option>
          {availableDecades.map(d => <option key={d} value={d}>{d}</option>)}
        </select>
      </div>

      {/* Liked movies */}
      {liked.length > 0 && (
        <div className="space-y-2 pt-2 border-t border-white/[0.04]">
          <button
            onClick={onShowLikedPanel}
            className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1 hover:text-text-secondary transition-colors w-full text-left"
          >
            <span className="text-sage">♥</span> Liked ({liked.length})
            {disliked.length > 0 && (
              <span className="ml-2 text-ember">✕ {disliked.length}</span>
            )}
            <svg className="w-3 h-3 ml-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
            </svg>
          </button>
          <div className="space-y-0.5">
            {liked.slice(0, 5).map(m => (
              <p key={m.id} className="text-[11px] text-text-secondary truncate">{m.title}</p>
            ))}
            {liked.length > 5 && (
              <p className="text-[10px] text-text-dim">+{liked.length - 5} more</p>
            )}
          </div>
        </div>
      )}
      {disliked.length > 0 && liked.length === 0 && (
        <div className="space-y-2 pt-2 border-t border-white/[0.04]">
          <button
            onClick={onShowLikedPanel}
            className="text-[11px] font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1 hover:text-text-secondary transition-colors w-full text-left"
          >
            <span className="text-ember">✕</span> Disliked ({disliked.length})
            <svg className="w-3 h-3 ml-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
            </svg>
          </button>
        </div>
      )}
    </aside>
  );
}
