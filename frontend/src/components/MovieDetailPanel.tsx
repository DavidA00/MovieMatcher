'use client';

import { useEffect, useRef } from 'react';
import type { Movie, NeighborhoodData } from '@/types';
import ForceGraph from './ForceGraph';

interface Props {
  movie: Movie;
  detail: Movie | null;
  neighborhood: NeighborhoodData | null;
  explanation?: string;
  groupPerspective?: string;
  isLoading: boolean;
  isLiked: boolean;
  isDisliked: boolean;
  onClose: () => void;
  onFeedback: (m: Movie, action: 'like' | 'dislike' | 'clear') => void;
}

export default function MovieDetailPanel({
  movie, detail, neighborhood, explanation, groupPerspective, isLoading,
  isLiked, isDisliked, onClose, onFeedback,
}: Props) {
  const panelRef = useRef<HTMLDivElement>(null);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const d = detail || movie;
  const posterUrl = d.poster_url || (d.poster_path ? `https://image.tmdb.org/t/p/w500${d.poster_path}` : null);
  const topDecade = d.decade_feel?.[0];

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 animate-fade-in"
        onClick={onClose}
      />

      {/* Panel */}
      <div
        ref={panelRef}
        className="fixed right-0 top-0 bottom-0 w-full max-w-[560px] bg-surface-1 border-l border-white/[0.06] z-50 overflow-y-auto animate-slide-in shadow-2xl"
      >
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 z-10 w-8 h-8 rounded-full bg-surface-2 border border-white/[0.08] flex items-center justify-center text-text-dim hover:text-text-primary hover:border-white/[0.16] transition-all"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>

        {/* Hero poster */}
        <div className="relative h-80 bg-surface-2 overflow-hidden">
          {posterUrl ? (
            <img
              src={posterUrl}
              alt={d.title}
              className="w-full h-full object-cover opacity-80"
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center text-text-dim text-lg">
              No poster
            </div>
          )}
          <div className="absolute inset-0 bg-gradient-to-t from-surface-1 via-surface-1/40 to-transparent" />

          {/* Title overlay */}
          <div className="absolute bottom-0 left-0 right-0 p-6">
            <h2 className="text-2xl font-bold text-white leading-tight mb-1">
              {d.title}
            </h2>
            <div className="flex items-center gap-3 text-sm text-white/70">
              {d.year && <span>{d.year}</span>}
              {(d as any).imdb_rating && (
                <span className="flex items-center gap-1.5">
                  <span className="text-yellow-400 font-bold">★ {(d as any).imdb_rating.toFixed(1)}</span>
                  {(d as any).imdb_votes && (
                    <span className="text-white/40 text-xs">({((d as any).imdb_votes / 1000).toFixed(0)}k votes)</span>
                  )}
                </span>
              )}
              {topDecade && (
                <span className="tag text-[10px] bg-amber-500/15 border-amber-500/20 text-amber-300">
                  Feels like {topDecade.decade}
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="p-6 space-y-6">
          {/* Action buttons */}
          <div className="flex gap-2">
            <button
              onClick={() => onFeedback(movie, isLiked ? 'clear' : 'like')}
              className={`flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg text-sm font-medium transition-all ${
                isLiked
                  ? 'bg-sage/20 text-sage border border-sage/30'
                  : 'bg-surface-2 border border-white/[0.06] text-text-secondary hover:border-sage/30 hover:text-sage'
              }`}
            >
              <svg className="w-4 h-4" fill={isLiked ? 'currentColor' : 'none'} stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
              </svg>
              {isLiked ? 'Liked' : 'Like'}
            </button>
            <button
              onClick={() => onFeedback(movie, isDisliked ? 'clear' : 'dislike')}
              className={`flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg text-sm font-medium transition-all ${
                isDisliked
                  ? 'bg-ember/20 text-ember border border-ember/30'
                  : 'bg-surface-2 border border-white/[0.06] text-text-secondary hover:border-ember/30 hover:text-ember'
              }`}
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
              {isDisliked ? 'Disliked' : 'Not for me'}
            </button>
          </div>

          {/* AI explanation */}
          {explanation && (
            <div className="bg-accent/[0.06] border border-accent/10 rounded-lg p-4">
              <h3 className="text-xs font-semibold text-accent uppercase tracking-wider mb-2 flex items-center gap-1.5">
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
                </svg>
                AI reasoning
              </h3>
              <p className="text-sm text-text-primary leading-relaxed">{explanation}</p>
            </div>
          )}

          {/* Group perspective (round 2+) */}
          {groupPerspective && (
            <div className="bg-purple-500/[0.06] border border-purple-500/10 rounded-lg p-4">
              <h3 className="text-xs font-semibold text-purple-400 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z" />
                </svg>
                Group perspective
              </h3>
              <p className="text-sm text-text-primary leading-relaxed">{groupPerspective}</p>
            </div>
          )}

          {/* Overview */}
          {d.overview && (
            <div>
              <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider mb-2">Overview</h3>
              <p className="text-sm text-text-secondary leading-relaxed">{d.overview}</p>
            </div>
          )}

          {/* Metadata grid */}
          {isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3].map(i => (
                <div key={i} className="h-8 shimmer rounded" />
              ))}
            </div>
          ) : (
            <div className="space-y-3">
              {d.genres && d.genres.length > 0 && (
                <MetaRow label="Genres" values={d.genres} color="accent" />
              )}
              {d.directors && d.directors.length > 0 && (
                <MetaRow label="Director" values={d.directors} />
              )}
              {d.actors && d.actors.length > 0 && (
                <MetaRow label="Cast" values={d.actors.slice(0, 8)} />
              )}
              {d.keywords && d.keywords.length > 0 && (
                <MetaRow label="Keywords" values={d.keywords.slice(0, 10)} color="dim" />
              )}
              {d.languages && d.languages.length > 0 && (
                <MetaRow label="Language" values={d.languages} />
              )}
            </div>
          )}

          {/* Decade feel chart */}
          {d.decade_feel && d.decade_feel.length > 0 && (
            <div>
              <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider mb-3">
                Decade feel
              </h3>
              <div className="space-y-1.5">
                {d.decade_feel.slice(0, 5).map((df, i) => {
                  const maxScore = d.decade_feel![0].score;
                  const pct = maxScore > 0 ? (df.score / maxScore) * 100 : 0;
                  return (
                    <div key={df.decade} className="flex items-center gap-2">
                      <span className="text-[11px] text-text-dim w-12 text-right font-mono">
                        {df.decade}
                      </span>
                      <div className="flex-1 h-4 bg-surface-2 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all duration-500 ${
                            i === 0 ? 'bg-amber-400/60' : 'bg-surface-4'
                          }`}
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className="text-[10px] text-text-dim font-mono w-10">
                        {df.score.toFixed(2)}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Knowledge Graph neighborhood */}
          {neighborhood && neighborhood.nodes.length > 1 && (
            <div>
              <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider mb-3">
                Knowledge graph neighborhood
              </h3>
              <div className="bg-surface-0 rounded-lg border border-white/[0.04] overflow-hidden" style={{ height: 360 }}>
                <ForceGraph data={neighborhood} />
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function MetaRow({ label, values, color }: {
  label: string;
  values: string[];
  color?: 'accent' | 'dim';
}) {
  return (
    <div>
      <span className="text-[10px] text-text-dim uppercase tracking-wider">{label}</span>
      <div className="flex flex-wrap gap-1 mt-1">
        {values.map(v => (
          <span
            key={v}
            className={`tag text-[11px] ${
              color === 'accent'
                ? 'bg-accent/10 border-accent/20 text-accent/80'
                : color === 'dim'
                ? 'bg-surface-2/80 border-white/[0.03] text-text-dim'
                : ''
            }`}
          >
            {v}
          </span>
        ))}
      </div>
    </div>
  );
}