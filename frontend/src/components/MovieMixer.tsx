'use client';

import { useState, useCallback } from 'react';
import type { Movie } from '@/types';
import * as api from '@/lib/api';

interface Props {
  movies: Movie[];
  onRemove: (movieId: number) => void;
  sessionId: string;
}

export default function MovieMixer({ movies, onRemove, sessionId }: Props) {
  const [connectors, setConnectors] = useState<Movie[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [embeddingSpace, setEmbeddingSpace] = useState<'graph' | 'semantic'>('graph');

  const handleMix = useCallback(async () => {
    if (movies.length < 2) return;
    setIsLoading(true);
    try {
      const resp = await api.getMixerResults(
        movies.map(m => m.movieId),
        embeddingSpace,
      );
      setConnectors(resp.connectors);
    } catch (e) {
      console.error('Mixer error:', e);
    } finally {
      setIsLoading(false);
    }
  }, [movies, embeddingSpace]);

  return (
    <div className="py-6 space-y-6">
      <div>
        <h2 className="text-xl font-bold text-text-primary mb-1">Movie Mixer</h2>
        <p className="text-sm text-text-secondary">
          Select 2 or more movies and find films that sit at the intersection of all of them.
        </p>
      </div>

      {/* Selected movies */}
      <div className="space-y-3">
        <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider">
          Selected movies ({movies.length})
        </h3>

        {movies.length === 0 ? (
          <div className="bg-surface-1 border border-dashed border-white/[0.08] rounded-xl p-8 text-center">
            <p className="text-sm text-text-dim">
              Go to Search and click the + button on movie cards to add them here
            </p>
          </div>
        ) : (
          <div className="flex flex-wrap gap-3">
            {movies.map(m => (
              <div
                key={m.movieId}
                className="group flex items-center gap-3 bg-surface-1 border border-white/[0.06] rounded-lg p-2 pr-3"
              >
                {m.poster_url ? (
                  <img
                    src={m.poster_url}
                    alt={m.title}
                    className="w-10 h-14 object-cover rounded"
                  />
                ) : (
                  <div className="w-10 h-14 bg-surface-2 rounded flex items-center justify-center text-text-dim text-[10px]">
                    ?
                  </div>
                )}
                <div className="min-w-0">
                  <p className="text-sm font-medium text-text-primary truncate max-w-[140px]">
                    {m.title}
                  </p>
                  {m.year && (
                    <p className="text-[11px] text-text-dim">{m.year}</p>
                  )}
                </div>
                <button
                  onClick={() => onRemove(m.movieId)}
                  className="ml-2 w-6 h-6 rounded-full bg-surface-2 text-text-dim hover:text-ember hover:bg-ember/10 flex items-center justify-center transition-colors"
                >
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Controls */}
      {movies.length >= 2 && (
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <span className="text-xs text-text-dim">Mode:</span>
            <button
              onClick={() => setEmbeddingSpace('graph')}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                embeddingSpace === 'graph'
                  ? 'bg-accent/15 text-accent border border-accent/30'
                  : 'bg-surface-2 border border-white/[0.06] text-text-secondary hover:text-text-primary'
              }`}
            >
              Structural
            </button>
            <button
              onClick={() => setEmbeddingSpace('semantic')}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                embeddingSpace === 'semantic'
                  ? 'bg-accent/15 text-accent border border-accent/30'
                  : 'bg-surface-2 border border-white/[0.06] text-text-secondary hover:text-text-primary'
              }`}
            >
              Thematic
            </button>
          </div>

          <button
            onClick={handleMix}
            disabled={isLoading}
            className="px-6 py-2.5 bg-accent hover:bg-accent-light disabled:bg-surface-3 disabled:text-text-dim text-white text-sm font-semibold rounded-lg transition-all flex items-center gap-2"
          >
            {isLoading ? (
              <>
                <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Mixing...
              </>
            ) : (
              <>
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
                </svg>
                Find connectors
              </>
            )}
          </button>
        </div>
      )}

      {/* Explanation */}
      {movies.length >= 2 && (
        <div className="bg-surface-1/50 border border-white/[0.04] rounded-lg p-3">
          <p className="text-[11px] text-text-dim leading-relaxed">
            <strong className="text-text-secondary">Structural</strong> finds movies that share similar cast, genres, and directors with your picks.{' '}
            <strong className="text-text-secondary">Thematic</strong> finds movies with similar plot themes and mood.
            The mixer computes a centroid of all selected movies in embedding space and finds the nearest neighbors.
          </p>
        </div>
      )}

      {/* Results */}
      {connectors.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider">
            Connector movies
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {connectors.map((c, i) => (
              <div
                key={c.movieId}
                className="bg-surface-1 border border-white/[0.06] rounded-xl overflow-hidden animate-fade-in"
                style={{ animationDelay: `${i * 60}ms`, opacity: 0 }}
              >
                <div className="flex gap-3 p-3">
                  {c.poster_url ? (
                    <img
                      src={c.poster_url}
                      alt={c.title}
                      className="w-16 h-24 object-cover rounded-md flex-shrink-0"
                    />
                  ) : (
                    <div className="w-16 h-24 bg-surface-2 rounded-md flex-shrink-0 flex items-center justify-center text-text-dim text-xs">
                      ?
                    </div>
                  )}
                  <div className="min-w-0 flex-1">
                    <h4 className="text-sm font-semibold text-text-primary leading-tight">
                      {c.title}
                    </h4>
                    <div className="flex items-center gap-2 mt-0.5">
                      {c.year && <span className="text-[11px] text-text-dim">{c.year}</span>}
                      {c.score && (
                        <span className="text-[10px] font-mono text-accent/70">
                          {(c.score * 100).toFixed(0)}% match
                        </span>
                      )}
                    </div>
                    {c.genres && c.genres.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-1.5">
                        {c.genres.slice(0, 3).map(g => (
                          <span key={g} className="tag text-[10px]">{g}</span>
                        ))}
                      </div>
                    )}
                    {c.overview && (
                      <p className="text-[11px] text-text-dim mt-1.5 line-clamp-2">
                        {c.overview}
                      </p>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
