'use client';

import type { SearchResult } from '@/lib/api';
import MovieCard from './MovieCard';

interface Props {
  movies: SearchResult[];
  explanations: Record<string, string>;
  likedIds: Set<number>;
  dislikedIds: Set<number>;
  onMovieClick: (m: SearchResult) => void;
  onFeedback: (m: SearchResult, action: 'like' | 'dislike' | 'clear') => void;
  onAddToMixer: (m: SearchResult) => void;
  isLoading: boolean;
  isEnriching?: boolean;
}

export default function MovieGrid({
  movies, explanations, likedIds, dislikedIds,
  onMovieClick, onFeedback, onAddToMixer, isLoading, isEnriching,
}: Props) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 py-2">
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="bg-surface-1 border border-white/[0.04] rounded-xl overflow-hidden">
            <div className="aspect-[2/3] shimmer" />
            <div className="p-3 space-y-2">
              <div className="h-4 w-3/4 shimmer rounded" />
              <div className="h-3 w-1/2 shimmer rounded" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (movies.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-center">
        <div className="w-16 h-16 rounded-full bg-surface-2 flex items-center justify-center mb-4">
          <svg className="w-8 h-8 text-text-dim" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
          </svg>
        </div>
        <h3 className="text-lg font-medium text-text-secondary mb-1">Start exploring</h3>
        <p className="text-sm text-text-dim max-w-sm">Type a vibe, mood, or movie name and press Search</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 py-2 pb-8">
      {movies.map((movie, i) => (
        <MovieCard
          key={movie.movieId}
          movie={movie}
          explanation={explanations[String(movie.movieId)]}
          isLiked={likedIds.has(movie.movieId)}
          isDisliked={dislikedIds.has(movie.movieId)}
          onClick={() => onMovieClick(movie)}
          onLike={() => onFeedback(movie, likedIds.has(movie.movieId) ? 'clear' : 'like')}
          onDislike={() => onFeedback(movie, dislikedIds.has(movie.movieId) ? 'clear' : 'dislike')}
          onMix={() => onAddToMixer(movie)}
          index={i}
          isEnriching={isEnriching && !explanations[String(movie.movieId)]}
        />
      ))}
    </div>
  );
}
