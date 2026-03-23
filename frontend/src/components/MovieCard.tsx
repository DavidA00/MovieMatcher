'use client';

import { useState } from 'react';
import type { SearchResult } from '@/lib/api';

interface Props {
  movie: SearchResult;
  explanation?: string;
  groupPerspective?: string;
  isLiked: boolean;
  isDisliked: boolean;
  onClick: () => void;
  onLike: () => void;
  onDislike: () => void;
  index: number;
  isEnriching?: boolean;
}

export default function MovieCard({
  movie, explanation, groupPerspective, isLiked, isDisliked,
  onClick, onLike, onDislike, index, isEnriching,
}: Props) {
  const [imgErr, setImgErr] = useState(false);
  const score = movie.score || movie.sem_score || 0;

  return (
    <div
      className="group relative bg-surface-1 border border-white/[0.04] hover:border-white/[0.1] rounded-xl overflow-hidden cursor-pointer transition-all duration-200 hover:shadow-lg hover:shadow-black/20"
      style={{ animation: `fadeInUp 0.3s ease-out ${index * 0.04}s both` }}
      onClick={onClick}
    >
      <div className="relative aspect-[2/3] bg-surface-2 overflow-hidden">
        {movie.poster_url && !imgErr ? (
          <img src={movie.poster_url} alt={movie.title}
            className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
            onError={() => setImgErr(true)} loading="lazy" />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-text-dim">
            <span className="text-xs text-center px-4">{movie.title}</span>
          </div>
        )}

        {/* IMDB rating + relevance score badges */}
        <div className="absolute top-2 right-2 flex flex-col gap-1">
          {movie.imdb_rating && (
            <div className="bg-yellow-500/90 backdrop-blur-sm rounded-md px-1.5 py-0.5 text-[10px] font-bold text-black flex items-center gap-0.5">
              ★ {movie.imdb_rating.toFixed(1)}
            </div>
          )}
          {score > 0 && (
            <div className="bg-black/70 backdrop-blur-sm rounded-md px-1.5 py-0.5 text-[10px] font-mono text-white/80">
              {(score * 100).toFixed(0)}%
            </div>
          )}
        </div>

        {movie.genres && movie.genres.length > 0 && (
          <div className="absolute bottom-0 left-0 right-0 px-2 pb-2 pt-8 bg-gradient-to-t from-black/80 to-transparent">
            <div className="flex flex-wrap gap-1">
              {movie.genres.slice(0, 3).map(g => (
                <span key={g} className="text-[10px] bg-white/15 backdrop-blur-sm text-white/90 px-1.5 py-0.5 rounded-full">{g}</span>
              ))}
            </div>
          </div>
        )}

        {/* Like / Dislike buttons — always visible */}
        <div className="absolute top-2 left-2 flex gap-1.5">
          <button
            onClick={e => { e.stopPropagation(); onLike(); }}
            className={`w-9 h-9 rounded-full flex items-center justify-center text-sm font-bold transition-all ${
              isLiked
                ? 'bg-emerald-500 text-white shadow-lg shadow-emerald-500/40 scale-110'
                : 'bg-black/60 backdrop-blur-sm text-white/80 hover:bg-emerald-500/80 hover:text-white hover:scale-105'
            }`}
          >♥</button>
          <button
            onClick={e => { e.stopPropagation(); onDislike(); }}
            className={`w-9 h-9 rounded-full flex items-center justify-center text-sm font-bold transition-all ${
              isDisliked
                ? 'bg-red-500 text-white shadow-lg shadow-red-500/40 scale-110'
                : 'bg-black/60 backdrop-blur-sm text-white/80 hover:bg-red-500/80 hover:text-white hover:scale-105'
            }`}
          >✕</button>
        </div>
      </div>

      <div className="p-3 space-y-1.5">
        <div className="flex items-start justify-between gap-2">
          <h3 className="text-sm font-semibold text-text-primary leading-tight line-clamp-2">{movie.title}</h3>
          {movie.year && <span className="text-[11px] text-text-dim flex-shrink-0 mt-0.5">{movie.year}</span>}
        </div>
        {movie.directors && movie.directors.length > 0 && (
          <p className="text-[11px] text-text-dim truncate">{movie.directors.join(', ')}</p>
        )}
        <div className="mt-2 pt-2 border-t border-white/[0.04]">
          {explanation ? (
            <p className="text-[11px] leading-relaxed text-text-secondary line-clamp-3">
              <span className="text-accent font-medium">AI: </span>{explanation}
            </p>
          ) : isEnriching ? (
            <div className="space-y-1">
              <div className="h-3 w-full shimmer rounded" />
              <div className="h-3 w-3/4 shimmer rounded" />
            </div>
          ) : null}
          {groupPerspective && (
            <p className="text-[11px] leading-relaxed text-purple-300/80 mt-1.5 line-clamp-3">
              <span className="text-purple-400 font-medium">Group: </span>{groupPerspective}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}