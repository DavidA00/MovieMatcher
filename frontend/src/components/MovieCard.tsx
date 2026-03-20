'use client';

import { useState } from 'react';
import type { SearchResult } from '@/lib/api';

interface Props {
  movie: SearchResult;
  explanation?: string;
  isLiked: boolean;
  isDisliked: boolean;
  onClick: () => void;
  onLike: () => void;
  onDislike: () => void;
  onMix: () => void;
  index: number;
  isEnriching?: boolean;
}

export default function MovieCard({
  movie, explanation, isLiked, isDisliked,
  onClick, onLike, onDislike, onMix, index, isEnriching,
}: Props) {
  const [imgErr, setImgErr] = useState(false);
  const score = movie.score || movie.sem_score || 0;

  return (
    <div
      className="animate-fade-in group relative bg-surface-1 border border-white/[0.04] hover:border-white/[0.1] rounded-xl overflow-hidden cursor-pointer transition-all duration-200 hover:shadow-lg hover:shadow-black/20"
      style={{ animationDelay: `${index * 40}ms`, opacity: 0 }}
      onClick={onClick}
    >
      {/* Poster */}
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

        {/* Score */}
        {score > 0 && (
          <div className="absolute top-2 right-2 bg-black/70 backdrop-blur-sm rounded-md px-1.5 py-0.5 text-[10px] font-mono text-white/80">
            {(score * 100).toFixed(0)}%
          </div>
        )}

        {/* Genres overlay */}
        {movie.genres && movie.genres.length > 0 && (
          <div className="absolute bottom-0 left-0 right-0 px-2 pb-2 pt-8 bg-gradient-to-t from-black/80 to-transparent">
            <div className="flex flex-wrap gap-1">
              {movie.genres.slice(0, 3).map(g => (
                <span key={g} className="text-[10px] bg-white/15 backdrop-blur-sm text-white/90 px-1.5 py-0.5 rounded-full">{g}</span>
              ))}
            </div>
          </div>
        )}

        {/* Action buttons on hover */}
        <div className="absolute top-2 left-2 flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
          {[
            { act: onLike, active: isLiked, color: 'sage', icon: '♥' },
            { act: onDislike, active: isDisliked, color: 'ember', icon: '✕' },
            { act: onMix, active: false, color: 'accent', icon: '+' },
          ].map(({ act, active, color, icon }, i) => (
            <button key={i} onClick={e => { e.stopPropagation(); act(); }}
              className={`w-7 h-7 rounded-full flex items-center justify-center text-xs transition-all ${
                active ? `bg-${color} text-white shadow-lg` : `bg-black/50 backdrop-blur-sm text-white/70 hover:bg-${color}/80 hover:text-white`
              }`}>{icon}</button>
          ))}
        </div>
      </div>

      {/* Info */}
      <div className="p-3 space-y-1.5">
        <div className="flex items-start justify-between gap-2">
          <h3 className="text-sm font-semibold text-text-primary leading-tight line-clamp-2">{movie.title}</h3>
          {movie.year && <span className="text-[11px] text-text-dim flex-shrink-0 mt-0.5">{movie.year}</span>}
        </div>

        {movie.directors && movie.directors.length > 0 && (
          <p className="text-[11px] text-text-dim truncate">{movie.directors.join(', ')}</p>
        )}

        {/* AI reasoning — shows shimmer while enriching */}
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
        </div>
      </div>
    </div>
  );
}
