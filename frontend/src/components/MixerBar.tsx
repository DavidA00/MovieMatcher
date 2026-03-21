'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import * as api from '@/lib/api';
import type { AutocompleteResult } from '@/lib/api';

interface SelectedMovie {
  movieId: number;
  title: string;
  year?: number;
  poster_url?: string | null;
  weight: number;  // 0-1
}

interface Props {
  selected: SelectedMovie[];
  onUpdate: (movies: SelectedMovie[]) => void;
  sessionId: string;
}

export default function MixerBar({ selected, onUpdate, sessionId }: Props) {
  const [searchText, setSearchText] = useState('');
  const [suggestions, setSuggestions] = useState<AutocompleteResult[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();

  const doAutocomplete = useCallback(async (q: string) => {
    setIsLoading(true);
    try {
      const resp = await api.movieAutocomplete(q || '', sessionId);
      setSuggestions(resp.results);
      setShowDropdown(resp.results.length > 0);
    } catch { }
    finally { setIsLoading(false); }
  }, [sessionId]);

  useEffect(() => {
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => doAutocomplete(searchText), 500);
    return () => clearTimeout(debounceRef.current);
  }, [searchText, doAutocomplete]);

  const handleFocus = () => doAutocomplete(searchText);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) setShowDropdown(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const handleSelect = (movie: AutocompleteResult) => {
    if (selected.find(s => s.movieId === movie.movieId)) return;
    onUpdate([...selected, { movieId: movie.movieId, title: movie.title, year: movie.year, poster_url: movie.poster_url, weight: 1.0 }]);
    setSearchText('');
    setShowDropdown(false);
    inputRef.current?.focus();
  };

  const handleRemove = (movieId: number) => {
    onUpdate(selected.filter(s => s.movieId !== movieId));
  };

  const handleWeightChange = (movieId: number, weight: number) => {
    onUpdate(selected.map(s => s.movieId === movieId ? { ...s, weight } : s));
  };

  const selectedIds = new Set(selected.map(s => s.movieId));

  return (
    <div className="space-y-3">
      <div>
        <h2 className="text-sm font-semibold text-text-primary">Movie Mixer</h2>
        <p className="text-[11px] text-text-dim">
          Add movies and adjust their weights to find films at the intersection.
          {selected.length < 2 && ' Add at least 2 movies.'}
        </p>
      </div>

      {/* Selected movies with weight sliders */}
      {selected.length > 0 && (
        <div className="space-y-2">
          {selected.map(m => (
            <div key={m.movieId}
              className="flex items-center gap-3 bg-surface-1 border border-white/[0.06] rounded-lg px-3 py-2">
              {m.poster_url && (
                <img src={m.poster_url} alt="" className="w-8 h-12 object-cover rounded flex-shrink-0" />
              )}
              <div className="min-w-0 flex-1">
                <div className="flex items-center justify-between">
                  <p className="text-xs font-medium text-text-primary truncate max-w-[140px]">{m.title}</p>
                  <span className="text-[10px] text-text-dim font-mono ml-2">{Math.round(m.weight * 100)}%</span>
                </div>
                <input type="range" min={0.1} max={1} step={0.05} value={m.weight}
                  onChange={e => handleWeightChange(m.movieId, parseFloat(e.target.value))}
                  className="w-full mt-1" />
              </div>
              <button onClick={() => handleRemove(m.movieId)}
                className="w-5 h-5 rounded-full bg-surface-2 text-text-dim hover:text-red-400 flex items-center justify-center text-[10px] transition-colors flex-shrink-0">
                ✕
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Autocomplete search */}
      <div className="relative" ref={dropdownRef}>
        <div className="relative flex items-center bg-surface-1 border border-white/[0.06] rounded-xl focus-within:border-accent/40 transition-colors">
          <div className="pl-3 pr-2">
            <svg className="w-4 h-4 text-text-dim" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
          </div>
          <input ref={inputRef} type="text" value={searchText}
            onChange={e => setSearchText(e.target.value)} onFocus={handleFocus}
            placeholder="Search movies to add..."
            className="flex-1 bg-transparent py-2.5 pr-3 text-sm text-text-primary placeholder:text-text-dim/50 outline-none" />
          {isLoading && (
            <div className="pr-3">
              <svg className="w-4 h-4 text-accent/50 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            </div>
          )}
        </div>

        {showDropdown && suggestions.length > 0 && (
          <div className="absolute top-full mt-1 left-0 right-0 bg-surface-2 border border-white/[0.08] rounded-lg shadow-2xl z-50 max-h-64 overflow-y-auto">
            {suggestions.map(s => {
              const isSel = selectedIds.has(s.movieId);
              return (
                <button key={s.movieId} onClick={() => handleSelect(s)} disabled={isSel}
                  className={`w-full flex items-center gap-3 px-3 py-2 text-left transition-colors ${isSel ? 'opacity-40' : 'hover:bg-white/[0.04]'}`}>
                  {s.poster_url ? (
                    <img src={s.poster_url} alt="" className="w-7 h-10 object-cover rounded flex-shrink-0" />
                  ) : (
                    <div className="w-7 h-10 bg-surface-3 rounded flex-shrink-0" />
                  )}
                  <div className="min-w-0 flex-1">
                    <p className="text-sm text-text-primary truncate">{s.title}</p>
                    <p className="text-[10px] text-text-dim">{s.year || ''}</p>
                  </div>
                  {s.is_liked && <span className="text-emerald-400 text-[10px]">♥</span>}
                  {isSel && <span className="text-[10px] text-text-dim">added</span>}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}