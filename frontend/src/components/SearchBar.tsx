'use client';

import { useRef } from 'react';

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  isSearching: boolean;
}

export default function SearchBar({ value, onChange, onSubmit, isSearching }: SearchBarProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  return (
    <div className="relative group">
      <div className={`
        relative flex items-center bg-surface-1 border rounded-xl
        transition-all duration-200
        ${isSearching
          ? 'border-accent/40 shadow-[0_0_24px_-4px_rgba(109,90,255,0.2)]'
          : 'border-white/[0.06] hover:border-white/[0.12] focus-within:border-accent/40 focus-within:shadow-[0_0_24px_-4px_rgba(109,90,255,0.15)]'
        }
      `}>
        {/* Search icon */}
        <div className="pl-4 pr-2">
          {isSearching ? (
            <svg className="w-5 h-5 text-accent animate-spin" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
          ) : (
            <svg className="w-5 h-5 text-text-dim group-focus-within:text-accent transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
          )}
        </div>

        <input
          ref={inputRef}
          type="text"
          value={value}
          onChange={e => onChange(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') onSubmit(); }}
          placeholder="Describe the movie vibe you're looking for..."
          className="flex-1 bg-transparent py-3.5 pr-4 text-[15px] text-text-primary placeholder:text-text-dim/60 outline-none"
          autoFocus
        />

        {value && (
          <button
            onClick={() => { onChange(''); inputRef.current?.focus(); }}
            className="pr-3 text-text-dim hover:text-text-secondary transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}

        <button
          onClick={onSubmit}
          disabled={!value.trim() || isSearching}
          className="mr-2 px-4 py-2 bg-accent hover:bg-accent-light disabled:bg-surface-3 disabled:text-text-dim text-white text-sm font-medium rounded-lg transition-all"
        >
          Search
        </button>
      </div>

      <p className="text-[11px] text-text-dim mt-1.5 pl-1">
        Try a vibe, a mood, a comparison, or a specific movie name
      </p>
    </div>
  );
}
