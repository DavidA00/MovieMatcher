'use client';

interface Props {
  mode: string;
  count: number;
  isSearching: boolean;
  topScore: number;
  elapsedMs?: number;
  isEnriching?: boolean;
}

export default function StatusBar({ mode, count, isSearching, topScore, elapsedMs, isEnriching }: Props) {
  if (!mode && !isSearching) return null;

  return (
    <div className="flex items-center justify-between py-2 px-1 text-[11px]">
      <div className="flex items-center gap-2">
        {isSearching ? (
          <span className="text-accent flex items-center gap-1.5">
            <span className="w-1.5 h-1.5 bg-accent rounded-full animate-pulse" />
            Searching...
          </span>
        ) : (
          <>
            <span className="text-text-dim">{count} results</span>
            {mode && <span className="text-text-dim/50">·</span>}
            {mode && <span className="font-mono text-accent/70">{mode}</span>}
            {elapsedMs != null && elapsedMs > 0 && (
              <>
                <span className="text-text-dim/50">·</span>
                <span className="font-mono text-text-dim">{elapsedMs}ms</span>
              </>
            )}
          </>
        )}
        {isEnriching && !isSearching && (
          <span className="text-text-dim flex items-center gap-1 ml-2">
            <svg className="w-3 h-3 animate-spin text-accent/50" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            AI reasoning loading...
          </span>
        )}
      </div>
      {topScore > 0 && !isSearching && (
        <span className="font-mono text-text-dim">{(topScore * 100).toFixed(0)}% match</span>
      )}
    </div>
  );
}
