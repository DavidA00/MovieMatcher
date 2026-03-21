'use client';

interface Ref { query: string; dimension: string; rationale: string; }

interface Props {
  reformulations: Ref[];
  onClick: (ref: Ref) => void;
  isCollapsed: boolean;
  onToggleCollapse: () => void;
  isLoading: boolean;
}

const DIM_STYLE: Record<string, { bg: string; icon: string }> = {
  mood:       { bg: 'bg-purple-500/10 text-purple-300 border-purple-500/20', icon: '✦' },
  era:        { bg: 'bg-amber-500/10 text-amber-300 border-amber-500/20',   icon: '⏳' },
  style:      { bg: 'bg-cyan-500/10 text-cyan-300 border-cyan-500/20',      icon: '🎬' },
  theme:      { bg: 'bg-emerald-500/10 text-emerald-300 border-emerald-500/20', icon: '◈' },
  comparison: { bg: 'bg-pink-500/10 text-pink-300 border-pink-500/20',      icon: '↔' },
};

export default function ReformulationSuggestions({
  reformulations, onClick, isCollapsed, onToggleCollapse, isLoading,
}: Props) {
  const hasRefs = reformulations.length > 0;

  // Nothing to show and not loading — hide entirely
  if (!hasRefs && !isLoading) return null;

  return (
    <div className="mt-3">
      {/* Header — always visible when we have refs or are loading */}
      <button
        onClick={onToggleCollapse}
        className="flex items-center gap-1.5 text-[11px] font-medium text-text-dim uppercase tracking-wider pl-1 mb-1.5 hover:text-text-secondary transition-colors"
      >
        <svg
          className={`w-3 h-3 transition-transform duration-200 ${isCollapsed ? '' : 'rotate-90'}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
        </svg>
        Refine your search
        {hasRefs && <span className="text-text-dim/50 font-normal normal-case">({reformulations.length})</span>}
        {isLoading && !hasRefs && (
          <span className="flex items-center gap-1 text-accent/60 font-normal normal-case ml-1">
            <svg className="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            generating...
          </span>
        )}
      </button>

      {/* Body */}
      {!isCollapsed && (
        <div className="flex flex-col gap-1">
          {/* Loading shimmer when no refs yet */}
          {isLoading && !hasRefs && (
            <div className="space-y-1.5 animate-fade-in">
              {[1, 2, 3].map(i => (
                <div key={i} className="flex items-center gap-2.5 px-3 py-2.5 rounded-lg bg-surface-1 border border-white/[0.04]">
                  <div className="w-5 h-5 rounded shimmer flex-shrink-0" />
                  <div className="flex-1 space-y-1">
                    <div className="h-3.5 w-4/5 shimmer rounded" />
                    <div className="h-2.5 w-2/5 shimmer rounded" />
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Actual reformulations */}
          {hasRefs && reformulations.map((ref, i) => {
            const s = DIM_STYLE[ref.dimension] || DIM_STYLE.theme;
            return (
              <button
                key={`${ref.query}-${i}`}
                onClick={() => onClick(ref)}
                className="group flex items-start gap-2.5 px-3 py-2 rounded-lg bg-surface-1 border border-white/[0.04] hover:border-white/[0.1] text-left transition-all hover:bg-surface-2"
                style={{ animation: `fadeInUp 0.3s ease-out ${i * 0.05}s both` }}
              >
                <span className={`inline-flex items-center justify-center w-5 h-5 rounded text-[11px] border ${s.bg} flex-shrink-0 mt-0.5`}>
                  {s.icon}
                </span>
                <div className="min-w-0 flex-1">
                  <p className="text-sm text-text-primary group-hover:text-white truncate">{ref.query}</p>
                  <p className="text-[11px] text-text-dim mt-0.5 line-clamp-1">{ref.rationale}</p>
                </div>
                <svg className="w-4 h-4 text-text-dim group-hover:text-accent flex-shrink-0 mt-1 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                </svg>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}