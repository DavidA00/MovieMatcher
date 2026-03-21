'use client';
import { useState } from 'react';

interface Props {
  userName: string;
  partyName: string;
  isAdmin: boolean;
  partyMembers: { session_id: string; name: string }[];
  sessionId: string;
  view: 'search' | 'mixer' | 'fuse';
  onViewChange: (v: 'search' | 'mixer' | 'fuse') => void;
  currentRound: number;
  preferenceIntensity: number;
  onPreferenceIntensityChange: (v: number) => void;
  lam: number;
  onLamChange: (v: number) => void;
  kFetch: number;
  onKFetchChange: (v: number) => void;
  steeringStrength: number;
  onSteeringStrengthChange: (v: number) => void;
  likedCount: number;
  dislikedCount: number;
  mixerCount: number;
  onShowLikedPanel: () => void;
  onLeaveParty: () => void;
  onLeaveAssign: (newAdminSid: string) => void;
  onCancelParty: () => void;
}

export default function TopBar({
  userName, partyName, isAdmin, partyMembers, sessionId, view, onViewChange, currentRound,
  preferenceIntensity, onPreferenceIntensityChange,
  lam, onLamChange, kFetch, onKFetchChange,
  steeringStrength, onSteeringStrengthChange,
  likedCount, dislikedCount, mixerCount, onShowLikedPanel,
  onLeaveParty, onLeaveAssign, onCancelParty,
}: Props) {
  const [showSettings, setShowSettings] = useState(false);
  const [showUserMenu, setShowUserMenu] = useState(false);
  const assignCandidates = partyMembers.filter(m => m.session_id !== sessionId);

  return (
    <header className="h-14 border-b border-white/[0.06] bg-surface-1/80 backdrop-blur-xl sticky top-0 z-40">
      <div className="max-w-[1400px] mx-auto h-full flex items-center justify-between px-6">
        <div className="flex items-center gap-6">
          <h1 className="text-lg font-bold tracking-tight">
            <span className="text-accent">Movie</span><span className="text-text-primary">Matcher</span>
          </h1>
          <nav className="flex gap-1 items-center">
            <button onClick={() => onViewChange('search')}
              className={`px-3 py-1.5 rounded-md text-sm font-medium transition-all ${
                view === 'search' ? 'bg-accent/15 text-accent-light' : 'text-text-secondary hover:text-text-primary hover:bg-white/[0.04]'
              }`}>Search</button>
            <button onClick={() => onViewChange('mixer')}
              className={`px-3 py-1.5 rounded-md text-sm font-medium transition-all flex items-center gap-1.5 ${
                view === 'mixer' ? 'bg-accent/15 text-accent-light' : 'text-text-secondary hover:text-text-primary hover:bg-white/[0.04]'
              }`}>
              Mixer
              {mixerCount > 0 && (
                <span className="bg-accent/20 text-accent text-xs px-1.5 py-0.5 rounded-full">{mixerCount}</span>
              )}
            </button>
            <button onClick={() => onViewChange('fuse')}
              className={`px-3 py-1.5 rounded-md text-sm font-medium transition-all ${
                view === 'fuse' ? 'bg-purple-500/15 text-purple-300' : 'text-text-secondary hover:text-text-primary hover:bg-white/[0.04]'
              }`}>Fuse</button>
            {currentRound > 1 && (
              <span className="ml-1 bg-purple-500/20 text-purple-300 text-[10px] font-bold px-2 py-0.5 rounded-full">
                Round {currentRound}
              </span>
            )}
          </nav>
        </div>

        <div className="flex items-center gap-3">
          <div className="relative">
            <button onClick={() => setShowUserMenu(!showUserMenu)}
              className="flex items-center gap-2 px-2 py-1 rounded-md hover:bg-white/[0.04] transition-colors">
              <div className="w-6 h-6 rounded-full bg-accent/20 flex items-center justify-center text-[11px] font-semibold text-accent">
                {userName[0].toUpperCase()}
              </div>
              <span className="text-sm text-text-secondary font-medium">{userName}</span>
              <span className="text-[10px] text-text-dim bg-surface-2 px-1.5 py-0.5 rounded-full">{partyName}</span>
            </button>
            {showUserMenu && (
              <>
                <div className="fixed inset-0 z-40" onClick={() => setShowUserMenu(false)} />
                <div className="absolute right-0 top-full mt-2 w-72 bg-surface-2 border border-white/[0.08] rounded-lg shadow-2xl p-3 z-50 animate-fade-in">
                  {!isAdmin ? (
                    <button
                      onClick={() => { setShowUserMenu(false); onLeaveParty(); }}
                      className="w-full text-left px-3 py-2 rounded-md text-sm text-red-300 hover:bg-red-500/10"
                    >
                      Exit party
                    </button>
                  ) : (
                    <div className="space-y-2">
                      <div>
                        <p className="text-[10px] text-text-dim uppercase tracking-wide px-1 mb-1">Exit and assign new admin</p>
                        <div className="space-y-1">
                          {assignCandidates.length === 0 ? (
                            <p className="text-xs text-text-dim px-2 py-1">No other members to assign.</p>
                          ) : assignCandidates.map(m => (
                            <button key={m.session_id}
                              onClick={() => { setShowUserMenu(false); onLeaveAssign(m.session_id); }}
                              className="w-full text-left px-3 py-2 rounded-md text-sm text-text-secondary hover:bg-white/[0.04]"
                            >
                              Assign to {m.name} and exit
                            </button>
                          ))}
                        </div>
                      </div>
                      <button
                        onClick={() => { setShowUserMenu(false); onCancelParty(); }}
                        className="w-full text-left px-3 py-2 rounded-md text-sm text-red-300 hover:bg-red-500/10"
                      >
                        Cancel party
                      </button>
                    </div>
                  )}
                </div>
              </>
            )}
          </div>

          {(likedCount > 0 || dislikedCount > 0) && (
            <button onClick={onShowLikedPanel}
              className="flex items-center gap-2 text-xs text-text-dim hover:text-text-secondary px-2 py-1 rounded-md hover:bg-white/[0.04] transition-colors">
              {likedCount > 0 && <span className="flex items-center gap-0.5"><span className="text-emerald-400">♥</span>{likedCount}</span>}
              {dislikedCount > 0 && <span className="flex items-center gap-0.5"><span className="text-red-400">✕</span>{dislikedCount}</span>}
            </button>
          )}

          <div className="relative">
            <button onClick={() => setShowSettings(!showSettings)}
              className="p-2 rounded-md text-text-dim hover:text-text-primary hover:bg-white/[0.04] transition-colors">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.325.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 011.37.49l1.296 2.247a1.125 1.125 0 01-.26 1.431l-1.003.827c-.293.241-.438.613-.431.992a7.723 7.723 0 010 .255c-.007.378.138.75.43.991l1.004.827c.424.35.534.954.26 1.43l-1.298 2.247a1.125 1.125 0 01-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.47 6.47 0 01-.22.128c-.331.183-.581.495-.644.869l-.213 1.281c-.09.543-.56.94-1.11.94h-2.594c-.55 0-1.019-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 01-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 01-1.369-.49l-1.297-2.247a1.125 1.125 0 01.26-1.431l1.004-.827c.292-.24.437-.613.43-.991a6.932 6.932 0 010-.255c.007-.38-.138-.751-.43-.992l-1.004-.827a1.125 1.125 0 01-.26-1.43l1.297-2.247a1.125 1.125 0 011.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.086.22-.128.332-.183.582-.495.644-.869l.214-1.28z" />
                <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
            </button>
            {showSettings && (
              <>
                <div className="fixed inset-0 z-40" onClick={() => setShowSettings(false)} />
                <div className="absolute right-0 top-full mt-2 w-80 bg-surface-2 border border-white/[0.08] rounded-lg shadow-2xl p-4 z-50 animate-fade-in">
                  <h3 className="text-sm font-semibold text-text-primary mb-4">Settings</h3>

                  <div className="space-y-5">
                    <div>
                      <label className="text-xs text-text-secondary flex justify-between mb-1.5">
                        <span>Preference influence</span>
                        <span className="text-text-dim font-mono">{Math.round(preferenceIntensity * 100)}%</span>
                      </label>
                      <input type="range" min={0} max={1} step={0.05} value={preferenceIntensity}
                        onChange={e => onPreferenceIntensityChange(parseFloat(e.target.value))} className="w-full" />
                      <div className="flex justify-between text-[10px] text-text-dim mt-0.5"><span>Explore</span><span>Personalize</span></div>
                      <p className="text-[10px] text-text-dim/70 mt-1.5 leading-relaxed">
                        Controls how your liked/disliked movies influence the <strong>sorting</strong> of results (not the search itself).
                        At 0%, results are sorted purely by relevance score.
                        At 100%, movies similar to ones you liked are pushed to the top,
                        and movies similar to ones you disliked are pushed down.
                      </p>
                    </div>

                    <div>
                      <label className="text-xs text-text-secondary flex justify-between mb-1.5">
                        <span>Steering strength</span>
                        <span className="text-text-dim font-mono">{steeringStrength.toFixed(2)}</span>
                      </label>
                      <input type="range" min={0.1} max={1} step={0.05} value={steeringStrength}
                        onChange={e => onSteeringStrengthChange(parseFloat(e.target.value))} className="w-full" />
                      <div className="flex justify-between text-[10px] text-text-dim mt-0.5">
                        <span>Subtle</span><span>Strong</span>
                      </div>
                      <p className="text-[10px] text-text-dim/70 mt-1.5 leading-relaxed">
                        Controls how strongly the Genre Mixer and Era Feel steer the search direction.
                        At low values, the query/mixer dominates and genre/era have a light influence.
                        At high values, genre and era directions strongly shape what movies appear.
                        Applies equally whether used individually or combined.
                      </p>
                    </div>

                    <div>
                      <label className="text-xs text-text-secondary flex justify-between mb-1.5">
                        <span>Search blend (λ)</span>
                        <span className="text-text-dim font-mono">{lam.toFixed(2)}</span>
                      </label>
                      <input type="range" min={0} max={1} step={0.05} value={lam}
                        onChange={e => onLamChange(parseFloat(e.target.value))} className="w-full" />
                      <div className="flex justify-between text-[10px] text-text-dim mt-0.5">
                        <span>Graph (cast/genre)</span><span>Semantic (vibe/mood)</span>
                      </div>
                      <p className="text-[10px] text-text-dim/70 mt-1.5 leading-relaxed">
                        Controls how results are ranked: graph similarity finds movies sharing cast, directors, and genre structure.
                        Semantic similarity matches the mood and theme of your query text.
                      </p>
                    </div>

                    <div>
                      <label className="text-xs text-text-secondary flex justify-between mb-1.5">
                        <span>Candidate pool size</span>
                        <span className="text-text-dim font-mono">{kFetch}</span>
                      </label>
                      <input type="range" min={10} max={100} step={5} value={kFetch}
                        onChange={e => onKFetchChange(parseInt(e.target.value))} className="w-full" />
                      <div className="flex justify-between text-[10px] text-text-dim mt-0.5">
                        <span>Faster (fewer)</span><span>Better (more)</span>
                      </div>
                      <p className="text-[10px] text-text-dim/70 mt-1.5 leading-relaxed">
                        How many movies to fetch before applying hard filters.
                        Higher = better matches when filters are active, but slightly slower.
                      </p>
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </header>
  );
}