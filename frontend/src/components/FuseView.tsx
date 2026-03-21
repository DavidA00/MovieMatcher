'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import * as api from '@/lib/api';
import type { PartyStatus, RoundSummary, SearchResult } from '@/lib/api';
import { deleteUser } from '@/lib/firebase';
import MovieGrid from './MovieGrid';

interface Props {
  sessionId: string;
  partyName: string;
  currentRound: number;
  onRoundChange: (round: number) => void;
  likedIds: Set<number>;
  dislikedIds: Set<number>;
  onFeedback: (m: SearchResult, action: 'like' | 'dislike' | 'clear') => void;
  onMovieClick: (m: SearchResult) => void;
}

export default function FuseView({
  sessionId, partyName, currentRound, onRoundChange,
  likedIds, dislikedIds, onFeedback, onMovieClick,
}: Props) {
  const [status, setStatus] = useState<PartyStatus | null>(null);
  const [isFusing, setIsFusing] = useState(false);
  const [error, setError] = useState('');
  const pollRef = useRef<ReturnType<typeof setInterval>>();

  const fetchStatus = useCallback(async () => {
    try {
      const s = await api.partyStatus(sessionId, partyName);
      setStatus(s);
      if (s.round !== currentRound) onRoundChange(s.round);
    } catch { }
  }, [sessionId, partyName, currentRound, onRoundChange]);

  useEffect(() => {
    fetchStatus();
    pollRef.current = setInterval(fetchStatus, 3000);
    return () => clearInterval(pollRef.current);
  }, [fetchStatus]);

  const handleToggleReady = async () => {
    if (!status) return;
    const myUser = status.users.find(u => u.session_id === sessionId);
    const s = await api.partyReady(sessionId, partyName, !myUser?.ready);
    setStatus(s);
  };

  const handleFuse = async () => {
    if (isFusing) return;
    setIsFusing(true); setError('');
    try {
      const result = await api.partyFuse(sessionId, partyName);
      if (result.status === 'complete') await fetchStatus();
      else if (result.status === 'error') setError(result.message || 'Fuse failed');
    } catch { setError('Failed to fuse'); }
    finally { setIsFusing(false); }
  };

  const handleRemovePlayer = async (targetSid: string, targetName: string) => {
    try {
      const s = await api.partyRemove(sessionId, partyName, targetSid);
      await deleteUser(partyName, targetName);
      setStatus(s);
    } catch (e: any) {
      setError(e?.message || 'Failed to remove player');
    }
  };

  if (!status) {
    return (
      <div className="flex items-center justify-center py-20">
        <svg className="w-6 h-6 text-accent animate-spin" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      </div>
    );
  }

  const myUser = status.users.find(u => u.session_id === sessionId);
  const imReady = myUser?.ready ?? false;
  const imAdmin = status.admin_sid === sessionId;
  const readyCount = status.users.filter(u => u.ready).length;

  return (
    <div className="max-w-[1400px] mx-auto py-6 px-6 space-y-8">
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-bold text-text-primary">
          <span className="text-purple-400">Fuse</span> — {partyName}
        </h2>
        <p className="text-sm text-text-secondary">
          When everyone is ready, the AI analyzes all preferences and suggests movies for the group
        </p>
      </div>

      {/* Users grid */}
      <div className="space-y-3">
        <h3 className="text-sm font-semibold text-text-dim uppercase tracking-wider">
          Party members ({status.users.length}/12)
        </h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {status.users.map(u => {
            const isMe = u.session_id === sessionId;
            return (
              <div key={u.session_id}
                className={`flex items-center gap-3 p-4 rounded-xl border transition-all ${
                  u.ready ? 'bg-emerald-500/10 border-emerald-500/30' : 'bg-surface-1 border-white/[0.06]'
                }`}>
                <div className={`w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold ${
                  u.ready ? 'bg-emerald-500 text-white' : 'bg-surface-2 text-text-dim'
                }`}>
                  {u.name[0].toUpperCase()}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-text-primary truncate">
                    {u.name}
                    {isMe && <span className="text-text-dim font-normal"> (you)</span>}
                    {u.is_admin && <span className="text-amber-400 text-[10px] font-normal ml-1">★ admin</span>}
                  </p>
                  <p className="text-[11px] text-text-dim">
                    {u.liked_count} liked · {u.disliked_count} disliked · {u.search_count} searches
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  <div className={`text-xs font-medium px-2 py-1 rounded-full ${
                    u.ready ? 'bg-emerald-500/20 text-emerald-400' : 'bg-surface-2 text-text-dim'
                  }`}>
                    {u.ready ? 'Ready' : 'Browsing'}
                  </div>
                  {imAdmin && !isMe && (
                    <button onClick={() => handleRemovePlayer(u.session_id, u.name)}
                      className="w-6 h-6 rounded-full bg-surface-2 hover:bg-red-500/20 text-text-dim hover:text-red-400 flex items-center justify-center text-[10px] transition-colors"
                      title={`Remove ${u.name}`}>
                      ✕
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Controls */}
      <div className="flex flex-col items-center gap-3">
        <button onClick={handleToggleReady}
          className={`px-8 py-3 rounded-xl text-base font-semibold transition-all ${
            imReady
              ? 'bg-surface-2 border border-white/[0.08] text-text-secondary hover:bg-surface-3'
              : 'bg-purple-500 hover:bg-purple-400 text-white shadow-lg shadow-purple-500/20'
          }`}>
          {imReady ? 'Cancel — keep browsing' : "I'm ready to Fuse!"}
        </button>
        <p className="text-xs text-text-dim">
          {readyCount} / {status.users.length} ready
          {status.users.length < 2 && ' · need at least 2 people'}
        </p>
        {status.all_ready && (
          <button onClick={handleFuse} disabled={isFusing}
            className="px-10 py-3.5 bg-gradient-to-r from-purple-500 to-accent hover:from-purple-400 hover:to-accent-light disabled:opacity-50 text-white text-lg font-bold rounded-xl shadow-xl transition-all animate-fade-in">
            {isFusing ? (
              <span className="flex items-center gap-2">
                <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Analyzing everyone&apos;s preferences...
              </span>
            ) : `Fuse! — End Round ${currentRound}`}
          </button>
        )}
        {error && <p className="text-sm text-red-400">{error}</p>}
      </div>

      {/* Round summaries */}
      {status.round_summaries.length > 0 && (
        <div className="space-y-8">
          {[...status.round_summaries].reverse().map((rs, idx) => (
            <RoundSummaryCard key={rs.round} summary={rs} isLatest={idx === 0}
              likedIds={likedIds} dislikedIds={dislikedIds}
              onFeedback={onFeedback} onMovieClick={onMovieClick} />
          ))}
        </div>
      )}
    </div>
  );
}

// ── Bullet list renderer (handles both string and array) ─────

function BulletList({ items, color }: { items: string | string[]; color: 'emerald' | 'amber' }) {
  const arr = Array.isArray(items) ? items : [items];
  const dotClass = color === 'emerald' ? 'text-emerald-400' : 'text-amber-400';
  return (
    <ul className="space-y-2">
      {arr.map((item, i) => (
        <li key={i} className="flex gap-2.5 text-sm text-text-secondary leading-relaxed">
          <span className={`${dotClass} mt-1 flex-shrink-0`}>•</span>
          <span>{item}</span>
        </li>
      ))}
    </ul>
  );
}

// ── Round Summary Card ───────────────────────────────────────

function RoundSummaryCard({ summary, isLatest, likedIds, dislikedIds, onFeedback, onMovieClick }: {
  summary: RoundSummary; isLatest: boolean;
  likedIds: Set<number>; dislikedIds: Set<number>;
  onFeedback: (m: SearchResult, action: 'like' | 'dislike' | 'clear') => void;
  onMovieClick: (m: SearchResult) => void;
}) {
  const [expanded, setExpanded] = useState(isLatest);

  const movieResults: SearchResult[] = (summary.suggestions || []).map(s => ({
    movieId: s.movieId, title: s.title, year: s.year, poster_url: s.poster_url,
    genres: s.genres, imdb_rating: s.imdb_rating, overview: s.overview,
  }));

  return (
    <div className={`rounded-xl border overflow-hidden transition-all ${
      isLatest ? 'border-purple-500/30 bg-surface-1' : 'border-white/[0.04] bg-surface-1/50'
    }`}>
      <button onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-white/[0.02] transition-colors">
        <div className="flex items-center gap-3">
          <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
            isLatest ? 'bg-purple-500 text-white' : 'bg-surface-3 text-text-dim'
          }`}>{summary.round}</div>
          <div>
            <h4 className="text-sm font-semibold text-text-primary">Round {summary.round} Summary</h4>
            <p className="text-[11px] text-text-dim truncate max-w-md">
              {summary.suggestions.length} suggestions
            </p>
          </div>
        </div>
        <svg className={`w-4 h-4 text-text-dim transition-transform ${expanded ? 'rotate-180' : ''}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {expanded && (
        <div className="px-4 pb-6 space-y-5 animate-fade-in">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
            <div className="space-y-2">
              <h5 className="text-xs font-semibold text-emerald-400 uppercase tracking-wider">What you all share</h5>
              <BulletList items={summary.summary.similarities} color="emerald" />
            </div>
            <div className="space-y-2">
              <h5 className="text-xs font-semibold text-amber-400 uppercase tracking-wider">Where you differ</h5>
              <BulletList items={summary.summary.differences} color="amber" />
            </div>
          </div>

          <div className="bg-surface-2 rounded-lg p-3 border border-white/[0.04]">
            <p className="text-[11px] text-text-dim uppercase tracking-wider mb-1">AI search query for the group</p>
            <p className="text-sm text-purple-300 font-medium leading-relaxed">&quot;{summary.summary.group_query}&quot;</p>
            {summary.summary.reasoning && (
              <p className="text-[11px] text-text-dim mt-1.5 leading-relaxed">{summary.summary.reasoning}</p>
            )}
          </div>

          {movieResults.length > 0 && (
            <div className="space-y-3">
              <h5 className="text-xs font-semibold text-text-dim uppercase tracking-wider">
                Group suggestions — click to explore, like/dislike to vote
              </h5>
              <MovieGrid movies={movieResults} explanations={{}}
                likedIds={likedIds} dislikedIds={dislikedIds}
                onMovieClick={onMovieClick} onFeedback={onFeedback}
                isLoading={false} isEnriching={false} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}