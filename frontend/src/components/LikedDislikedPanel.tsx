'use client';

import { useEffect } from 'react';
import type { FeedbackItem } from '@/types';

interface Props {
  liked: FeedbackItem[];
  disliked: FeedbackItem[];
  onClose: () => void;
  onRemoveLike: (id: number, title: string) => void;
  onRemoveDislike: (id: number, title: string) => void;
}

export default function LikedDislikedPanel({
  liked, disliked, onClose, onRemoveLike, onRemoveDislike,
}: Props) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 animate-fade-in"
        onClick={onClose}
      />

      {/* Panel */}
      <div className="fixed right-0 top-0 bottom-0 w-full max-w-[400px] bg-surface-1 border-l border-white/[0.06] z-50 overflow-y-auto animate-slide-in shadow-2xl">
        {/* Header */}
        <div className="sticky top-0 bg-surface-1/95 backdrop-blur-sm border-b border-white/[0.06] px-6 py-4 flex items-center justify-between z-10">
          <h2 className="text-lg font-bold text-text-primary">Your preferences</h2>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-full bg-surface-2 border border-white/[0.08] flex items-center justify-center text-text-dim hover:text-text-primary hover:border-white/[0.16] transition-all"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="p-6 space-y-6">
          {/* Liked movies */}
          <div>
            <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5 mb-3">
              <span className="text-sage">♥</span>
              Liked movies ({liked.length})
            </h3>
            {liked.length === 0 ? (
              <p className="text-sm text-text-dim py-2">No liked movies yet. Like movies from search results to personalize your recommendations.</p>
            ) : (
              <div className="space-y-1">
                {liked.map(item => (
                  <div
                    key={item.id}
                    className="flex items-center justify-between py-2 px-3 rounded-lg hover:bg-surface-2/50 group transition-colors"
                  >
                    <span className="text-sm text-text-primary truncate mr-3">{item.title}</span>
                    <button
                      onClick={() => onRemoveLike(item.id, item.title)}
                      className="flex-shrink-0 w-6 h-6 rounded-full bg-surface-2 text-text-dim hover:text-ember hover:bg-ember/10 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all"
                      title="Remove like"
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

          {/* Disliked movies */}
          <div>
            <h3 className="text-xs font-semibold text-text-dim uppercase tracking-wider flex items-center gap-1.5 mb-3">
              <span className="text-ember">✕</span>
              Disliked movies ({disliked.length})
            </h3>
            {disliked.length === 0 ? (
              <p className="text-sm text-text-dim py-2">No disliked movies yet.</p>
            ) : (
              <div className="space-y-1">
                {disliked.map(item => (
                  <div
                    key={item.id}
                    className="flex items-center justify-between py-2 px-3 rounded-lg hover:bg-surface-2/50 group transition-colors"
                  >
                    <span className="text-sm text-text-primary truncate mr-3">{item.title}</span>
                    <button
                      onClick={() => onRemoveDislike(item.id, item.title)}
                      className="flex-shrink-0 w-6 h-6 rounded-full bg-surface-2 text-text-dim hover:text-sage hover:bg-sage/10 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all"
                      title="Remove dislike"
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

          {/* Info */}
          <div className="bg-surface-2/50 border border-white/[0.04] rounded-lg p-3">
            <p className="text-[11px] text-text-dim leading-relaxed">
              Your liked and disliked movies influence future search results.
              Adjust the <strong className="text-text-secondary">preference influence</strong> slider
              in settings to control how strongly your history affects recommendations.
            </p>
          </div>
        </div>
      </div>
    </>
  );
}
