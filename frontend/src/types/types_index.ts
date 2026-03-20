// Re-export from api for backward compatibility
import type { SearchResult, FeedbackItem } from '@/lib/api';
export type Movie = SearchResult;
export type { FeedbackItem };

export interface SuggestedFilters {
  genres?: string[];
  decades?: string[];
}

export interface GraphNode {
  id: string;
  name: string;
  type: string;
  isCenter: boolean;
}

export interface GraphLink {
  source: string;
  target: string;
  type: string;
}

export interface NeighborhoodData {
  nodes: GraphNode[];
  links: GraphLink[];
}
