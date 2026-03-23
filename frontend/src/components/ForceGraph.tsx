'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import type { NeighborhoodData } from '@/types';

interface Props {
  data: NeighborhoodData;
  onAddGenreFilter?: (genre: string) => void;
  onAddGenreMixer?: (genre: string) => void;
  onGraphSearch?: (nodeType: string, nodeName: string) => void;
}

const NODE_COLORS: Record<string, string> = {
  Movie: '#6d5aff',
  Genre: '#34d399',
  Director: '#fbbf24',
  Actor: '#f472b6',
  Keyword: '#60a5fa',
  Decade: '#fb923c',
};

const NODE_RADIUS: Record<string, number> = {
  Movie: 22,
  Genre: 14,
  Director: 14,
  Actor: 11,
  Keyword: 10,
  Decade: 12,
};

const TOGGLEABLE_TYPES = ['Genre', 'Director', 'Actor', 'Keyword', 'Decade'];
const DEFAULT_ON = new Set(['Genre', 'Director', 'Actor', 'Keyword', 'Decade']);

export default function ForceGraph({ data, onAddGenreFilter, onAddGenreMixer, onGraphSearch }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [activeTypes, setActiveTypes] = useState<Set<string>>(new Set(DEFAULT_ON));
  const [hoveredNode, setHoveredNode] = useState<{ name: string; type: string; x: number; y: number } | null>(null);
  const zoomRef = useRef<any>(null);
  const simRef = useRef<any>(null);

  const toggleType = useCallback((type: string) => {
    setActiveTypes(prev => {
      const next = new Set(prev);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  }, []);

  const handleReset = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    import('d3').then(d3 => {
      d3.select(svgRef.current).transition().duration(400).call(zoomRef.current.transform, d3.zoomIdentity);
    });
  }, []);

  useEffect(() => {
    if (!svgRef.current || !containerRef.current || !data.nodes.length) return;
    let cancelled = false;
    // Stop any previous simulation
    if (simRef.current) { simRef.current.stop(); simRef.current = null; }

    import('d3').then(d3 => {
      if (cancelled || !svgRef.current || !containerRef.current) return;
      const svg = d3.select(svgRef.current);
      svg.selectAll('*').remove();
      setHoveredNode(null);

      const container = containerRef.current!;
      const width = container.clientWidth || 500;
      const height = container.clientHeight || 400;
      svg.attr('width', width).attr('height', height);

      // Filter nodes by active types (always keep Movie center)
      const visibleNodes = data.nodes.filter(n => n.isCenter || activeTypes.has(n.type));
      const visibleIds = new Set(visibleNodes.map(n => n.id));

      // Always normalize source/target to string IDs (D3 mutates these to objects)
      const toId = (v: any): string => (typeof v === 'object' && v !== null && v.id) ? v.id : String(v);
      const visibleLinks = data.links
        .filter(l => visibleIds.has(toId(l.source)) && visibleIds.has(toId(l.target)));

      // Fresh deep copies — no stale D3 positions
      const nodes: any[] = visibleNodes.map(n => ({ id: n.id, name: n.name, type: n.type, isCenter: n.isCenter }));
      const links: any[] = visibleLinks.map(l => ({ source: toId(l.source), target: toId(l.target), type: l.type }));

      if (nodes.length === 0) return;

      const center = nodes.find(n => n.isCenter);
      if (center) { center.fx = width / 2; center.fy = height / 2; }

      // Zoom
      const g = svg.append('g');
      const zoom = d3.zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.3, 3])
        .on('zoom', (event: any) => g.attr('transform', event.transform));
      svg.call(zoom);
      zoomRef.current = zoom;

      // Simulation — scale forces to node count
      const nCount = nodes.length;
      const chargeStr = Math.min(-200, -80 * Math.sqrt(nCount));
      const linkDist = Math.max(60, 120 - nCount);

      const sim = d3.forceSimulation(nodes)
        .force('link', d3.forceLink(links).id((d: any) => d.id).distance(linkDist).strength(0.3))
        .force('charge', d3.forceManyBody().strength(chargeStr))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius((d: any) => (NODE_RADIUS[d.type] || 10) + 10))
        .force('x', d3.forceX(width / 2).strength(0.03))
        .force('y', d3.forceY(height / 2).strength(0.03))
        .alpha(1)
        .alphaDecay(0.02);
      simRef.current = sim;

      // Links — store selection directly
      const linkG = g.append('g');
      const link = linkG.selectAll('line').data(links).enter().append('line')
        .attr('stroke', '#ffffff10').attr('stroke-width', 1.5);

      // Node groups
      const node = g.append('g').selectAll('g').data(nodes).enter().append('g')
        .style('cursor', 'pointer');

      // Drag
      const drag = d3.drag<SVGGElement, any>()
        .on('start', (event, d) => { if (!event.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
        .on('end', (event, d) => { if (!event.active) sim.alphaTarget(0); if (!d.isCenter) { d.fx = null; d.fy = null; } });
      node.call(drag as any);

      // Circles
      node.append('circle')
        .attr('r', (d: any) => d.isCenter ? 24 : (NODE_RADIUS[d.type] || 10))
        .attr('fill', (d: any) => { const c = NODE_COLORS[d.type] || '#666'; return d.isCenter ? c : c + '25'; })
        .attr('stroke', (d: any) => NODE_COLORS[d.type] || '#666')
        .attr('stroke-width', (d: any) => d.isCenter ? 2.5 : 1.5)
        .attr('stroke-opacity', 0.6);

      // Labels
      node.append('text')
        .text((d: any) => { const n = d.name || ''; return n.length > 18 ? n.slice(0, 16) + '…' : n; })
        .attr('text-anchor', 'middle')
        .attr('dy', (d: any) => d.isCenter ? 36 : (NODE_RADIUS[d.type] || 10) + 14)
        .attr('font-size', (d: any) => d.isCenter ? 11 : 9)
        .attr('fill', '#9894a8')
        .attr('font-family', 'DM Sans, system-ui, sans-serif');

      // Type letter inside node
      node.filter((d: any) => !d.isCenter).append('text')
        .text((d: any) => d.type?.[0] || '')
        .attr('text-anchor', 'middle').attr('dy', 4).attr('font-size', 9).attr('font-weight', 600)
        .attr('fill', (d: any) => NODE_COLORS[d.type] || '#666')
        .attr('font-family', 'JetBrains Mono, monospace');

      // Click — show action popup via React state
      node.on('click', function (event: any, d: any) {
        event.stopPropagation();
        if (d.isCenter) return;
        const rect = container.getBoundingClientRect();
        const svgRect = svgRef.current!.getBoundingClientRect();
        const transform = d3.zoomTransform(svgRef.current!);
        const screenX = transform.applyX(d.x) + svgRect.left - rect.left;
        const screenY = transform.applyY(d.y) + svgRect.top - rect.top;
        setHoveredNode(prev =>
          prev?.name === d.name ? null : { name: d.name, type: d.type, x: screenX, y: screenY }
        );
      });

      // Highlight on hover (visual only)
      node.on('mouseover', function (event: any, d: any) {
        if (d.isCenter) return;
        d3.select(this).select('circle')
          .transition().duration(150)
          .attr('stroke-width', 3).attr('stroke-opacity', 1);
      })
      .on('mouseout', function () {
        d3.select(this).select('circle')
          .transition().duration(150)
          .attr('stroke-width', 1.5).attr('stroke-opacity', 0.6);
      });

      // Click on background to dismiss popup
      svg.on('click', () => setHoveredNode(null));

      // Tick — use stored link selection
      sim.on('tick', () => {
        link
          .attr('x1', (d: any) => d.source?.x ?? 0)
          .attr('y1', (d: any) => d.source?.y ?? 0)
          .attr('x2', (d: any) => d.target?.x ?? 0)
          .attr('y2', (d: any) => d.target?.y ?? 0);
        node.attr('transform', (d: any) => `translate(${d.x ?? 0},${d.y ?? 0})`);
      });
    });

    return () => {
      cancelled = true;
      if (simRef.current) { simRef.current.stop(); simRef.current = null; }
    };
  }, [data, activeTypes]);

  return (
    <div ref={containerRef} className="w-full h-full relative" style={{ minHeight: 400 }}>
      {/* Toggle chips */}
      <div className="absolute top-2 left-2 z-10 flex flex-wrap gap-1">
        {TOGGLEABLE_TYPES.map(type => {
          const isOn = activeTypes.has(type);
          const count = data.nodes.filter(n => n.type === type).length;
          if (count === 0) return null;
          return (
            <button key={type} onClick={() => toggleType(type)}
              className={`flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium border transition-all ${
                isOn
                  ? 'border-white/20 bg-surface-2 text-text-primary'
                  : 'border-white/[0.06] bg-surface-1/50 text-text-dim/50'
              }`}>
              <span className="w-1.5 h-1.5 rounded-full" style={{ background: isOn ? NODE_COLORS[type] : '#444' }} />
              {type} <span className="opacity-60">{count}</span>
            </button>
          );
        })}
      </div>

      {/* Reset zoom button */}
      <button onClick={handleReset}
        className="absolute top-2 right-2 z-10 px-2 py-1 rounded-md bg-surface-2/80 border border-white/[0.06] text-[10px] text-text-dim hover:text-text-primary transition-colors">
        Reset view
      </button>

      {/* SVG */}
      <svg ref={svgRef} className="w-full h-full" />

      {/* Interactive hover popup */}
      {hoveredNode && (
        <div
          className="absolute z-20 bg-surface-2 border border-white/[0.1] rounded-lg shadow-xl p-2 space-y-1 animate-fade-in"
          style={{ left: hoveredNode.x + 16, top: hoveredNode.y - 20, minWidth: 160 }}
          onClick={e => e.stopPropagation()}
        >
          <div className="flex items-center gap-1.5 px-1 pb-1 border-b border-white/[0.06]">
            <span className="w-2 h-2 rounded-full" style={{ background: NODE_COLORS[hoveredNode.type] }} />
            <span className="text-xs font-medium text-text-primary truncate">{hoveredNode.name}</span>
            <span className="text-[9px] text-text-dim ml-auto">{hoveredNode.type}</span>
          </div>

          {hoveredNode.type === 'Genre' ? (
            <>
              <button onClick={() => { onAddGenreFilter?.(hoveredNode.name); setHoveredNode(null); }}
                className="w-full text-left px-2 py-1.5 rounded-md text-[11px] text-text-secondary hover:bg-white/[0.04] transition-colors">
                Add as hard filter
              </button>
              <button onClick={() => { onAddGenreMixer?.(hoveredNode.name); setHoveredNode(null); }}
                className="w-full text-left px-2 py-1.5 rounded-md text-[11px] text-text-secondary hover:bg-white/[0.04] transition-colors">
                Add to genre mixer
              </button>
              <button onClick={() => { onGraphSearch?.(hoveredNode.type, hoveredNode.name); setHoveredNode(null); }}
                className="w-full text-left px-2 py-1.5 rounded-md text-[11px] text-accent hover:bg-accent/10 transition-colors">
                Search for {hoveredNode.name} movies
              </button>
            </>
          ) : (
            <button onClick={() => { onGraphSearch?.(hoveredNode.type, hoveredNode.name); setHoveredNode(null); }}
              className="w-full text-left px-2 py-1.5 rounded-md text-[11px] text-accent hover:bg-accent/10 transition-colors">
              Search for movies with {hoveredNode.name}
            </button>
          )}
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-2 right-2 flex flex-wrap gap-2">
        <span className="text-[9px] text-text-dim/40">scroll to zoom · drag to pan</span>
      </div>
    </div>
  );
}