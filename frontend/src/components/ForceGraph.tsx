'use client';

import { useRef, useEffect } from 'react';
import type { NeighborhoodData, GraphNode, GraphLink } from '@/types';

interface Props {
  data: NeighborhoodData;
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
  Movie: 20,
  Genre: 14,
  Director: 14,
  Actor: 12,
  Keyword: 10,
  Decade: 12,
};

export default function ForceGraph({ data }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!svgRef.current || !containerRef.current || !data.nodes.length) return;

    // Dynamic import D3 to avoid SSR issues
    import('d3').then(d3 => {
      const svg = d3.select(svgRef.current);
      svg.selectAll('*').remove();

      const container = containerRef.current!;
      const width = container.clientWidth;
      const height = container.clientHeight;

      svg.attr('width', width).attr('height', height);

      // Deep copy nodes/links so D3 can mutate them
      const nodes: (GraphNode & { x?: number; y?: number; fx?: number | null; fy?: number | null })[] =
        data.nodes.map(n => ({ ...n }));
      const links: (GraphLink & { source: any; target: any })[] =
        data.links.map(l => ({ ...l }));

      // Fix center node
      const center = nodes.find(n => n.isCenter);
      if (center) {
        center.fx = width / 2;
        center.fy = height / 2;
      }

      const simulation = d3.forceSimulation(nodes as any)
        .force('link', d3.forceLink(links as any).id((d: any) => d.id).distance(80).strength(0.8))
        .force('charge', d3.forceManyBody().strength(-200))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius((d: any) => (NODE_RADIUS[d.type] || 10) + 4));

      // Links
      const link = svg.append('g')
        .selectAll('line')
        .data(links)
        .enter().append('line')
        .attr('stroke', '#ffffff10')
        .attr('stroke-width', 1.5);

      // Node groups
      const node = svg.append('g')
        .selectAll('g')
        .data(nodes)
        .enter().append('g')
        .style('cursor', 'pointer');

      // Drag behavior
      const drag = d3.drag<SVGGElement, any>()
        .on('start', (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart();
          d.fx = d.x;
          d.fy = d.y;
        })
        .on('drag', (event, d) => {
          d.fx = event.x;
          d.fy = event.y;
        })
        .on('end', (event, d) => {
          if (!event.active) simulation.alphaTarget(0);
          if (!d.isCenter) { d.fx = null; d.fy = null; }
        });

      node.call(drag as any);

      // Node circles
      node.append('circle')
        .attr('r', (d: any) => d.isCenter ? 24 : (NODE_RADIUS[d.type] || 10))
        .attr('fill', (d: any) => {
          const c = NODE_COLORS[d.type] || '#666';
          return d.isCenter ? c : c + '30';
        })
        .attr('stroke', (d: any) => NODE_COLORS[d.type] || '#666')
        .attr('stroke-width', (d: any) => d.isCenter ? 2.5 : 1.5)
        .attr('stroke-opacity', 0.6);

      // Node labels
      node.append('text')
        .text((d: any) => {
          const name = d.name || '';
          return name.length > 16 ? name.slice(0, 14) + '…' : name;
        })
        .attr('text-anchor', 'middle')
        .attr('dy', (d: any) => (d.isCenter ? 36 : (NODE_RADIUS[d.type] || 10) + 14))
        .attr('font-size', (d: any) => d.isCenter ? 11 : 9)
        .attr('fill', '#9894a8')
        .attr('font-family', 'DM Sans, system-ui, sans-serif');

      // Type labels (small)
      node.filter((d: any) => !d.isCenter)
        .append('text')
        .text((d: any) => d.type?.[0] || '')
        .attr('text-anchor', 'middle')
        .attr('dy', 4)
        .attr('font-size', 9)
        .attr('font-weight', 600)
        .attr('fill', (d: any) => NODE_COLORS[d.type] || '#666')
        .attr('font-family', 'JetBrains Mono, monospace');

      // Hover tooltip
      const tooltip = d3.select(container).append('div')
        .style('position', 'absolute')
        .style('background', '#1a1a26')
        .style('border', '1px solid rgba(255,255,255,0.1)')
        .style('border-radius', '6px')
        .style('padding', '6px 10px')
        .style('font-size', '11px')
        .style('color', '#e8e6f0')
        .style('pointer-events', 'none')
        .style('opacity', 0)
        .style('z-index', '10')
        .style('font-family', 'DM Sans, system-ui, sans-serif');

      node.on('mouseover', (event: any, d: any) => {
        tooltip
          .html(`<strong>${d.name}</strong><br/><span style="color:#6b6680">${d.type}</span>`)
          .style('opacity', 1)
          .style('left', (event.offsetX + 12) + 'px')
          .style('top', (event.offsetY - 10) + 'px');
      })
      .on('mouseout', () => {
        tooltip.style('opacity', 0);
      });

      simulation.on('tick', () => {
        link
          .attr('x1', (d: any) => d.source.x)
          .attr('y1', (d: any) => d.source.y)
          .attr('x2', (d: any) => d.target.x)
          .attr('y2', (d: any) => d.target.y);

        node.attr('transform', (d: any) => `translate(${d.x},${d.y})`);
      });

      return () => {
        simulation.stop();
        tooltip.remove();
      };
    });
  }, [data]);

  return (
    <div ref={containerRef} className="w-full h-full relative">
      <svg ref={svgRef} className="w-full h-full" />
      {/* Legend */}
      <div className="absolute bottom-2 left-2 flex flex-wrap gap-2">
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <span key={type} className="flex items-center gap-1 text-[9px] text-text-dim">
            <span className="w-2 h-2 rounded-full" style={{ background: color }} />
            {type}
          </span>
        ))}
      </div>
    </div>
  );
}
