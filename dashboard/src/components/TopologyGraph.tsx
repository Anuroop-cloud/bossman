import { useMemo } from "react";
import { ReactFlow, Background, type Node, type Edge, MarkerType } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { Graph } from "../types";
import clsx from "clsx";
import { Cpu, FileText, Box, Users } from "lucide-react";

const NODE_WIDTH = 180;
const NODE_HEIGHT = 60;

function CustomNode({ data }: any) {
  const Icon = data.kind === "supervisor" ? Users : data.kind === "agent" ? Cpu : data.kind === "task" ? FileText : Box;
  
  return (
    <div className={clsx("rf-node", data.kind, data.locked && "locked", data.suspected_deadlock && "deadlock")}>
      <div className="rf-title">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{ width: 24, height: 24, borderRadius: 4, background: 'var(--bg)', display: 'grid', placeItems: 'center', border: '1px solid var(--border)' }}>
            <Icon size={12} className="dim" />
          </div>
          <span style={{ fontWeight: 500 }}>{data.label}</span>
        </div>
        {data.status === "PASS" || data.status === "RUNNING" || data.status === "COMPLETED" ? <div className="dot ok" /> : 
         data.status === "WARN" || data.status === "STUCK" ? <div className="dot warn" /> :
         data.status === "FAIL" || data.status === "FAILED" ? <div className="dot bad" /> :
         data.status === "IDLE" ? <div className="dot info" /> :
         data.kind === "resource" ? (data.locked ? <div className="dot warn" /> : <div className="dot muted" />) :
         null}
      </div>
      <div className="rf-sub">{data.role || data.kind}</div>
    </div>
  );
}

const nodeTypes = {
  custom: CustomNode,
};

export function TopologyGraph({ graph }: { graph: Graph }) {
  const { nodes, edges } = useMemo(() => {
    // Very simple auto-layout for hierarchical topology
    const levelMap = {
      supervisor: 0,
      agent: 1,
      task: 2,
      resource: 3
    };

    const levels: Record<number, any[]> = { 0: [], 1: [], 2: [], 3: [] };
    graph.nodes.forEach(n => {
      const lvl = levelMap[n.kind as keyof typeof levelMap] ?? 4;
      if (levels[lvl]) levels[lvl].push(n);
    });

    const outNodes: Node[] = [];
    
    // Position nodes layer by layer
    Object.keys(levels).forEach((lvlStr) => {
      const lvl = parseInt(lvlStr);
      const row = levels[lvl];
      const spacingX = 220;
      const spacingY = 120;
      const startX = -((row.length - 1) * spacingX) / 2;
      
      row.forEach((n, i) => {
        outNodes.push({
          id: n.id,
          type: 'custom',
          position: { x: startX + i * spacingX, y: lvl * spacingY + 20 },
          data: { ...n }
        });
      });
    });

    const outEdges: Edge[] = graph.edges.map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      animated: e.kind === "works_on" || e.kind === "supervises",
      style: { stroke: e.suspected_deadlock ? 'var(--bad)' : 'var(--border-strong)', strokeWidth: 1.5 },
      markerEnd: { type: MarkerType.ArrowClosed, color: e.suspected_deadlock ? 'var(--bad)' : 'var(--border-strong)' }
    }));

    return { nodes: outNodes, edges: outEdges };
  }, [graph]);

  return (
    <div className="graph" style={{ width: '100%', height: '100%', minHeight: 400 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        minZoom={0.2}
        maxZoom={1.5}
      >
        <Background gap={16} color="var(--border)" size={1} />
      </ReactFlow>
    </div>
  );
}
