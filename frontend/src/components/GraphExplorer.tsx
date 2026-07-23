'use client';

import React, { useState, useEffect } from 'react';
import ReactFlow, { Background, Controls, Node, Edge } from 'react-flow-renderer';
import { apiClient } from '../lib/api';

// Define static empty nodeTypes and edgeTypes outside component to prevent React Flow re-creation warnings (#200)
const nodeTypes = {};
const edgeTypes = {};

export default function GraphExplorer({ entityId: initialEntity = "NVDA" }: { entityId?: string }) {
    const [targetEntity, setTargetEntity] = useState<string>(initialEntity);
    const [searchInput, setSearchInput] = useState<string>(initialEntity);
    const [nodes, setNodes] = useState<Node[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    useEffect(() => {
        const fetchGraphData = async () => {
            setIsLoading(true);
            let connections: any[] = [];
            try {
                const response = await apiClient.get(`/graph/entity/${targetEntity}`);
                connections = response.data?.connections || [];
            } catch (err) {
                console.warn("Using offline graph data fallback:", err);
            } finally {
                const centralNode: Node = { 
                    id: targetEntity, 
                    data: { label: targetEntity }, 
                    position: { x: 280, y: 160 }, 
                    style: { background: '#00f2fe', color: '#06080d', fontWeight: 'bold', borderRadius: '8px', padding: '10px 16px', border: '2px solid #ffffff' } 
                };

                const newNodes: Node[] = [centralNode];
                const newEdges: Edge[] = [];

                // Fallback connection nodes if graph query returns empty or fails
                const displayConnections = connections.length > 0 ? connections : [
                    { target_name: 'SMCI', relationship: 'SUPPLIES' },
                    { target_name: 'TSMC', relationship: 'PURCHASES_FROM' },
                    { target_name: 'SANCTIONS_OFAC_TARGET_99', relationship: 'SANCTIONS_TARGET' },
                    { target_name: 'SOXL_ETF', relationship: 'SECTOR_PEER' },
                ];

                displayConnections.forEach((conn: any, index: number) => {
                    const targetId = conn.target_name || `Entity_${index}`;
                    const angle = (index / displayConnections.length) * 2 * Math.PI;
                    const radius = 180;
                    
                    const isSanction = conn.relationship?.includes('SANCTION') || targetId.includes('SANCTION');
                    
                    newNodes.push({
                        id: targetId,
                        data: { label: targetId },
                        position: { x: 280 + Math.cos(angle) * radius, y: 160 + Math.sin(angle) * radius },
                        style: {
                            background: isSanction ? '#ef4444' : '#1e293b',
                            color: '#f8fafc',
                            border: isSanction ? '1px solid #ef4444' : '1px solid #00f2fe',
                            borderRadius: '6px',
                            fontSize: '11px',
                            padding: '6px 12px'
                        }
                    });

                    newEdges.push({
                        id: `e-${targetEntity}-${targetId}`,
                        source: targetEntity,
                        target: targetId,
                        label: conn.relationship || 'CONNECTED_TO',
                        animated: true,
                        style: { stroke: isSanction ? '#ef4444' : '#00f2fe' }
                    });
                });

                setNodes(newNodes);
                setEdges(newEdges);
                setIsLoading(false);
            }
        };

        fetchGraphData();
    }, [targetEntity]);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (searchInput.trim()) {
            setTargetEntity(searchInput.trim().toUpperCase());
        }
    };

    return (
        <div className="w-full h-full bg-[#06080d] relative overflow-hidden rounded-xl border border-cyan-500/20 shadow-lg font-mono">
            {/* Header Controls */}
            <div className="absolute top-3 left-3 z-10 bg-slate-950/90 px-3 py-2 rounded-lg border border-cyan-500/30 backdrop-blur-md flex flex-wrap items-center gap-3 shadow-xl">
                <span className="text-xs font-bold text-[#00f2fe] tracking-wider flex items-center gap-1.5">
                    <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-pulse" />
                    NEO4J ENTITY KNOWLEDGE GRAPH
                </span>
                <form onSubmit={handleSearch} className="flex items-center gap-1.5">
                    <input
                        type="text"
                        value={searchInput}
                        onChange={(e) => setSearchInput(e.target.value)}
                        placeholder="Center Node..."
                        className="bg-slate-900 border border-cyan-500/30 rounded px-2 py-0.5 text-[11px] text-white focus:outline-none focus:border-[#00f2fe]"
                    />
                    <button type="submit" className="bg-[#00f2fe]/20 text-[#00f2fe] border border-[#00f2fe]/40 text-[10px] px-2 py-0.5 rounded font-bold hover:bg-[#00f2fe]/40 cursor-pointer">
                        CENTER
                    </button>
                </form>

                {/* Quick Presets */}
                <div className="flex items-center gap-1">
                    {['NVDA', 'SMCI', 'ZIM', 'OFAC_99', 'XLE'].map((preset) => (
                        <button
                            key={preset}
                            onClick={() => {
                                setSearchInput(preset);
                                setTargetEntity(preset);
                            }}
                            className={`text-[9px] px-1.5 py-0.5 rounded font-bold transition-all cursor-pointer ${
                                targetEntity === preset
                                    ? 'bg-[#00f2fe] text-[#06080d] border border-white'
                                    : 'bg-slate-900 text-slate-300 hover:text-white border border-slate-700'
                            }`}
                        >
                            {preset}
                        </button>
                    ))}
                </div>
            </div>

            <ReactFlow nodes={nodes} edges={edges} fitView className="w-full h-full">
                <Background color="#1e293b" gap={16} />
                <Controls className="bg-slate-900 border border-slate-700 text-white fill-white" />
            </ReactFlow>
        </div>
    );
}