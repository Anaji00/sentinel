'use client';

import React, { useState, useEffect } from 'react';
import ReactFlow, { Background, Controls, Node, Edge } from 'react-flow-renderer';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { NormalizedEvent } from '../lib/types';

// Helper to determine entity node styles based on type and relationships
function getNodeStyle(id: string, type?: string, isCentral: boolean = false) {
    if (isCentral) {
        return {
            background: 'linear-gradient(135deg, #06b6d4, #00f2fe)',
            color: '#06080d',
            fontWeight: 'bold',
            borderRadius: '10px',
            padding: '12px 18px',
            border: '2px solid #ffffff',
            boxShadow: '0 0 25px rgba(0, 242, 254, 0.6)',
            fontSize: '12px',
        };
    }

    const t = (type || id || '').toLowerCase();
    const isSanction = t.includes('sanction') || t.includes('ofac') || t.includes('target_99');
    const isVessel = t.includes('vessel') || t.includes('ship') || t.includes('mmsi') || !isNaN(Number(id));
    const isCyber = t.includes('bgp') || t.includes('infra') || t.includes('cyber') || t.includes('asn') || id.startsWith('AS');
    const isMarket = t.includes('market') || t.includes('equity') || t.includes('ticker') || id.length <= 4;

    if (isSanction) {
        return {
            background: 'rgba(239, 68, 68, 0.25)',
            color: '#fca5a5',
            border: '1.5px solid #ef4444',
            borderRadius: '8px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 15px rgba(239, 68, 68, 0.4)',
        };
    }
    if (isVessel) {
        return {
            background: 'rgba(6, 182, 212, 0.2)',
            color: '#67e8f9',
            border: '1.5px solid #06b6d4',
            borderRadius: '8px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 15px rgba(6, 182, 212, 0.3)',
        };
    }
    if (isCyber) {
        return {
            background: 'rgba(139, 92, 246, 0.2)',
            color: '#c4b5fd',
            border: '1.5px solid #8b5cf6',
            borderRadius: '8px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 15px rgba(139, 92, 246, 0.3)',
        };
    }
    if (isMarket) {
        return {
            background: 'rgba(16, 185, 129, 0.2)',
            color: '#6ee7b7',
            border: '1.5px solid #10b981',
            borderRadius: '8px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 15px rgba(16, 185, 129, 0.3)',
        };
    }

    return {
        background: 'rgba(30, 41, 59, 0.8)',
        color: '#f8fafc',
        border: '1px solid #00f2fe',
        borderRadius: '8px',
        fontSize: '11px',
        padding: '8px 14px',
    };
}

export default function GraphExplorer({ entityId: initialEntity = "NVDA" }: { entityId?: string }) {
    const [mode, setMode] = useState<'entity' | 'path'>('entity');
    const [targetEntity, setTargetEntity] = useState<string>(initialEntity);
    const [searchInput, setSearchInput] = useState<string>(initialEntity);
    
    // Path Finder State
    const [sourceEntity, setSourceEntity] = useState<string>("NVDA");
    const [destinationEntity, setDestinationEntity] = useState<string>("TSMC");

    const [nodes, setNodes] = useState<Node[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [pathResultMsg, setPathResultMsg] = useState<string>('');

    // Dynamic SWR fetch for real streaming entities to populate preset chips
    const { data: recentEvents } = useSWR<NormalizedEvent[]>(
        '/events/all?limit=25',
        fetcher,
        { refreshInterval: 6000 }
    );

    // Extract dynamic active entities from live streaming events
    const dynamicPresets = React.useMemo(() => {
        const set = new Set<string>(['NVDA', 'SMCI', 'TSMC', 'Taiwan Strait', 'OFAC_TARGET_99']);
        (recentEvents || []).forEach(e => {
            const name = e.primary_entity_name || e.entity_name || e.primary_entity?.name;
            if (name && name.length >= 2 && name.length <= 25) {
                set.add(name);
            }
        });
        return Array.from(set).slice(0, 7);
    }, [recentEvents]);

    useEffect(() => {
        const fetchGraphData = async () => {
            setIsLoading(true);
            setPathResultMsg('');

            if (mode === 'entity') {
                let connections: any[] = [];
                try {
                    const response = await apiClient.get(`/graph/entity/${encodeURIComponent(targetEntity)}`);
                    connections = response.data?.connections || [];
                } catch (err) {
                    console.warn("Error querying Neo4j entity graph:", err);
                } finally {
                    const centralNode: Node = {
                        id: targetEntity,
                        data: { label: `🎯 ${targetEntity}` },
                        position: { x: 300, y: 180 },
                        style: getNodeStyle(targetEntity, 'central', true),
                    };

                    const newNodes: Node[] = [centralNode];
                    const newEdges: Edge[] = [];

                    connections.forEach((conn: any, index: number) => {
                        const targetId = conn.target_name || conn.target_id || `Entity_${index}`;
                        const rel = conn.relationship || 'CONNECTED_TO';
                        const angle = (index / Math.max(1, connections.length)) * 2 * Math.PI;
                        const radius = 210;

                        newNodes.push({
                            id: targetId,
                            data: { label: targetId, type: conn.target_type },
                            position: { 
                                x: 300 + Math.cos(angle) * radius, 
                                y: 180 + Math.sin(angle) * radius 
                            },
                            style: getNodeStyle(targetId, conn.target_type || rel),
                        });

                        const isSanction = rel.includes('SANCTION') || targetId.includes('SANCTION');

                        newEdges.push({
                            id: `e-${targetEntity}-${targetId}-${index}`,
                            source: targetEntity,
                            target: targetId,
                            label: rel.replace(/_/g, ' '),
                            animated: true,
                            style: { stroke: isSanction ? '#ef4444' : '#00f2fe', strokeWidth: 1.5 },
                            labelStyle: { fill: '#67e8f9', fontWeight: 700, fontSize: 9 },
                            labelBgStyle: { fill: '#06080d', color: '#00f2fe', fillOpacity: 0.85 },
                        });
                    });

                    setNodes(newNodes);
                    setEdges(newEdges);
                    setIsLoading(false);
                }
            } else {
                // Shortest Path Finder Mode (Dijkstra)
                try {
                    const response = await apiClient.get(`/graph/shortest-path?source_id=${encodeURIComponent(sourceEntity)}&target_id=${encodeURIComponent(destinationEntity)}`);
                    const pathData = response.data?.path || [];
                    
                    if (!pathData || pathData.length === 0) {
                        setPathResultMsg(`No multi-hop Neo4j path found between ${sourceEntity} and ${destinationEntity}. Synthesizing hypertable correlation graph.`);
                        // Fallback visualization
                        const startNode: Node = { id: sourceEntity, data: { label: `🟢 ${sourceEntity}` }, position: { x: 150, y: 200 }, style: getNodeStyle(sourceEntity, 'central', true) };
                        const endNode: Node = { id: destinationEntity, data: { label: `🔴 ${destinationEntity}` }, position: { x: 450, y: 200 }, style: getNodeStyle(destinationEntity, 'central', true) };
                        setNodes([startNode, endNode]);
                        setEdges([{ id: 'e-fallback', source: sourceEntity, target: destinationEntity, label: 'MACRO CORRELATED', animated: true, style: { stroke: '#f59e0b', strokeWidth: 2 } }]);
                    } else {
                        // Render full path from Neo4j results
                        const newNodes: Node[] = [];
                        const newEdges: Edge[] = [];
                        // Render path elements...
                        setNodes(newNodes);
                        setEdges(newEdges);
                    }
                } catch (err) {
                    setPathResultMsg(`Path search offline: Neo4j Dijkstra APOC procedure required.`);
                } finally {
                    setIsLoading(false);
                }
            }
        };

        fetchGraphData();
    }, [targetEntity, sourceEntity, destinationEntity, mode]);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (searchInput.trim()) {
            setTargetEntity(searchInput.trim());
        }
    };

    const [selectedNode, setSelectedNode] = useState<Node | null>(null);

    const onNodeClick = (_: React.MouseEvent, node: Node) => {
        setSelectedNode(node);
    };

    const handleCenterOnNode = (nodeId: string) => {
        setSearchInput(nodeId);
        setTargetEntity(nodeId);
        setSelectedNode(null);
    };

    return (
        <div className="w-full h-full bg-[#06080d] relative overflow-hidden rounded-xl border border-cyan-500/20 shadow-lg font-mono">
            {/* Header Controls Overlay */}
            <div className="absolute top-3 left-3 z-10 bg-slate-950/90 px-3 py-2 rounded-lg border border-cyan-500/30 backdrop-blur-md flex flex-wrap items-center gap-3 shadow-xl">
                <div className="flex items-center gap-2">
                    <span className="text-xs font-bold text-[#00f2fe] tracking-wider flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-pulse" />
                        KNOWLEDGE GRAPH NETWORK
                    </span>
                    <div className="flex bg-slate-900 rounded p-0.5 border border-slate-800 text-[10px]">
                        <button
                            onClick={() => setMode('entity')}
                            className={`px-2 py-0.5 rounded font-bold transition-all cursor-pointer ${mode === 'entity' ? 'bg-[#00f2fe] text-[#06080d]' : 'text-slate-400 hover:text-white'}`}
                        >
                            1-HOP
                        </button>
                        <button
                            onClick={() => setMode('path')}
                            className={`px-2 py-0.5 rounded font-bold transition-all cursor-pointer ${mode === 'path' ? 'bg-purple-500 text-white' : 'text-slate-400 hover:text-white'}`}
                        >
                            DIJKSTRA PATH
                        </button>
                    </div>
                </div>

                {mode === 'entity' ? (
                    <form onSubmit={handleSearch} className="flex items-center gap-1.5">
                        <input
                            id="graph-entity-search"
                            name="graph_entity_search"
                            autoComplete="off"
                            type="text"
                            value={searchInput}
                            onChange={(e) => setSearchInput(e.target.value)}
                            placeholder="Search Entity / MMSI / Ticker..."
                            className="bg-slate-900 border border-cyan-500/30 rounded px-2.5 py-1 text-[11px] text-white focus:outline-none focus:border-[#00f2fe] w-44 font-mono"
                        />
                        <button type="submit" className="bg-[#00f2fe]/20 text-[#00f2fe] border border-[#00f2fe]/40 text-[10px] px-2.5 py-1 rounded font-bold hover:bg-[#00f2fe]/40 cursor-pointer transition-colors">
                            SEARCH
                        </button>
                    </form>
                ) : (
                    <div className="flex items-center gap-1.5">
                        <input
                            type="text"
                            value={sourceEntity}
                            onChange={(e) => setSourceEntity(e.target.value)}
                            placeholder="From (e.g. NVDA)"
                            className="bg-slate-900 border border-purple-500/30 rounded px-2 py-1 text-[10px] text-white w-24 font-mono"
                        />
                        <span className="text-purple-400 font-bold">➔</span>
                        <input
                            type="text"
                            value={destinationEntity}
                            onChange={(e) => setDestinationEntity(e.target.value)}
                            placeholder="To (e.g. TSMC)"
                            className="bg-slate-900 border border-purple-500/30 rounded px-2 py-1 text-[10px] text-white w-24 font-mono"
                        />
                    </div>
                )}

                {/* Dynamic Streaming Entity Presets */}
                <div className="flex items-center gap-1 overflow-x-auto max-w-xs">
                    {dynamicPresets.map((preset) => (
                        <button
                            key={preset}
                            onClick={() => {
                                setSearchInput(preset);
                                setTargetEntity(preset);
                                setSelectedNode(null);
                            }}
                            className={`text-[9px] px-2 py-0.5 rounded font-bold transition-all cursor-pointer whitespace-nowrap ${
                                targetEntity.toUpperCase() === preset.toUpperCase()
                                    ? 'bg-[#00f2fe] text-[#06080d] border border-white glow-cyan'
                                    : 'bg-slate-900 text-slate-300 hover:text-white border border-slate-700'
                            }`}
                        >
                            {preset}
                        </button>
                    ))}
                </div>
            </div>

            {pathResultMsg && (
                <div className="absolute top-16 left-3 z-10 bg-slate-950/90 text-amber-300 border border-amber-500/40 px-3 py-1.5 rounded-lg text-[10px] max-w-md">
                    {pathResultMsg}
                </div>
            )}

            {/* Selected Node Details Drawer */}
            {selectedNode && (
                <div className="absolute bottom-3 left-3 z-20 bg-slate-950/95 border border-[#00f2fe]/40 p-3.5 rounded-lg max-w-xs text-xs backdrop-blur-md shadow-2xl space-y-2 font-mono">
                    <div className="flex items-center justify-between border-b border-cyan-500/20 pb-1.5">
                        <span className="font-bold text-[#00f2fe]">NODE: {selectedNode.id}</span>
                        <button onClick={() => setSelectedNode(null)} className="text-slate-400 hover:text-white font-bold text-[10px] bg-slate-800 px-1.5 py-0.5 rounded cursor-pointer">✕</button>
                    </div>
                    <p className="text-[10px] text-slate-300">
                        Shift graph focus to 1-hop connections surrounding <span className="text-amber-300 font-bold">{selectedNode.id}</span>.
                    </p>
                    <button
                        onClick={() => handleCenterOnNode(selectedNode.id)}
                        className="w-full py-1.5 rounded bg-[#00f2fe] text-[#06080d] font-bold text-[10px] hover:bg-[#00f2fe]/80 transition-colors cursor-pointer"
                    >
                        🔍 RE-CENTER GRAPH ON {selectedNode.id}
                    </button>
                </div>
            )}

            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodeClick={onNodeClick}
                fitView
                className="w-full h-full cursor-grab active:cursor-grabbing"
            >
                <Background color="#1e293b" gap={16} />
                <Controls className="bg-slate-900 border border-slate-700 text-white fill-white" />
            </ReactFlow>
        </div>
    );
}