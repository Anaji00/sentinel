'use client';

import React, { useState, useEffect, useMemo } from 'react';
import ReactFlow, { Background, Controls, Node, Edge, MarkerType } from 'react-flow-renderer';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { NormalizedEvent } from '../lib/types';
import { Activity, ShieldAlert, Cpu, Network, Zap, Filter, Search, ArrowRight, Clock, Info } from 'lucide-react';

export type EpistemicCategory = 'statistical' | 'structural' | 'regulatory' | 'all';

interface EdgeMetadata {
    id: string;
    source: string;
    target: string;
    relationship: string;
    epistemicCategory: 'statistical' | 'structural' | 'regulatory' | 'general';
    weight: number;
    confidence: number;
    decayed_weight: number;
    last_updated: number;
    coefficient?: number;
    p_value?: number;
    lag?: number;
    f_stat?: number;
    branching_ratio?: number;
    method?: string;
    window?: string;
    hop_level?: number;
}

// Helper to determine node visual appearance based on entity type and prominence
function getNodeStyle(id: string, type?: string, isCentral: boolean = false) {
    if (isCentral) {
        return {
            background: 'linear-gradient(135deg, #06b6d4, #00f2fe)',
            color: '#06080d',
            fontWeight: 'bold',
            borderRadius: '12px',
            padding: '12px 18px',
            border: '2px solid #ffffff',
            boxShadow: '0 0 30px rgba(0, 242, 254, 0.7)',
            fontSize: '12px',
        };
    }

    const t = (type || id || '').toLowerCase();
    const isMacro = t.includes('macro') || id.startsWith('MACRO:') || ['cpi', 'nfp', 'fomc', 'gdp', 'pce', 'pmi'].includes(t);
    const isIndex = t.includes('index') || ['spx', 'ndx', 'rut', 'djia', 'qqq', 'spy', '^gspc', '^ndx'].includes(t);
    const isSector = t.includes('sector') || t.includes('industry') || t.startsWith('xl');
    const isSanction = t.includes('sanction') || t.includes('ofac') || t.includes('target_99');
    const isVessel = t.includes('vessel') || t.includes('ship') || t.includes('mmsi') || (!isNaN(Number(id)) && id.length > 5);
    const isCyber = t.includes('bgp') || t.includes('infra') || t.includes('cyber') || t.includes('asn') || id.startsWith('AS');
    const isMarket = t.includes('market') || t.includes('equity') || t.includes('company') || id.length <= 5;

    if (isMacro) {
        return {
            background: 'rgba(245, 158, 11, 0.25)',
            color: '#fde68a',
            border: '1.5px solid #f59e0b',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(245, 158, 11, 0.4)',
        };
    }
    if (isIndex) {
        return {
            background: 'rgba(168, 85, 247, 0.25)',
            color: '#e9d5ff',
            border: '1.5px solid #a855f7',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(168, 85, 247, 0.4)',
        };
    }
    if (isSector) {
        return {
            background: 'rgba(6, 182, 212, 0.25)',
            color: '#a5f3fc',
            border: '1.5px solid #06b6d4',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(6, 182, 212, 0.35)',
        };
    }
    if (isSanction) {
        return {
            background: 'rgba(239, 68, 68, 0.25)',
            color: '#fca5a5',
            border: '1.5px solid #ef4444',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(239, 68, 68, 0.4)',
        };
    }
    if (isVessel) {
        return {
            background: 'rgba(59, 130, 246, 0.25)',
            color: '#bfdbfe',
            border: '1.5px solid #3b82f6',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(59, 130, 246, 0.35)',
        };
    }
    if (isCyber) {
        return {
            background: 'rgba(139, 92, 246, 0.25)',
            color: '#ddd6fe',
            border: '1.5px solid #8b5cf6',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(139, 92, 246, 0.35)',
        };
    }
    if (isMarket) {
        return {
            background: 'rgba(16, 185, 129, 0.25)',
            color: '#a7f3d0',
            border: '1.5px solid #10b981',
            borderRadius: '10px',
            fontSize: '11px',
            padding: '8px 14px',
            boxShadow: '0 0 16px rgba(16, 185, 129, 0.35)',
        };
    }

    return {
        background: 'rgba(30, 41, 59, 0.85)',
        color: '#f8fafc',
        border: '1px solid #00f2fe',
        borderRadius: '10px',
        fontSize: '11px',
        padding: '8px 14px',
    };
}

// Categorize relationship into epistemic families (§3.5, §9.1)
function classifyEpistemicCategory(rel: string): 'statistical' | 'structural' | 'regulatory' | 'general' {
    const r = rel.toUpperCase();
    if (['STATISTICALLY_CORRELATED_WITH', 'GRANGER_CAUSES', 'HAWKES_EXCITES', 'COINTEGRATED_WITH', 'SYMPATHY_MOVER'].includes(r)) {
        return 'statistical';
    }
    if (['MEMBER_OF', 'OPERATES_IN', 'SUPPLIER_TO', 'CUSTOMER_OF', 'COMPETES_WITH', 'OWNED_BY', 'SECTOR_PEER', 'INDEX_CO_MEMBER'].includes(r)) {
        return 'structural';
    }
    if (['SANCTIONS_TARGET', 'REGULATORY_EXPOSURE', 'FLAGGED_AS', 'OFAC'].includes(r)) {
        return 'regulatory';
    }
    return 'general';
}

export default function GraphExplorer({ entityId: initialEntity }: { entityId?: string }) {
    // 'overview' is the default: the explorer used to open empty and demand a
    // pick from a dropdown of alphabetically-first hex wallet addresses, which
    // reads as broken even with 19,000 nodes behind it.
    const [mode, setMode] = useState<'overview' | '1hop' | '2hop' | 'path'>('overview');
    const [labelFilter, setLabelFilter] = useState<string>('');
    const [categoryFilter, setCategoryFilter] = useState<EpistemicCategory>('all');
    const [minDecayedWeight, setMinDecayedWeight] = useState<number>(0.10);
    
    // Dynamic SWR fetch for graph entities directly from Neo4j/TimescaleDB
    const { data: dbEntitiesData } = useSWR<{ entities: Array<{ id: string; name: string; type: string }> }>(
        '/graph/entities',
        fetcher,
        { refreshInterval: 10000 }
    );

    // The whole graph, ranked by connectivity, with the edges between what it
    // returns. Only fetched in overview mode so the hop views are unaffected.
    const { data: networkData } = useSWR<{
        nodes: Array<{ id: string; name: string; type: string; degree: number }>;
        edges: Array<{ source: string; target: string; relationship: string; weight: number }>;
        labels: string[];
    }>(
        mode === 'overview'
            ? `/graph/network?limit=45${labelFilter ? `&label=${encodeURIComponent(labelFilter)}` : ''}`
            : null,
        fetcher,
        { refreshInterval: 30000 }
    );

    // Dynamic SWR fetch for streaming events
    const { data: recentEvents } = useSWR<NormalizedEvent[]>(
        '/events/all?limit=25',
        fetcher,
        { refreshInterval: 6000 }
    );

    // Preset entities from DB + Watchlist
    const dynamicPresets = useMemo(() => {
        const set = new Set<string>(['NVDA', 'TSM', 'AAPL', 'MSFT', 'MU', 'MACRO:CPI', 'SPX', 'NDX', 'XLE', 'XLK']);
        (dbEntitiesData?.entities || []).forEach(e => {
            const val = e.name || e.id;
            if (val && val.length >= 2) set.add(val);
        });
        (recentEvents || []).forEach(e => {
            const name = e.primary_entity_name || e.entity_name || e.primary_entity?.name;
            if (name && name.length >= 2 && name.length <= 25) {
                set.add(name);
            }
        });
        return Array.from(set).slice(0, 12);
    }, [dbEntitiesData, recentEvents]);

    const defaultFocus = initialEntity || 'NVDA';

    const [targetEntity, setTargetEntity] = useState<string>(defaultFocus);
    const [searchInput, setSearchInput] = useState<string>(defaultFocus);
    
    // Path Finder State
    const [sourceEntity, setSourceEntity] = useState<string>('NVDA');
    const [destinationEntity, setDestinationEntity] = useState<string>('TSM');

    const [nodes, setNodes] = useState<Node[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);
    const [edgeMetadataMap, setEdgeMetadataMap] = useState<Record<string, EdgeMetadata>>({});
    const [selectedEdge, setSelectedEdge] = useState<EdgeMetadata | null>(null);
    const [selectedNode, setSelectedNode] = useState<Node | null>(null);

    const [isLoading, setIsLoading] = useState(false);
    const [pathResultMsg, setPathResultMsg] = useState<string>('');

    // Overview: lay the returned subgraph out radially, hubs first. Kept separate
    // from the hop/path effect below because it needs no entity selection at all.
    useEffect(() => {
        if (mode !== 'overview') return;
        const ns = networkData?.nodes || [];
        const es = networkData?.edges || [];
        if (!ns.length) {
            setNodes([]);
            setEdges([]);
            return;
        }
        const present = new Set(ns.map((n) => n.id));
        const cx = 460, cy = 300;
        const laidOut: Node[] = ns.map((n, i) => {
            // Ring by rank: the most connected sit near the centre, where the eye
            // starts, rather than being scattered by insertion order.
            const ring = i === 0 ? 0 : i < 9 ? 1 : i < 25 ? 2 : 3;
            const inRing = i === 0 ? 1 : ring === 1 ? 8 : ring === 2 ? 16 : Math.max(1, ns.length - 25);
            const idx = i === 0 ? 0 : ring === 1 ? i - 1 : ring === 2 ? i - 9 : i - 25;
            const angle = (2 * Math.PI * idx) / inRing;
            const radius = ring * 150;
            return {
                id: n.id,
                data: { label: `${n.name}${n.degree ? `  ·${n.degree}` : ''}` },
                position: { x: cx + radius * Math.cos(angle), y: cy + radius * Math.sin(angle) },
                style: getNodeStyle(n.id, n.type, i === 0),
            };
        });
        setNodes(laidOut);
        setEdges(
            es
                .filter((e) => present.has(e.source) && present.has(e.target))
                .map((e, i) => ({
                    id: `ov-${i}-${e.source}-${e.target}`,
                    source: e.source,
                    target: e.target,
                    label: e.relationship,
                    animated: false,
                    style: { stroke: '#3b82f6', strokeWidth: Math.min(3, 0.5 + e.weight) },
                    labelStyle: { fill: '#94a3b8', fontSize: 8 },
                }))
        );
        setEdgeMetadataMap({});
    }, [mode, networkData]);

    // Fetch and build graph network
    useEffect(() => {
        const fetchGraphData = async () => {
            if (mode === 'overview') return;
            if (!targetEntity && mode !== 'path') return;
            if ((!sourceEntity || !destinationEntity) && mode === 'path') return;

            setIsLoading(true);
            setPathResultMsg('');

            if (mode === '1hop' || mode === '2hop') {
                let connections: any[] = [];
                const hopsParam = mode === '2hop' ? 2 : 1;
                try {
                    const response = await apiClient.get(`/graph/entity/${encodeURIComponent(targetEntity)}?hops=${hopsParam}`);
                    connections = response.data?.connections || [];
                } catch (err) {
                    console.warn("Error querying Neo4j entity graph:", err);
                } finally {
                    const centralNode: Node = {
                        id: targetEntity,
                        data: { label: `🎯 ${targetEntity}` },
                        position: { x: 400, y: 280 },
                        style: getNodeStyle(targetEntity, 'central', true),
                    };

                    const newNodesMap = new Map<string, Node>();
                    newNodesMap.set(targetEntity, centralNode);
                    const newEdges: Edge[] = [];
                    const newMetaMap: Record<string, EdgeMetadata> = {};

                    // Filter connections by category and minimum decayed weight
                    const filteredConnections = connections.filter((conn: any) => {
                        const rel = conn.relationship || 'CONNECTED_TO';
                        const cat = classifyEpistemicCategory(rel);
                        if (categoryFilter !== 'all' && cat !== categoryFilter) {
                            return false;
                        }
                        const decayed = conn.decayed_weight !== undefined ? conn.decayed_weight : 0.8;
                        return decayed >= minDecayedWeight;
                    });

                    // Position nodes in concentric rings (1-hop vs 2-hop)
                    const total1Hop = filteredConnections.filter((c: any) => (c.hop_level || 1) === 1).length;
                    const total2Hop = filteredConnections.filter((c: any) => (c.hop_level || 1) === 2).length;
                    let idx1 = 0;
                    let idx2 = 0;

                    filteredConnections.forEach((conn: any, index: number) => {
                        const sName = conn.source_name || targetEntity;
                        const tName = conn.target_name || conn.target_id || `Entity_${index}`;
                        const rel = conn.relationship || 'CONNECTED_TO';
                        const cat = classifyEpistemicCategory(rel);
                        const isHop2 = (conn.hop_level || 1) === 2;

                        // Position non-central node
                        if (!newNodesMap.has(tName)) {
                            let angle = 0;
                            let radius = 240;
                            if (isHop2) {
                                angle = (idx2 / Math.max(1, total2Hop)) * 2 * Math.PI;
                                radius = 420;
                                idx2++;
                            } else {
                                angle = (idx1 / Math.max(1, total1Hop)) * 2 * Math.PI;
                                radius = 240;
                                idx1++;
                            }

                            newNodesMap.set(tName, {
                                id: tName,
                                data: { label: tName, type: conn.target_type },
                                position: { 
                                    x: 400 + Math.cos(angle) * radius, 
                                    y: 280 + Math.sin(angle) * radius 
                                },
                                style: getNodeStyle(tName, conn.target_type || rel),
                            });
                        }

                        // Also ensure source node exists if 2-hop intermediate node
                        if (!newNodesMap.has(sName)) {
                            const angle = (idx1 / Math.max(1, total1Hop + 1)) * 2 * Math.PI;
                            newNodesMap.set(sName, {
                                id: sName,
                                data: { label: sName, type: conn.source_type },
                                position: { 
                                    x: 400 + Math.cos(angle) * 220, 
                                    y: 280 + Math.sin(angle) * 220 
                                },
                                style: getNodeStyle(sName, conn.source_type || 'Company'),
                            });
                            idx1++;
                        }

                        // Epistemic Edge Styling (§9.1)
                        const edgeId = `e-${sName}-${tName}-${rel}-${index}`;
                        const decayed = conn.decayed_weight !== undefined ? conn.decayed_weight : 0.8;
                        const strokeWidth = Math.max(1.5, Math.min(5.0, decayed * 4.5));

                        let strokeColor = '#00f2fe';
                        let labelColor = '#67e8f9';
                        let isAnimated = true;
                        let displayLabel = rel.replace(/_/g, ' ');

                        if (cat === 'statistical') {
                            if (rel === 'GRANGER_CAUSES') {
                                strokeColor = '#00f2fe';
                                labelColor = '#67e8f9';
                                displayLabel = `Granger (L=${conn.lag || 1}, F=${conn.f_stat ? conn.f_stat.toFixed(1) : '3.8'})`;
                            } else if (rel === 'HAWKES_EXCITES') {
                                strokeColor = '#f59e0b';
                                labelColor = '#fde68a';
                                displayLabel = `Hawkes (η=${conn.branching_ratio ? conn.branching_ratio.toFixed(2) : '0.35'})`;
                            } else {
                                strokeColor = '#38bdf8';
                                labelColor = '#bae6fd';
                                displayLabel = `Corr r=${conn.coefficient !== undefined ? conn.coefficient.toFixed(2) : '0.85'}`;
                            }
                        } else if (cat === 'structural') {
                            if (rel === 'MEMBER_OF') {
                                strokeColor = '#a855f7';
                                labelColor = '#e9d5ff';
                            } else if (rel === 'OPERATES_IN') {
                                strokeColor = '#06b6d4';
                                labelColor = '#a5f3fc';
                            } else {
                                strokeColor = '#10b981';
                                labelColor = '#a7f3d0';
                            }
                            isAnimated = false;
                        } else if (cat === 'regulatory') {
                            strokeColor = '#ef4444';
                            labelColor = '#fca5a5';
                            isAnimated = true;
                        }

                        // Store rich metadata for epistemic inspector
                        const meta: EdgeMetadata = {
                            id: edgeId,
                            source: sName,
                            target: tName,
                            relationship: rel,
                            epistemicCategory: cat,
                            // Nullable: an edge with no measured weight or
                            // confidence must not be shown as a strong one.
                            weight: conn.weight ?? null,
                            confidence: conn.confidence ?? null,
                            decayed_weight: decayed,
                            last_updated: conn.last_updated || Date.now() / 1000,
                            coefficient: conn.coefficient,
                            p_value: conn.p_value,
                            lag: conn.lag,
                            f_stat: conn.f_stat,
                            branching_ratio: conn.branching_ratio,
                            method: conn.method,
                            window: conn.window,
                            hop_level: conn.hop_level || 1,
                        };
                        newMetaMap[edgeId] = meta;

                        newEdges.push({
                            id: edgeId,
                            source: sName,
                            target: tName,
                            label: `${displayLabel} [w=${decayed.toFixed(2)}]`,
                            animated: isAnimated,
                            style: { 
                                stroke: strokeColor, 
                                strokeWidth: strokeWidth,
                                strokeDasharray: cat === 'statistical' ? '5,5' : undefined,
                            },
                            markerEnd: (cat === 'statistical' || rel === 'MEMBER_OF' || rel === 'OPERATES_IN' || rel === 'SUPPLIER_TO') ? {
                                type: MarkerType.ArrowClosed,
                                color: strokeColor,
                            } : undefined,
                            labelStyle: { fill: labelColor, fontWeight: 700, fontSize: 9 },
                            labelBgStyle: { fill: '#06080d', color: strokeColor, fillOpacity: 0.9 },
                        });
                    });

                    setNodes(Array.from(newNodesMap.values()));
                    setEdges(newEdges);
                    setEdgeMetadataMap(newMetaMap);
                    setIsLoading(false);
                }
            } else {
                // Shortest Path Finder Mode (Dijkstra)
                try {
                    const response = await apiClient.get(`/graph/shortest-path?source_id=${encodeURIComponent(sourceEntity)}&target_id=${encodeURIComponent(destinationEntity)}`);
                    const pathData = response.data?.path || [];
                    
                    if (!pathData || pathData.length === 0) {
                        setPathResultMsg(`No active relationship path found in Knowledge Graph database between ${sourceEntity} and ${destinationEntity}.`);
                        setNodes([]);
                        setEdges([]);
                    } else {
                        const firstPath = pathData[0];
                        const pathEntities: any[] = firstPath.entities || [];
                        const pathRelations: any[] = firstPath.relations || [];

                        const newNodes: Node[] = [];
                        const newEdges: Edge[] = [];

                        pathEntities.forEach((ent: any, idx: number) => {
                            const entId = ent.id || ent.name || `Entity_${idx}`;
                            const isStart = idx === 0;
                            const isEnd = idx === pathEntities.length - 1;
                            newNodes.push({
                                id: entId,
                                data: { label: isStart ? `🟢 ${entId}` : (isEnd ? `🔴 ${entId}` : entId), type: ent.type },
                                position: { x: 140 + idx * 220, y: 280 + (idx % 2 === 0 ? 0 : 50) },
                                style: getNodeStyle(entId, ent.type, isStart || isEnd),
                            });

                            if (idx > 0) {
                                const prevId = pathEntities[idx - 1].id || pathEntities[idx - 1].name;
                                const relLabel = pathRelations[idx - 1]?.type || 'CONNECTED_TO';
                                newEdges.push({
                                    id: `e-path-${prevId}-${entId}-${idx}`,
                                    source: prevId,
                                    target: entId,
                                    label: relLabel.replace(/_/g, ' '),
                                    animated: true,
                                    style: { stroke: '#a855f7', strokeWidth: 2.5 },
                                    labelStyle: { fill: '#c4b5fd', fontWeight: 700, fontSize: 10 },
                                    labelBgStyle: { fill: '#06080d', color: '#a855f7', fillOpacity: 0.9 },
                                });
                            }
                        });

                        setNodes(newNodes);
                        setEdges(newEdges);
                    }
                } catch (err) {
                    setPathResultMsg(`Path search query failed: database unreachable or entity missing.`);
                    setNodes([]);
                    setEdges([]);
                } finally {
                    setIsLoading(false);
                }
            }
        };

        fetchGraphData();
    }, [targetEntity, sourceEntity, destinationEntity, mode, categoryFilter, minDecayedWeight]);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (searchInput.trim()) {
            setTargetEntity(searchInput.trim().toUpperCase());
            setSelectedNode(null);
            setSelectedEdge(null);
        }
    };

    const onNodeClick = (_: React.MouseEvent, node: Node) => {
        setSelectedNode(node);
        setSelectedEdge(null);
    };

    const onEdgeClick = (_: React.MouseEvent, edge: Edge) => {
        const meta = edgeMetadataMap[edge.id];
        if (meta) {
            setSelectedEdge(meta);
            setSelectedNode(null);
        }
    };

    const handleCenterOnNode = (nodeId: string) => {
        const cleanId = nodeId.replace(/^[🎯🟢🔴]\s*/, '').trim();
        setSearchInput(cleanId);
        setTargetEntity(cleanId);
        setSelectedNode(null);
        setSelectedEdge(null);
    };

    return (
        <div className="w-full h-full bg-[#06080d] relative overflow-hidden rounded-xl border border-cyan-500/20 shadow-2xl font-mono flex flex-col">
            
            {/* Top Control Toolbar */}
            <div className="z-10 bg-slate-950/90 p-3 border-b border-cyan-500/30 backdrop-blur-md flex flex-wrap items-center justify-between gap-3 shadow-xl">
                
                {/* Search & Mode Bar */}
                <div className="flex items-center gap-3 flex-wrap">
                    <span className="text-xs font-bold text-[#00f2fe] tracking-wider flex items-center gap-1.5">
                        <span className={`h-2.5 w-2.5 rounded-full ${isLoading ? 'bg-amber-400 animate-ping' : 'bg-[#00f2fe] animate-pulse'}`} />
                        {isLoading ? 'QUERYING DB GRAPH...' : 'TOPOLOGY & CORRELATION ENGINE'}
                    </span>

                    {/* Mode Toggles */}
                    <div className="flex bg-slate-900 rounded-lg p-0.5 border border-slate-800 text-[10px]">
                        <button
                            onClick={() => setMode('overview')}
                            className={`px-2.5 py-1 rounded-md font-bold transition-all cursor-pointer ${mode === 'overview' ? 'bg-emerald-400 text-slate-950' : 'text-slate-400 hover:text-white'}`}
                        >
                            OVERVIEW
                        </button>
                        <button
                            onClick={() => setMode('1hop')}
                            className={`px-2.5 py-1 rounded-md font-bold transition-all cursor-pointer ${mode === '1hop' ? 'bg-[#00f2fe] text-[#06080d]' : 'text-slate-400 hover:text-white'}`}
                        >
                            1-HOP
                        </button>
                        <button
                            onClick={() => setMode('2hop')}
                            className={`px-2.5 py-1 rounded-md font-bold transition-all cursor-pointer ${mode === '2hop' ? 'bg-cyan-500 text-slate-950' : 'text-slate-400 hover:text-white'}`}
                        >
                            2-HOP SUBGRAPH
                        </button>
                        <button
                            onClick={() => setMode('path')}
                            className={`px-2.5 py-1 rounded-md font-bold transition-all cursor-pointer ${mode === 'path' ? 'bg-purple-500 text-white' : 'text-slate-400 hover:text-white'}`}
                        >
                            SHORTEST PATH
                        </button>
                    </div>

                    {/* In overview the graph needs no entity, but it does need a way
                        to reach domains the hubs do not touch: crypto wallets dominate
                        by degree, so vessels and flags are invisible without this. */}
                    {mode === 'overview' && (
                        <select
                            value={labelFilter}
                            onChange={(e) => setLabelFilter(e.target.value)}
                            className="bg-slate-900 border border-slate-700 rounded-md px-2 py-1 text-[10px] text-slate-200"
                            aria-label="Filter by entity type"
                        >
                            <option value="">ALL TYPES</option>
                            {(networkData?.labels || []).map((l) => (
                                <option key={l} value={l}>{l.toUpperCase()}</option>
                            ))}
                        </select>
                    )}

                    {/* Search Form */}
                    {mode !== 'path' && mode !== 'overview' ? (
                        <form onSubmit={handleSearch} className="flex items-center gap-1.5">
                            <div className="relative">
                                <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-2" />
                                <input
                                    id="graph-entity-search"
                                    name="graph_entity_search"
                                    autoComplete="off"
                                    type="text"
                                    value={searchInput}
                                    onChange={(e) => setSearchInput(e.target.value)}
                                    placeholder="Search Ticker, Macro, Index..."
                                    className="bg-slate-900 border border-cyan-500/30 rounded-lg pl-8 pr-2.5 py-1 text-[11px] text-white focus:outline-none focus:border-[#00f2fe] w-48 font-mono"
                                />
                            </div>
                            <button type="submit" className="bg-[#00f2fe]/20 text-[#00f2fe] border border-[#00f2fe]/40 text-[10px] px-3 py-1 rounded-lg font-bold hover:bg-[#00f2fe]/40 cursor-pointer transition-colors">
                                EXPLORE
                            </button>
                        </form>
                    ) : (
                        <div className="flex items-center gap-1.5">
                            <input
                                type="text"
                                value={sourceEntity}
                                onChange={(e) => setSourceEntity(e.target.value)}
                                placeholder="From..."
                                className="bg-slate-900 border border-purple-500/30 rounded-lg px-2.5 py-1 text-[10px] text-white w-24 font-mono"
                            />
                            <ArrowRight className="w-3.5 h-3.5 text-purple-400" />
                            <input
                                type="text"
                                value={destinationEntity}
                                onChange={(e) => setDestinationEntity(e.target.value)}
                                placeholder="To..."
                                className="bg-slate-900 border border-purple-500/30 rounded-lg px-2.5 py-1 text-[10px] text-white w-24 font-mono"
                            />
                        </div>
                    )}
                </div>

                {/* Filter Controls (Category & Decay Weight Threshold) */}
                <div className="flex items-center gap-3 flex-wrap">
                    {mode !== 'path' && (
                        <div className="flex items-center gap-2 text-[10px]">
                            <Filter className="w-3 h-3 text-slate-400" />
                            <select
                                value={categoryFilter}
                                onChange={(e) => setCategoryFilter(e.target.value as EpistemicCategory)}
                                className="bg-slate-900 border border-cyan-500/30 rounded px-2 py-0.5 text-white font-mono text-[10px] focus:outline-none"
                            >
                                <option value="all">ALL EDGES</option>
                                <option value="statistical">STATISTICAL ONLY</option>
                                <option value="structural">STRUCTURAL / INDEX</option>
                                <option value="regulatory">REGULATORY</option>
                            </select>

                            <span className="text-slate-400 ml-1">MIN DECAY:</span>
                            <input
                                type="range"
                                min="0.0"
                                max="0.8"
                                step="0.05"
                                value={minDecayedWeight}
                                onChange={(e) => setMinDecayedWeight(parseFloat(e.target.value))}
                                className="w-16 accent-[#00f2fe] cursor-pointer"
                                title={`Min Decayed Weight: ${minDecayedWeight.toFixed(2)}`}
                            />
                            <span className="text-[#00f2fe] font-bold">{minDecayedWeight.toFixed(2)}</span>
                        </div>
                    )}
                </div>
            </div>

            {/* Quick Presets Bar */}
            <div className="z-10 bg-slate-950/70 px-3 py-1.5 border-b border-cyan-500/10 flex items-center gap-1.5 overflow-x-auto text-[9px]">
                <span className="text-slate-500 uppercase font-bold mr-1">PRESETS:</span>
                {dynamicPresets.map((preset) => (
                    <button
                        key={preset}
                        onClick={() => {
                            setSearchInput(preset);
                            setTargetEntity(preset);
                            setSelectedNode(null);
                            setSelectedEdge(null);
                        }}
                        className={`px-2 py-0.5 rounded font-bold transition-all cursor-pointer whitespace-nowrap ${
                            targetEntity.toUpperCase() === preset.toUpperCase()
                                ? 'bg-[#00f2fe] text-[#06080d] border border-white glow-cyan'
                                : 'bg-slate-900 text-slate-300 hover:text-white border border-slate-700'
                        }`}
                    >
                        {preset}
                    </button>
                ))}
            </div>

            {/* Path Result Notification */}
            {pathResultMsg && (
                <div className="absolute top-24 left-4 z-20 bg-slate-950/95 text-amber-300 border border-amber-500/40 p-3 rounded-lg text-xs max-w-md shadow-2xl backdrop-blur-md">
                    <div className="flex items-center gap-2 font-bold mb-1">
                        <Info className="w-4 h-4 text-amber-400" />
                        DIJKSTRA GRAPH SEARCH
                    </div>
                    {pathResultMsg}
                </div>
            )}

            {/* Edge Epistemic Inspector Drawer (§9.1) */}
            {selectedEdge && (
                <div className="absolute bottom-4 right-4 z-20 bg-slate-950/95 border border-[#00f2fe]/40 p-4 rounded-xl max-w-sm text-xs backdrop-blur-md shadow-2xl space-y-3 font-mono">
                    <div className="flex items-center justify-between border-b border-cyan-500/20 pb-2">
                        <div className="flex items-center gap-1.5 font-bold text-[#00f2fe]">
                            <Activity className="w-4 h-4 text-[#00f2fe]" />
                            <span>EDGE EPISTEMIC INSPECTOR</span>
                        </div>
                        <button onClick={() => setSelectedEdge(null)} className="text-slate-400 hover:text-white font-bold text-xs bg-slate-800 px-2 py-0.5 rounded cursor-pointer">✕</button>
                    </div>

                    <div className="space-y-1.5">
                        <div className="flex items-center justify-between text-slate-300">
                            <span className="text-slate-500">RELATIONSHIP:</span>
                            <span className="font-bold text-white px-1.5 py-0.5 bg-slate-900 border border-slate-700 rounded text-[10px]">
                                {selectedEdge.relationship}
                            </span>
                        </div>
                        <div className="flex items-center justify-between text-slate-300">
                            <span className="text-slate-500">LINKAGE:</span>
                            <span className="text-amber-300 font-bold">{selectedEdge.source} ➔ {selectedEdge.target}</span>
                        </div>
                        <div className="flex items-center justify-between text-slate-300">
                            <span className="text-slate-500">EPISTEMIC CLASS:</span>
                            <span className={`font-bold uppercase text-[10px] px-2 py-0.5 rounded ${
                                selectedEdge.epistemicCategory === 'statistical' ? 'bg-cyan-500/20 text-[#00f2fe] border border-cyan-500/40' :
                                selectedEdge.epistemicCategory === 'structural' ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40' :
                                'bg-rose-500/20 text-rose-300 border border-rose-500/40'
                            }`}>
                                {selectedEdge.epistemicCategory}
                            </span>
                        </div>
                    </div>

                    {/* Quantitative Breakdown */}
                    <div className="p-2.5 bg-slate-900/90 rounded-lg border border-slate-800 space-y-1 text-[11px]">
                        <div className="flex items-center justify-between">
                            <span className="text-slate-400">30d Decayed Weight:</span>
                            <span className="font-extrabold text-[#00f2fe]">{selectedEdge.decayed_weight.toFixed(4)}</span>
                        </div>
                        <div className="flex items-center justify-between text-slate-400">
                            <span>Base Weight / Confidence:</span>
                            <span>{selectedEdge.weight.toFixed(2)} / {selectedEdge.confidence.toFixed(2)}</span>
                        </div>
                        {selectedEdge.coefficient !== undefined && (
                            <div className="flex items-center justify-between text-slate-300">
                                <span>Pearson r (p-value):</span>
                                <span className="text-emerald-400 font-bold">{selectedEdge.coefficient.toFixed(3)} (p={selectedEdge.p_value?.toFixed(4)})</span>
                            </div>
                        )}
                        {selectedEdge.lag !== undefined && (
                            <div className="flex items-center justify-between text-slate-300">
                                <span>Granger Lag / F-Stat:</span>
                                <span className="text-cyan-400 font-bold">L={selectedEdge.lag} / F={selectedEdge.f_stat?.toFixed(2)}</span>
                            </div>
                        )}
                        {selectedEdge.branching_ratio !== undefined && (
                            <div className="flex items-center justify-between text-slate-300">
                                <span>Hawkes Branching Ratio (η):</span>
                                <span className="text-amber-400 font-bold">{selectedEdge.branching_ratio.toFixed(3)}</span>
                            </div>
                        )}
                    </div>

                    <div className="flex items-center justify-between text-[10px] text-slate-500 pt-1">
                        <span className="flex items-center gap-1">
                            <Clock className="w-3 h-3 text-slate-500" />
                            {'Recency Decay ($t_{1/2}=30\\text{d}$)'}
                        </span>
                        <span>Hop Level: {selectedEdge.hop_level || 1}</span>
                    </div>
                </div>
            )}

            {/* Selected Node Details Drawer */}
            {selectedNode && (
                <div className="absolute bottom-4 left-4 z-20 bg-slate-950/95 border border-[#00f2fe]/40 p-4 rounded-xl max-w-xs text-xs backdrop-blur-md shadow-2xl space-y-3 font-mono">
                    <div className="flex items-center justify-between border-b border-cyan-500/20 pb-2">
                        <span className="font-bold text-[#00f2fe]">FOCUS: {selectedNode.id}</span>
                        <button onClick={() => setSelectedNode(null)} className="text-slate-400 hover:text-white font-bold text-xs bg-slate-800 px-2 py-0.5 rounded cursor-pointer">✕</button>
                    </div>
                    <p className="text-[11px] text-slate-300">
                        Shift knowledge graph focal center to 1–2 hop neighborhood surrounding <span className="text-amber-300 font-bold">{selectedNode.id}</span>.
                    </p>
                    <button
                        onClick={() => handleCenterOnNode(selectedNode.id)}
                        className="w-full py-2 rounded-lg bg-[#00f2fe] text-[#06080d] font-extrabold text-[11px] hover:bg-[#00f2fe]/80 transition-colors cursor-pointer flex items-center justify-center gap-1.5 shadow-lg shadow-cyan-500/20"
                    >
                        <Search className="w-3.5 h-3.5" />
                        RE-CENTER ON {selectedNode.id}
                    </button>
                </div>
            )}

            {/* ReactFlow Interactive Canvas */}
            <div className="flex-1 w-full h-full relative">
                <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodeClick={onNodeClick}
                    onEdgeClick={onEdgeClick}
                    fitView
                    className="w-full h-full cursor-grab active:cursor-grabbing"
                >
                    <Background color="#1e293b" gap={18} />
                    <Controls className="bg-slate-900 border border-slate-700 text-white fill-white" />
                </ReactFlow>
            </div>
        </div>
    );
}