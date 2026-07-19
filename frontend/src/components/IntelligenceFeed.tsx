'use client';

import React, { useState } from "react";
import useSWR from "swr";
import { fetcher } from "../lib/api";
import { Scenario, NormalizedEvent } from "../lib/types";

export default function IntelligenceFeed() {
    const [activeTab, setActiveTab] = useState<"scenarios" | "events">("events");
    const [selectedDomain, setSelectedDomain] = useState<string>("all");

    // Fetch AI Scenarios
    const { data: scenarios, error: scenariosError, isLoading: scenariosLoading } = useSWR<Scenario[]>(
        "/scenarios?limit=20",
        fetcher,
        { refreshInterval: 15000 }
    );

    // Dynamic Event domain fetches
    const fetchDomains = ["tradfi", "crypto", "prediction", "cyber", "maritime"];
    
    // Create SWR fetch calls for selected domain or parallel all domains
    const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
        selectedDomain === "all" || selectedDomain === "tradfi" ? "/events/tradfi?limit=25" : null,
        fetcher,
        { refreshInterval: 10000 }
    );
    const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
        selectedDomain === "all" || selectedDomain === "crypto" ? "/events/crypto?limit=25" : null,
        fetcher,
        { refreshInterval: 10000 }
    );
    const { data: predictionEvents } = useSWR<NormalizedEvent[]>(
        selectedDomain === "all" || selectedDomain === "prediction" ? "/events/prediction?limit=25" : null,
        fetcher,
        { refreshInterval: 10000 }
    );
    const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
        selectedDomain === "all" || selectedDomain === "cyber" ? "/events/cyber?limit=25" : null,
        fetcher,
        { refreshInterval: 10000 }
    );
    const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
        selectedDomain === "all" || selectedDomain === "maritime" ? "/events/maritime?limit=25" : null,
        fetcher,
        { refreshInterval: 10000 }
    );

    // Merge and sort events
    const allEvents: NormalizedEvent[] = [];
    if (selectedDomain === "all" || selectedDomain === "tradfi") allEvents.push(...(tradfiEvents || []));
    if (selectedDomain === "all" || selectedDomain === "crypto") allEvents.push(...(cryptoEvents || []));
    if (selectedDomain === "all" || selectedDomain === "prediction") allEvents.push(...(predictionEvents || []));
    if (selectedDomain === "all" || selectedDomain === "cyber") allEvents.push(...(cyberEvents || []));
    if (selectedDomain === "all" || selectedDomain === "maritime") allEvents.push(...(maritimeEvents || []));

    // Sort by occurred_at descending
    const sortedEvents = allEvents.sort((a, b) => new Date(b.occurred_at).getTime() - new Date(a.occurred_at).getTime()).slice(0, 40);

    const isEventsLoading = 
        (selectedDomain === "all" && !tradfiEvents && !cryptoEvents && !predictionEvents && !cyberEvents && !maritimeEvents) ||
        (selectedDomain === "tradfi" && !tradfiEvents) ||
        (selectedDomain === "crypto" && !cryptoEvents) ||
        (selectedDomain === "prediction" && !predictionEvents) ||
        (selectedDomain === "cyber" && !cyberEvents) ||
        (selectedDomain === "maritime" && !maritimeEvents);

    return (
        <div className="h-full flex flex-col bg-[#0b0c10] border-r border-[#1f2833] overflow-hidden">
            {/* Header & Tabs */}
            <div className="p-4 border-b border-[#1f2833] bg-[#0b0c10]">
                <h1 className="text-lg font-bold tracking-widest text-[#66fcf1] uppercase mb-4 font-mono">
                    SENTINEL INTEL FEED
                </h1>
                
                <div className="flex gap-2 p-1 bg-[#1f2833]/30 rounded-md border border-[#1f2833]">
                    <button
                        onClick={() => setActiveTab("events")}
                        className={`flex-1 py-1.5 text-xs font-mono font-semibold rounded uppercase tracking-wider transition-all duration-200 ${
                            activeTab === "events"
                                ? "bg-[#66fcf1] text-black shadow-md"
                                : "text-gray-400 hover:text-white"
                        }`}
                    >
                        Live Feeds
                    </button>
                    <button
                        onClick={() => setActiveTab("scenarios")}
                        className={`flex-1 py-1.5 text-xs font-mono font-semibold rounded uppercase tracking-wider transition-all duration-200 ${
                            activeTab === "scenarios"
                                ? "bg-[#66fcf1] text-black shadow-md"
                                : "text-gray-400 hover:text-white"
                        }`}
                    >
                        AI Hypotheses
                    </button>
                </div>
            </div>

            {/* Filter bar for Live Events */}
            {activeTab === "events" && (
                <div className="px-4 py-2 border-b border-[#1f2833]/50 bg-[#0f1115] flex gap-2 overflow-x-auto scrollbar-thin scrollbar-thumb-gray-800">
                    {["all", "tradfi", "crypto", "prediction", "cyber", "maritime"].map((dom) => (
                        <button
                            key={dom}
                            onClick={() => setSelectedDomain(dom)}
                            className={`px-3 py-1 rounded-full text-[10px] font-mono font-semibold uppercase tracking-wider transition-all duration-200 whitespace-nowrap ${
                                selectedDomain === dom
                                    ? "bg-[#45f3ff]/20 text-[#66fcf1] border border-[#66fcf1]"
                                    : "bg-[#1f2833]/50 text-gray-400 hover:bg-[#1f2833] border border-transparent"
                            }`}
                        >
                            {dom === "all" ? "ALL" : dom}
                        </button>
                    ))}
                </div>
            )}

            {/* List Container */}
            <div className="flex-1 overflow-y-auto p-4 space-y-4 scrollbar-thin scrollbar-thumb-gray-800">
                {activeTab === "scenarios" ? (
                    scenariosLoading ? (
                        <div className="flex justify-center items-center h-32 text-gray-500 font-mono text-xs animate-pulse">
                            DECRYPTING COGNITIVE HYPOTHESES...
                        </div>
                    ) : scenariosError ? (
                        <div className="text-red-500 text-xs font-mono">Failed to fetch active intelligence scenarios.</div>
                    ) : scenarios?.length === 0 ? (
                        <div className="text-gray-500 text-xs font-mono text-center py-10">No active scenarios detected.</div>
                    ) : (
                        scenarios?.map((scenario) => (
                            <div
                                key={scenario.correlation_id}
                                className="bg-[#151b26]/50 p-4 rounded border border-[#1f2833] hover:border-[#66fcf1] transition-all duration-300 shadow-lg group relative overflow-hidden"
                            >
                                <div className="absolute top-0 right-0 w-16 h-16 bg-[#66fcf1]/5 rounded-bl-full pointer-events-none group-hover:bg-[#66fcf1]/10 transition-colors" />
                                <div className="flex justify-between items-center mb-2">
                                    <span className="text-[10px] font-mono font-bold text-[#66fcf1] bg-[#66fcf1]/10 px-2 py-0.5 rounded border border-[#66fcf1]/30">
                                        CONF: {scenario.confidence_overall}%
                                    </span>
                                    <span className="text-[10px] font-mono text-gray-500">
                                        {new Date(scenario.created_at).toLocaleTimeString()}
                                    </span>
                                </div>
                                <h3 className="text-sm font-semibold text-gray-200 mb-2 leading-snug group-hover:text-white transition-colors">
                                    {scenario.headline}
                                </h3>
                                <p className="text-xs text-gray-400 line-clamp-3">
                                    {scenario.significance}
                                </p>
                            </div>
                        ))
                    )
                ) : (
                    // Live Event Feed Rendering
                    isEventsLoading ? (
                        <div className="flex justify-center items-center h-32 text-gray-500 font-mono text-xs animate-pulse">
                            MONITORING LIVE NETWORK EVENTS...
                        </div>
                    ) : sortedEvents.length === 0 ? (
                        <div className="text-gray-500 text-xs font-mono text-center py-10">No recent events found.</div>
                    ) : (
                        sortedEvents.map((event) => {
                            const isHighAnomaly = event.anomaly_score >= 0.75;
                            const isWhaleSweep = event.type === "options_flow" && (event.financial_data?.premium_usd || 0) >= 100000;
                            
                            // Determine domain visual accents
                            let borderAccent = "border-[#1f2833] hover:border-gray-500";
                            let iconColor = "text-gray-400";
                            if (event.type.startsWith("vessel_") || event.type.startsWith("maritime_") || event.type === "maritime") {
                                borderAccent = "border-amber-900/30 hover:border-amber-500";
                                iconColor = "text-amber-400";
                            } else if (event.type === "options_flow" || event.type === "equity_block") {
                                borderAccent = isWhaleSweep ? "border-cyan-500 hover:border-cyan-300" : "border-blue-900/30 hover:border-blue-500";
                                iconColor = isWhaleSweep ? "text-cyan-400" : "text-blue-400";
                            } else if (event.type === "prediction_market_trade" || event.type === "prediction_market") {
                                borderAccent = "border-purple-900/30 hover:border-purple-500";
                                iconColor = "text-purple-400";
                            } else if (event.type === "crypto_liquidation" || event.type === "crypto_trade" || event.type === "crypto_transfer") {
                                borderAccent = "border-emerald-900/30 hover:border-emerald-500";
                                iconColor = "text-emerald-400";
                            } else if (event.type === "bgp_anomaly" || event.type === "breach_detected" || event.type === "infra_exposed") {
                                borderAccent = "border-red-900/30 hover:border-red-500";
                                iconColor = "text-red-500";
                            }

                            return (
                                <div
                                    key={event.event_id}
                                    className={`bg-[#0f1218] p-4 rounded border ${borderAccent} transition-all duration-300 relative overflow-hidden ${
                                        isHighAnomaly ? "shadow-red-950/20 shadow-md" : "shadow-md"
                                    }`}
                                >
                                    {/* High Anomaly Warning Line */}
                                    {isHighAnomaly && (
                                        <div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-red-600 via-orange-500 to-transparent" />
                                    )}

                                    {/* Card Header */}
                                    <div className="flex justify-between items-center mb-2">
                                        <div className="flex items-center gap-1.5">
                                            <span className={`text-[9px] font-mono font-bold px-1.5 py-0.5 rounded uppercase ${
                                                isHighAnomaly 
                                                    ? "bg-red-950/50 text-red-400 border border-red-800" 
                                                    : "bg-[#1f2833]/50 text-gray-400 border border-[#1f2833]"
                                            }`}>
                                                {event.source}
                                            </span>
                                            {isWhaleSweep && (
                                                <span className="text-[9px] font-mono font-bold px-1.5 py-0.5 rounded bg-cyan-950/50 text-cyan-400 border border-cyan-800 animate-pulse">
                                                    🐋 WHALE SWEEP
                                                </span>
                                            )}
                                        </div>
                                        <span className="text-[9px] font-mono text-gray-500">
                                            {new Date(event.occurred_at).toLocaleTimeString()}
                                        </span>
                                    </div>

                                    {/* Headline */}
                                    <h4 className="text-xs font-bold text-gray-200 mb-2 font-mono leading-relaxed">
                                        {event.headline}
                                    </h4>

                                    {/* Dynamic Field Presenters */}
                                    {/* 1. OPTIONS FLOW */}
                                    {event.type === "options_flow" && event.financial_data && (
                                        <div className="mb-2 p-2 bg-black/40 rounded border border-[#1f2833] font-mono text-[10px] text-gray-400 grid grid-cols-2 gap-1">
                                            <div>Asset: <span className="text-white">{event.financial_data.ticker}</span></div>
                                            <div>Prem: <span className="text-cyan-400 font-bold">${((event.financial_data.premium_usd || 0) / 1000).toFixed(1)}k</span></div>
                                            <div>Price: <span className="text-white">${event.financial_data.underlying_price}</span></div>
                                            <div>Vol: <span className="text-white">{event.financial_data.volume}</span></div>
                                        </div>
                                    )}

                                    {/* 2. PREDICTION MARKETS */}
                                    {event.type === "prediction_market_trade" && event.prediction_market_data && (
                                        <div className="mb-2 p-2 bg-black/40 rounded border border-[#1f2833] font-mono text-[10px] text-gray-400">
                                            <div className="text-[9px] text-[#66fcf1] mb-1 truncate">{event.prediction_market_data.title}</div>
                                            
                                            {/* Implied Probability Bars */}
                                            {event.prediction_market_data.yes_probability !== undefined && (
                                                <div className="mt-1.5 space-y-1">
                                                    <div className="flex justify-between text-[9px]">
                                                        <span>YES Prob: {(event.prediction_market_data.yes_probability * 100).toFixed(0)}%</span>
                                                        <span>NO Prob: {((event.prediction_market_data.no_probability ?? (1 - event.prediction_market_data.yes_probability)) * 100).toFixed(0)}%</span>
                                                    </div>
                                                    <div className="h-1.5 w-full bg-gray-800 rounded-full overflow-hidden flex">
                                                        <div 
                                                            className="bg-purple-500 h-full" 
                                                            style={{ width: `${event.prediction_market_data.yes_probability * 100}%` }}
                                                        />
                                                        <div 
                                                            className="bg-gray-600 h-full" 
                                                            style={{ width: `${(1 - event.prediction_market_data.yes_probability) * 100}%` }}
                                                        />
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    )}

                                    {/* 3. CYBER METRICS */}
                                    {event.type === "bgp_anomaly" && event.security_data && (
                                        <div className="mb-2 p-2 bg-black/40 rounded border border-[#1f2833] font-mono text-[10px] text-gray-400">
                                            <div>Affected ASN: <span className="text-red-400">{event.security_data.affected_org}</span></div>
                                            <div>Route Prefix: <span className="text-white">{event.security_data.ip_address}</span></div>
                                        </div>
                                    )}

                                    {/* Anomaly score indicator */}
                                    <div className="flex justify-between items-center mt-2.5">
                                        <div className="flex gap-1.5">
                                            {event.tags?.slice(0, 3).map((tag, idx) => (
                                                <span key={idx} className="text-[8px] font-mono bg-[#1f2833]/30 px-1 rounded text-gray-500">
                                                    #{tag}
                                                </span>
                                            ))}
                                        </div>
                                        <div className="flex items-center gap-1 font-mono text-[10px]">
                                            <span className="text-gray-500">ANOMALY:</span>
                                            <span className={event.anomaly_score >= 0.75 ? "text-red-500 font-bold" : event.anomaly_score >= 0.5 ? "text-amber-500" : "text-green-500"}>
                                                {event.anomaly_score.toFixed(2)}
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            );
                        })
                    )
                )}
            </div>
        </div>
    );
}