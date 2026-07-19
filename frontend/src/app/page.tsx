/**
 * @file page.tsx
 * 
 * Main entry point for the Sentinel Dashboard.
 * Lays out the interactive Global Map, Graph Explorer, Live Intelligence feed,
 * and the Quantitative Financial Advisor suggestions in a 3-column glassmorphic HUD grid.
 */

import IntelligenceFeed from '../components/IntelligenceFeed';
import GraphExplorer from '../components/GraphExplorer';
import GlobalMap from '../components/GlobalMap';
import FinancialAdvisorAdvice from '../components/FinancialAdvisorAdvice';

export default function Dashboard() {
    return (
        <main className="flex h-screen w-full bg-[#0a0b0d] text-white overflow-hidden select-none">
            
            {/* 1. Left Sidebar: Live Event Feed & Scenarios (25% width) */}
            <div className="w-1/4 h-full z-10 shadow-2xl flex flex-col border-r border-[#1f2833]">
                <IntelligenceFeed />
            </div>

            {/* 2. Central Area: Map & Neo4j Graph (50% width) */}
            <div className="w-2/4 flex flex-col h-full border-r border-[#1f2833]">
                
                {/* Top Half: Interactive SVG Map */}
                <div className="h-1/2 w-full border-b border-[#1f2833] relative bg-[#0b0c10]">
                    <div className="absolute top-4 left-4 z-20 bg-black/60 px-3 py-1.5 rounded text-[10px] text-[#66fcf1] font-mono border border-[#66fcf1]/30 uppercase tracking-widest backdrop-blur-md">
                        GLOBAL TELEMETRY OVERSIGHT
                    </div>
                    <GlobalMap />
                </div>

                {/* Bottom Half: Neo4j Graph Network */}
                <div className="h-1/2 w-full relative bg-[#0a0c10]">
                    <div className="absolute top-4 left-4 z-20 bg-black/60 px-3 py-1.5 rounded text-[10px] text-[#66fcf1] font-mono border border-[#66fcf1]/30 uppercase tracking-widest pointer-events-none backdrop-blur-md">
                        NEO4J ENTITY GRAPH
                    </div>
                    <GraphExplorer />
                </div>

            </div>

            {/* 3. Right Sidebar: Standalone Quantitative Financial Advisor (25% width) */}
            <div className="w-1/4 h-full z-10 shadow-2xl flex flex-col">
                <FinancialAdvisorAdvice />
            </div>

        </main>
    );
}