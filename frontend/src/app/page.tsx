/**
 * @file page.tsx
 * 
 * This is the main entry point for the Sentinel Dashboard. In Next.js App Router, 
 * `page.tsx` files automatically become publicly accessible routes. Since this is 
 * in the root `app/` directory, it serves as the home page (`/`).
 */

import IntelligenceFeed from '../components/IntelligenceFeed';
import GraphExplorer from '../components/GraphExplorer';
// import GlobalMap from '@/components/GlobalMap'; // Placeholder for future map integration

/**
 * Dashboard Component
 * 
 * Renders the primary user interface layout. It uses a CSS Flexbox layout to split 
 * the screen into a left sidebar (Intelligence Feed) and a main content area (Map & Graph).
 */
export default function Dashboard() {
    return (
        // Changed `w-screen` to `w-full` to prevent horizontal scrollbar layout shifts.
        // `h-screen` makes it take exactly 100% of the viewport height.
        // `overflow-hidden` ensures that child components manage their own scrolling natively.
        <main className="flex h-screen w-full bg-black text-white overflow-hidden">
            
            {/* 
              Left Sidebar: Intelligence Feed 
              - `w-1/4`: Takes up 25% of the total width.
              - `z-10` & `shadow-xl`: Elevates the sidebar slightly above the main area for visual depth.
            */}
            <div className="w-1/4 h-full z-10 shadow-xl">
                <IntelligenceFeed />
            </div>

            {/* 
              Main Area: Split vertically between the Map and the Graph 
              - `w-3/4`: Takes up the remaining 75% of the width.
              - `flex flex-col`: Stacks its children vertically.
            */}
            <div className="w-3/4 flex flex-col h-full">
                
                {/* 
                  Top Half: Global Map 
                  - `h-1/2`: Takes up 50% of the parent's height.
                  - `relative`: Crucial for absolutely positioning the "OVERSIGHT ACTIVE" badge inside this container.
                */}
                <div className="h-1/2 w-full border-b border-gray-800 relative bg-gray-900">
                    {/* Overlay Badge - `absolute` positions it relative to the top-left of the `relative` parent div */}
                    <div className="absolute top-4 left-4 z-10 bg-black/50 px-4 py-2 rounded text-sm text-green-400 font-mono border border-green-900">
                        MARITIME OVERSIGHT ACTIVE
                    </div>
                    {/* <GlobalMap /> */}
                    {/* Fallback UI while the actual map component is inactive */}
                    <div className="flex items-center justify-center h-full text-gray-500 font-mono">
                        [ MAP RENDERING LAYER ]
                    </div>
                </div>

                {/* 
                  Bottom Half: Graph Explorer 
                  - `h-1/2`: Takes the remaining 50% of the parent's height.
                */}
                <div className="h-1/2 w-full relative">
                    {/* 
                      Overlay Badge 
                      - `pointer-events-none`: Ensures that hovering or clicking this badge doesn't block interactions with the graph underneath it.
                    */}
                    <div className="absolute top-4 left-4 z-10 bg-black/50 px-4 py-2 rounded text-sm text-blue-400 font-mono border border-blue-900 pointer-events-none">
                        NEO4J ENTITY GRAPH
                    </div>
                    {/* Renders the interactive node-edge graph component */}
                    <GraphExplorer />
                </div>

            </div>
        </main>
    );
}