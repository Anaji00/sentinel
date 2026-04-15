'use client'; // Marks this as a Client Component in Next.js, allowing the use of React hooks like useSWR.

import useSWR from "swr";
import { fetcher } from '../lib/api'
import { Scenario } from "../lib/types";

/**
 * IntelligenceFeed Component
 * 
 * This component displays a live feed of active security scenarios.
 * It automatically fetches new data from the backend at a set interval
 * and displays the results in a scrollable list.
 */
export default function IntelligenceFeed() {
    // useSWR is a React hook specifically for data fetching and caching.
    // It takes an endpoint ('/scenarios?limit=10'), a fetcher function (how to get the data),
    // and an options object. Here, we set refreshInterval to 15000ms (15 seconds)
    // to automatically poll the API for new scenarios.
    const {data, error, isLoading} = useSWR<Scenario[]>('/scenarios?limit=10', fetcher, {
        refreshInterval: 15000
    });

    // Early returns for error and loading states.
    // This ensures the main UI doesn't crash if data fails to load or is still on its way.
    if (error) return <div className="text-red-500">Failed to load intelligence.</div>;
    if (isLoading) return <div className="text-gray-400 animate-pulse">Decrypting feeds...</div>

    return (
        <div className="h-full overflow-y-auto bg-gray-900 border-r border-gray-800 p-4">
            <h2 className="text-xl font-bold text-gray-100 mb-4 tracking-wider uppercase">Active Scenarios</h2>
            <div className="space-y-4">
                {/* We use optional chaining (data?.map) to safely execute the map function only if 'data' is defined. 
                    For each scenario in the data array, we render a display card. */}
                {data?.map((scenario) =>  (
                    // The 'key' prop is crucial for React. It helps React identify which items have changed, 
                    // are added, or are removed, optimizing rendering performance.
                    <div key={scenario.correlation_id} className="bg-gray-800 p-4 rounded-lg border border-gray-700 hover:border-blue-500 cursor-pointer transition-colors">
                        <div className="flex justify-between items-start mb-2">
                            <span className="text-xs font-mono text-blue-400 bg-blue-900/30 px-2 py-1 rounded">
                                CONF: {scenario.confidence_overall}%
                            </span>
                        </div>
                        {/* Displaying the main headline of the scenario */}
                        <h3 className="text-sm font-semibold text-gray-200 mb-2">{scenario.headline}</h3>
                        
                        {/* Displaying a brief description. 'line-clamp-3' is a Tailwind CSS utility
                            that safely truncates the text with an ellipsis (...) if it exceeds 3 lines. */}
                        <p className="text-xs text-gray-400 line-clamp-3">{scenario.significance}</p>
                    </div>
                ))}
            </div>
        </div>
    );
}