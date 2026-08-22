import React from 'react';
import { IntelligenceFeedIsland } from './islands';

/**
 * Server Component. The shell renders on the server; only the live feed is a
 * client island.
 */
export default function IntelligencePage() {
  return (
    <div className="flex h-full w-full flex-col p-4">
      <h1 className="text-2xl font-semibold mb-4 text-slate-100 uppercase tracking-wider">
        Multi-Agent Global Threat &amp; Risk Feed
      </h1>
      <div className="flex-1 min-h-0 relative">
        <IntelligenceFeedIsland />
      </div>
    </div>
  );
}
