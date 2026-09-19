import React from 'react';
import AgentSwarmTelemetry from '@/components/AgentSwarmTelemetry';
import AgentConclusions from '@/components/AgentConclusions';

export default function AgentSwarmPage() {
  return (
    <div className="h-full w-full space-y-3 overflow-y-auto p-2">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Agents</h1>
      {/* Bounded rather than h-full. The page now stacks two sections, and a
          panel that claims the whole viewport pushes the second below the fold
          with nothing to indicate it is there. */}
      <div className="h-[min(60vh,32rem)]">
        <AgentSwarmTelemetry />
      </div>

      {/* What the tier has concluded and stored, as opposed to how it is
          running. Seven keys held this and nothing read any of them. */}
      <section>
        <h2 className="mb-2 px-1 text-micro font-semibold uppercase tracking-wide text-ink-mute">
          Stored conclusions
        </h2>
        <AgentConclusions />
      </section>
    </div>
  );
}
