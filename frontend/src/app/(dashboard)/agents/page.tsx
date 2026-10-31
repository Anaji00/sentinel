import React from 'react';
import AgentSwarmTelemetry from '@/components/AgentSwarmTelemetry';
import AgentConclusions from '@/components/AgentConclusions';
import { PageHeader } from '@/components/ui/PageHeader';

export default function AgentsPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="AI analysts"
        description="Small AI models, running on this machine, that each watch one area and make calls. Every call is scored when the outcome is known."
        related={[
          { href: '/calls', label: 'Stock calls' },
          { href: '/intel', label: 'News & intel' },
        ]}
      />
      {/* Bounded rather than h-full. The page stacks two sections, and a
          panel that claims the whole viewport pushes the second below the fold
          with nothing to indicate it is there. */}
      <div className="h-[min(60vh,32rem)]">
        <AgentSwarmTelemetry />
      </div>
      {/* What the tier has concluded and stored, as opposed to how it is
          running. Seven keys held this and nothing read any of them. */}
      <section>
        <h2 className="mb-2 px-1 text-sm font-semibold text-ink">What they have concluded</h2>
        <AgentConclusions />
      </section>
    </div>
  );
}
