import React from 'react';
import ScanBoard from '@/components/ScanBoard';
import { PageHeader } from '@/components/ui/PageHeader';

export default function ScanPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="Scan"
        description="Where the market is moving and which stocks to look at: how broad today's move is, how each sector did, and a list you can sort by size of move, unusual volume or the model's view."
        related={[{ href: '/calls', label: 'Calls' }, { href: '/markets', label: 'Markets' }]}
      />
      <ScanBoard />
    </div>
  );
}
