import React from 'react';
import Institutional13FPanel from '@/components/Institutional13FPanel';
import FilingsStream from '@/components/FilingsStream';
import OwnershipPanel from '@/components/OwnershipPanel';
import { PageHeader } from '@/components/ui/PageHeader';
import { InsiderIntent } from '@/components/InsiderIntent';

export const metadata = {
  title: 'Filings & fund holdings | Sentinel',
  description: 'Company filings as they reach the SEC, what the big funds own, and who controls each company.',
};

export default function FilingsPage() {
  return (
    <div className="flex flex-col gap-6 p-2 max-w-7xl mx-auto w-full">
      <PageHeader
        title="Filings & fund holdings"
        description="Company filings as they reach the SEC (big events, ownership stakes, insider sales), what the largest funds own each quarter, and who controls each company."
        related={[
          { href: '/markets?view=flow', label: 'Big-money flow' },
          { href: '/calls', label: 'Look up a stock' },
          { href: '/calls', label: 'Signal board' },
        ]}
      />

      {/* The page promised "8-K material events ... and regulatory disclosures"
          in its own subtitle and rendered only the 13F panel. `/filings/latest`
          serves the disclosure stream and had no caller. */}
      <div className="min-h-[24rem]">
        <FilingsStream />
      </div>

      <InsiderIntent />

      <Institutional13FPanel />

      <OwnershipPanel />
    </div>
  );
}
