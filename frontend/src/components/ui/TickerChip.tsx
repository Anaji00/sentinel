import React from 'react';
import Link from 'next/link';
import { companyForVendor } from '../../lib/surveillanceCompanies';

/** A small link to a listed surveillance company's stock page, beside its name
 * in the civic records. Renders nothing for a private company. */
export function TickerChip({ vendor }: { vendor: string | null | undefined }) {
  const c = companyForVendor(vendor);
  if (!c) return null;
  return (
    <Link
      href={`/ticker?symbol=${c.ticker}`}
      title={`${c.name} is publicly traded: see its stock`}
      onClick={(e) => e.stopPropagation()}
      className="ml-1.5 inline-flex rounded border border-accent/40 px-1 font-mono text-micro text-accent hover:bg-accent-dim"
    >
      {c.ticker}
    </Link>
  );
}

export default TickerChip;
