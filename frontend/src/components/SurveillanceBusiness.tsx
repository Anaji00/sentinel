'use client';

/**
 * On a surveillance company's stock page: what it sells to police and
 * government, the federal money it has been paid, the devices of its make
 * volunteers have mapped, and what it spends lobbying. The same records the
 * civic watch reads, seen from the investor's side.
 */

import React from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { formatCompact, formatNumber } from '../lib/format';
import { companyForTicker } from '../lib/surveillanceCompanies';
import { Skeleton } from './ui/Skeleton';

interface Money {
  totals: { kind: string; matched: string; awards: number; total: number | null }[];
  agencies: { agency: string; sub_agency: string; awards: number; total: number }[];
}

interface Summary {
  devices_by_maker: { name: string; count: number }[];
}

interface Lobbying {
  by_vendor: { vendor: string; total: number }[];
}

function Fact({ value, label }: { value: string; label: string }) {
  return (
    <div className="rounded-xl border border-civic/20 bg-page/40 px-3 py-2">
      <div className="font-mono text-head leading-none text-ink">{value}</div>
      <div className="mt-1 text-micro text-ink-dim">{label}</div>
    </div>
  );
}

export function SurveillanceBusiness({ ticker }: { ticker: string }) {
  const c = companyForTicker(ticker);
  const { data: money } = useSWR<Money>(
    c?.money ? `/civic/money?limit=5&matched=${encodeURIComponent(c.money)}` : null,
    fetcher,
  );
  const { data: summary } = useSWR<Summary>(c?.maker ? '/civic/summary' : null, fetcher);
  const { data: lobby } = useSWR<Lobbying>(c?.lobby ? '/civic/lobbying' : null, fetcher);
  if (!c) return null;

  const contract = money?.totals.find((t) => t.kind === 'contract' && t.matched === c.money);
  const devices = summary?.devices_by_maker.find((d) => d.name === c.maker)?.count;
  const lobbied = lobby?.by_vendor.find((v) => v.vendor === c.lobby)?.total;
  const payers = [...(money?.agencies ?? [])].sort((a, b) => b.total - a.total).slice(0, 3);
  const loading = (c.money && !money) || (c.maker && !summary) || (c.lobby && !lobby);

  return (
    <section className="rounded-2xl border border-civic/30 bg-gradient-to-br from-civic-dim via-raised to-raised p-4 sm:p-5 space-y-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <div>
          <div className="text-micro font-semibold uppercase tracking-wider text-civic">Surveillance business</div>
          <h2 className="text-sm font-semibold text-ink">
            {c.name} sells {c.sells}.
          </h2>
        </div>
        <Link href="/watch#money" className="text-micro text-civic hover:underline">
          Follow the money &rarr;
        </Link>
      </div>
      {loading ? (
        <Skeleton height="4rem" />
      ) : (
        <>
          <div className="grid grid-cols-2 lg:grid-cols-3 gap-2">
            {contract?.total != null && (
              <Fact
                value={`$${formatCompact(contract.total, { decimals: 1 })}`}
                label={`in ${formatNumber(contract.awards, { decimals: 0 })} federal contracts`}
              />
            )}
            {devices != null && <Fact value={formatNumber(devices, { decimals: 0 })} label="of its devices mapped by volunteers" />}
            {lobbied != null && (
              <Fact value={`$${formatCompact(lobbied, { decimals: 1 })}`} label="spent lobbying Congress since 1999" />
            )}
          </div>
          {payers.length > 0 && (
            <p className="text-xs text-ink-dim">
              Biggest federal customers:{' '}
              {payers.map((p) => (p.sub_agency && p.sub_agency !== p.agency ? p.sub_agency : p.agency)).join(', ')}.
            </p>
          )}
          {contract == null && devices == null && lobbied == null && (
            <p className="text-xs text-ink-mute">
              No federal contracts, mapped devices or lobbying filings are in the records yet.
            </p>
          )}
        </>
      )}
    </section>
  );
}

export default SurveillanceBusiness;
