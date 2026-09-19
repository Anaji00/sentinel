'use client';

/**
 * The filings themselves, which the filings page did not show.
 *
 * `/filings` rendered one panel: 13F institutional holdings, from
 * `/filings/13f/prominent`. `/filings/latest` serves the actual disclosure
 * stream -- 8-Ks, 424Bs, 10-Ks, Form 4s as they arrive from EDGAR -- and had no
 * caller. A page named for a domain was showing one slice of it.
 *
 * `is_material_8k` is the field worth building around. Most of what EDGAR emits
 * is routine: a 424B2 prospectus supplement is a bank registering more notes,
 * and this audit has already found it being scored like an annual report. The
 * platform decides materiality server-side, so the filter is the server's
 * judgement rather than a keyword match made here.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ExportButton } from './ui/ExportButton';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';
import { IconAlert, IconDocument } from './ui/icons';
import { formatNumber } from '../lib/format';
import type { CsvColumn } from '../lib/csv';

interface Filing {
  ticker: string | null;
  company_name: string | null;
  form_type: string;
  filing_date: string;
  is_material_8k: boolean;
  items: string[];
  summary: string;
  primary_doc_url: string | null;
}

const CSV_COLUMNS: CsvColumn<Filing>[] = [
  { label: 'filing_date', value: (f) => f.filing_date },
  { label: 'ticker', value: (f) => f.ticker },
  { label: 'company_name', value: (f) => f.company_name },
  { label: 'form_type', value: (f) => f.form_type },
  { label: 'is_material_8k', value: (f) => String(f.is_material_8k) },
  { label: 'items', value: (f) => (f.items.length ? f.items.join('; ') : null) },
  { label: 'summary', value: (f) => f.summary },
  { label: 'primary_doc_url', value: (f) => f.primary_doc_url },
];

export default function FilingsStream() {
  const [materialOnly, setMaterialOnly] = React.useState(false);

  const { data, error, isLoading } = useSWR<Filing[]>('/filings/latest?limit=50', fetcher, {
    // EDGAR publishes in bursts through the day; this is not a tick feed.
    refreshInterval: POLL.slow,
  });

  const filings = React.useMemo(() => {
    const rows = Array.isArray(data) ? data : [];
    return materialOnly ? rows.filter((f) => f.is_material_8k) : rows;
  }, [data, materialOnly]);

  const materialCount = (Array.isArray(data) ? data : []).filter((f) => f.is_material_8k).length;

  if (error) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Filings unavailable"
          detail={describeApiError(error) ?? undefined}
        />
      </Card>
    );
  }

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title flex items-center gap-1.5">
            <IconDocument />
            Filing stream
          </h2>
          <p className="panel-subtitle">
            {data
              ? `${formatNumber(data.length, { decimals: 0 })} recent, ${formatNumber(materialCount, { decimals: 0 })} flagged material`
              : 'SEC EDGAR disclosures as they arrive'}
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          <ExportButton subject="filings" rows={filings} columns={CSV_COLUMNS} />
          <button
            onClick={() => setMaterialOnly((v) => !v)}
            aria-pressed={materialOnly}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
              materialOnly
                ? 'border-line-accent bg-accent-dim text-accent'
                : 'border-line text-ink-mute hover:text-ink-dim'
            }`}
          >
            Material only
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {isLoading && !data ? (
          <EmptyState kind="loading" title="Reading EDGAR" />
        ) : filings.length === 0 ? (
          <EmptyState
            kind="empty"
            title={materialOnly ? 'Nothing flagged material' : 'No recent filings'}
            detail={
              materialOnly
                ? 'Routine filings are still arriving; none of them carries a material 8-K item.'
                : 'The collector has published nothing recently.'
            }
          />
        ) : (
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-inset">
              <tr className="border-b border-line">
                <th className="stat-label px-3 py-2">Date</th>
                <th className="stat-label px-3 py-2">Ticker</th>
                <th className="stat-label px-3 py-2">Company</th>
                <th className="stat-label px-3 py-2">Form</th>
                <th className="stat-label px-3 py-2">Summary</th>
              </tr>
            </thead>
            <tbody>
              {filings.map((f, i) => (
                <tr
                  key={`${f.ticker}-${f.form_type}-${f.filing_date}-${i}`}
                  className="border-b border-line/60 hover:bg-overlay"
                >
                  <td className="whitespace-nowrap px-3 py-2 font-mono text-ink-mute">
                    <ClockTime value={f.filing_date} seconds={false} />
                  </td>
                  <td className="px-3 py-2">
                    <span className="symbol text-accent">{f.ticker ?? '—'}</span>
                  </td>
                  <td
                    className="max-w-[14rem] truncate px-3 py-2 text-ink-dim"
                    title={f.company_name ?? ''}
                  >
                    {f.company_name ?? '—'}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className={`badge ${f.is_material_8k ? 'tone-caution' : 'tone-muted'}`}
                      title={
                        f.is_material_8k
                          ? 'The platform judged this a material 8-K.'
                          : 'Routine disclosure.'
                      }
                    >
                      {f.is_material_8k && <IconAlert />}
                      {f.form_type}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-ink-dim">
                    {/* The document, not a copy of it. The summary is the
                        platform's; the filing is the SEC's, and an analyst
                        acting on one needs the second. */}
                    {f.primary_doc_url ? (
                      <a
                        href={f.primary_doc_url}
                        target="_blank"
                        rel="noreferrer noopener"
                        className="text-ink-dim underline decoration-line-strong underline-offset-2 hover:text-accent"
                      >
                        {f.summary}
                      </a>
                    ) : (
                      f.summary
                    )}
                    {f.items.length > 0 && (
                      <span className="ml-1.5 text-micro text-ink-mute">
                        items {f.items.join(', ')}
                      </span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </Card>
  );
}
