'use client';

/**
 * The ledger the interface already claimed existed.
 *
 * `DataSovereigntyModal` tells every operator that "every order submission,
 * flag toggle, and watchlist mutation is immutably recorded in a SHA-256
 * hash-chained ledger." `/audit/trail` has served exactly that, gated to
 * ANALYST, for the whole life of the project, and `/audit/verify` will walk
 * the chain from genesis and say whether it holds. No component called either.
 *
 * So the strongest claim the product makes about its own integrity was a
 * sentence in a modal, with no way for the person reading it to check. That is
 * the same shape as every other finding in this codebase's audit -- a claim in
 * one place, unbacked in another -- and it is the worst place for it, because
 * an audit trail nobody can read is not an audit trail.
 *
 * The verdict is reported exactly as the backend words it. `EMPTY_LEDGER` and
 * `VERIFIED_VALID` are different facts -- the backend was changed earlier in
 * this audit specifically so they would stop returning the same `valid: true`
 * -- and collapsing them into one green tick here would undo that fix in the
 * only place a person would ever see it.
 */

import React from 'react';
import useSWR from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ExportButton } from './ui/ExportButton';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';
import { IconAlert, IconCheck, IconLock, IconPending } from './ui/icons';
import { ABSENT, formatNumber } from '../lib/format';
import { jsonCell, type CsvColumn } from '../lib/csv';

interface AuditEntry {
  hash: string | null;
  prev_hash: string | null;
  timestamp: string | null;
  actor: string | null;
  action: string | null;
  resource_type: string | null;
  resource_id: string | null;
  ip_address: string | null;
  details: Record<string, unknown>;
}

interface TrailResponse {
  count: number;
  offset: number;
  limit: number;
  entries: AuditEntry[];
}

/** Every shape `verify_chain` returns. Kept in the union so no branch is lost. */
interface VerifyResponse {
  valid: boolean;
  verified?: boolean;
  entries_checked?: number;
  latest_hash?: string;
  broken_at_index?: number;
  expected_hash?: string;
  found_hash?: string;
  detail?: string;
  error?: string;
  status:
    'VERIFIED_VALID' | 'EMPTY_LEDGER' | 'NO_STORAGE' | 'HASH_CORRUPTED' | 'VERIFICATION_ERROR';
}

const PAGE = 50;

const CSV_COLUMNS: CsvColumn<AuditEntry>[] = [
  { label: 'timestamp', value: (e) => e.timestamp },
  { label: 'actor', value: (e) => e.actor },
  { label: 'action', value: (e) => e.action },
  { label: 'resource_type', value: (e) => e.resource_type },
  { label: 'resource_id', value: (e) => e.resource_id },
  { label: 'ip_address', value: (e) => e.ip_address },
  { label: 'hash', value: (e) => e.hash },
  { label: 'prev_hash', value: (e) => e.prev_hash },
  // Serialised rather than flattened: the keys differ per action, so columns
  // built from the first row would drop whatever the others carry.
  { label: 'details', value: (e) => jsonCell(e.details) },
];

/** How the chain verdict reads, in the backend's own terms. */
const VERDICT: Record<
  VerifyResponse['status'],
  { tone: string; Icon: React.FC<{ className?: string }>; title: string }
> = {
  VERIFIED_VALID: { tone: 'text-positive', Icon: IconCheck, title: 'Chain intact' },
  EMPTY_LEDGER: { tone: 'text-ink-mute', Icon: IconPending, title: 'Nothing recorded yet' },
  NO_STORAGE: { tone: 'text-caution', Icon: IconAlert, title: 'No durable store configured' },
  HASH_CORRUPTED: { tone: 'text-negative', Icon: IconAlert, title: 'Chain broken' },
  VERIFICATION_ERROR: { tone: 'text-negative', Icon: IconAlert, title: 'Could not verify' },
};

export default function AuditTrail() {
  const [offset, setOffset] = React.useState(0);
  const [verify, setVerify] = React.useState<VerifyResponse | null>(null);
  const [verifying, setVerifying] = React.useState(false);
  const [verifyError, setVerifyError] = React.useState<string | null>(null);

  const { data, error, isLoading } = useSWR<TrailResponse>(
    `/audit/trail?limit=${PAGE}&offset=${offset}`,
    fetcher,
    { refreshInterval: POLL.slow },
  );

  const entries = data?.entries ?? [];

  const runVerify = async () => {
    setVerifying(true);
    setVerifyError(null);
    try {
      // POST, and ADMIN-only on the gateway. An ANALYST gets a 403 here, which
      // is a correct answer rather than a failure, so it is worded as one.
      const res = await apiClient.post<VerifyResponse>('/audit/verify');
      setVerify(res.data);
    } catch (err: unknown) {
      setVerifyError(
        describeApiError(err) === 'Session expired'
          ? 'Verifying the chain requires an administrator.'
          : (describeApiError(err) ?? 'Could not verify the chain.'),
      );
      setVerify(null);
    }
    setVerifying(false);
  };

  if (error) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Audit trail unavailable"
          detail={describeApiError(error) ?? undefined}
        />
      </Card>
    );
  }

  const verdict = verify ? VERDICT[verify.status] : null;

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title flex items-center gap-1.5">
            <IconLock />
            Audit trail
          </h2>
          <p className="panel-subtitle">
            Hash-chained record of every order, flag and watchlist change
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          <ExportButton subject="audit trail" rows={entries} columns={CSV_COLUMNS} />
          <button
            onClick={runVerify}
            disabled={verifying}
            className="cursor-pointer rounded-md border border-line px-2 py-0.5 text-micro font-medium text-ink-dim transition-colors enabled:hover:border-line-strong enabled:hover:text-ink disabled:opacity-40"
          >
            {verifying ? 'Verifying…' : 'Verify chain'}
          </button>
        </div>
      </div>

      {(verdict || verifyError) && (
        <div className="shrink-0 border-b border-line bg-inset px-3.5 py-2.5">
          {verifyError ? (
            <p role="alert" className="text-micro text-caution">
              {verifyError}
            </p>
          ) : (
            verdict &&
            verify && (
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <span className={`flex items-center gap-1.5 text-xs font-semibold ${verdict.tone}`}>
                  <verdict.Icon />
                  {verdict.title}
                </span>
                <span className="text-micro text-ink-mute">
                  {/* The backend's own sentence when it has one. It says why an
                      empty ledger is not a verified one, which is the single
                      most misreadable thing on this panel. */}
                  {verify.detail ??
                    verify.error ??
                    (verify.entries_checked !== undefined
                      ? `${formatNumber(verify.entries_checked, { decimals: 0 })} entries checked`
                      : null)}
                </span>
                {verify.latest_hash && (
                  <span className="font-mono text-micro text-ink-mute" title={verify.latest_hash}>
                    head {verify.latest_hash.slice(0, 12)}…
                  </span>
                )}
                {verify.broken_at_index !== undefined && (
                  <span className="font-mono text-micro text-negative">
                    breaks at entry {verify.broken_at_index}
                  </span>
                )}
              </div>
            )
          )}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-y-auto">
        {isLoading && !data ? (
          <EmptyState kind="loading" title="Reading the ledger" />
        ) : entries.length === 0 ? (
          <EmptyState
            kind="empty"
            title="No entries recorded"
            detail="Nothing has written to the ledger yet. An empty trail is not a verified one."
          />
        ) : (
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-inset">
              <tr className="border-b border-line">
                <th className="stat-label px-3 py-2">Time</th>
                <th className="stat-label px-3 py-2">Actor</th>
                <th className="stat-label px-3 py-2">Action</th>
                <th className="stat-label px-3 py-2">Resource</th>
                <th className="stat-label px-3 py-2">Source</th>
                <th className="stat-label px-3 py-2">Hash</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => (
                <tr
                  key={e.hash ?? `${e.timestamp}-${e.action}`}
                  className="border-b border-line/60"
                >
                  <td className="px-3 py-2 font-mono text-ink-dim">
                    <ClockTime value={e.timestamp} />
                  </td>
                  <td className="px-3 py-2 text-ink">{e.actor || ABSENT}</td>
                  <td className="px-3 py-2 text-ink-dim">{e.action || ABSENT}</td>
                  <td className="px-3 py-2 text-ink-dim">
                    {e.resource_type ? (
                      <span>
                        {e.resource_type}
                        {e.resource_id ? (
                          <span className="font-mono text-ink-mute"> {e.resource_id}</span>
                        ) : null}
                      </span>
                    ) : (
                      ABSENT
                    )}
                  </td>
                  <td className="px-3 py-2 font-mono text-ink-mute">{e.ip_address || ABSENT}</td>
                  <td className="px-3 py-2 font-mono text-ink-mute" title={e.hash ?? undefined}>
                    {e.hash ? `${e.hash.slice(0, 10)}…` : ABSENT}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Offset paging, because the endpoint pages and the ledger grows without
          bound. The next control is disabled on a short page rather than on a
          count, since the endpoint does not publish a total. */}
      <div className="flex shrink-0 items-center justify-between border-t border-line px-3.5 py-2">
        <span className="text-micro text-ink-mute">
          {entries.length > 0
            ? `Entries ${offset + 1}–${offset + entries.length}, newest first`
            : 'No entries on this page'}
        </span>
        <div className="flex items-center gap-1.5">
          <button
            onClick={() => setOffset((o) => Math.max(0, o - PAGE))}
            disabled={offset === 0}
            className="cursor-pointer rounded-md border border-line px-2 py-0.5 text-micro text-ink-dim transition-colors enabled:hover:text-ink disabled:opacity-40"
          >
            Newer
          </button>
          <button
            onClick={() => setOffset((o) => o + PAGE)}
            disabled={entries.length < PAGE}
            className="cursor-pointer rounded-md border border-line px-2 py-0.5 text-micro text-ink-dim transition-colors enabled:hover:text-ink disabled:opacity-40"
          >
            Older
          </button>
        </div>
      </div>
    </Card>
  );
}
