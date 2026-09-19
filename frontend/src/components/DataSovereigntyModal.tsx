'use client';

/**
 * The sovereignty manifest, as the platform reports it rather than as prose.
 *
 * This modal asserted the platform's strongest claims about itself in a hundred
 * and fifty lines of hardcoded copy: a "100% Local" badge, four boxes naming
 * subsystems, a sentence about a SHA-256 hash-chained ledger. Every one of
 * those was a literal. `/system/sovereignty` has served the measured version
 * for the life of the project, and nothing called it.
 *
 * That is the shape this audit has now found three times over -- the interface
 * stating something the backend can actually answer, with nothing connecting
 * them -- and it is worst here, because the claims are about trustworthiness.
 * A hardcoded "tamper_evident: true" stays true on a screen after it stops
 * being true in the ledger.
 *
 * The chain status is the sharpest case. `audit_chain_status` reads the real
 * ledger, and reads `EMPTY_LEDGER` when nothing has been recorded -- a
 * distinction the backend was changed earlier in this audit to make, precisely
 * because an intact chain and no chain at all had been reporting the same
 * verdict. Rendering a green shield over that would put the defect back on the
 * only screen where anyone would see it.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';
import { EmptyState } from './ui/EmptyState';
import {
  IconAlert,
  IconBlocked,
  IconCheck,
  IconClose,
  IconLock,
  IconPending,
  IconRadar,
  IconShield,
  IconStore,
} from './ui/icons';
import { ABSENT, formatNumber } from '../lib/format';

interface Subsystem {
  service_name: string;
  location: string;
  data_direction: string;
  description: string;
  user_data_exposed: boolean;
}

interface CryptographicGuarantees {
  audit_trail?: string;
  genesis_hash?: string;
  tamper_evident?: boolean;
  audit_chain_status?: string;
  audit_entries_verified?: number;
  session_security?: string;
  analytics_telemetry_trackers?: string;
}

interface SovereigntyManifest {
  architecture_model: string;
  sovereignty_score_pct: number;
  prohibited_categories: string[];
  local_subsystems: Subsystem[];
  external_ingest_feeds: Subsystem[];
  cryptographic_guarantees: CryptographicGuarantees;
  statement_of_intent: string;
}

/** `mobile_carrier_location` is a field name; a reader is owed a phrase. */
function humanize(token: string): string {
  const words = token.replace(/_/g, ' ');
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/**
 * How the chain verdict reads, in the backend's own vocabulary.
 *
 * `EMPTY_LEDGER` is not a pass. The endpoint distinguishes it from
 * `VERIFIED_VALID` on purpose, and collapsing the two here would undo that fix
 * in the one place a person looks to decide whether to trust the platform.
 */
const CHAIN: Record<
  string,
  { tone: string; Icon: React.FC<{ className?: string }>; label: string }
> = {
  VERIFIED_VALID: { tone: 'tone-positive', Icon: IconCheck, label: 'Chain verified' },
  EMPTY_LEDGER: { tone: 'tone-muted', Icon: IconPending, label: 'Nothing recorded yet' },
  NO_STORAGE: { tone: 'tone-caution', Icon: IconAlert, label: 'No durable store' },
  HASH_CORRUPTED: { tone: 'tone-negative', Icon: IconAlert, label: 'Chain broken' },
  VERIFICATION_ERROR: { tone: 'tone-negative', Icon: IconAlert, label: 'Could not verify' },
};

function SubsystemRow({ s, outbound }: { s: Subsystem; outbound?: boolean }) {
  return (
    <div className="rounded-lg border border-line bg-page p-3">
      <div className="flex items-start justify-between gap-2">
        <span className="text-xs font-semibold text-ink">{s.service_name}</span>
        <span
          className={`badge shrink-0 ${s.user_data_exposed ? 'tone-negative' : 'tone-positive'}`}
          title={
            s.user_data_exposed
              ? 'This subsystem is reported as exposing user data.'
              : 'No user data is reported as reaching this subsystem.'
          }
        >
          {s.user_data_exposed ? 'exposes user data' : 'no user data'}
        </span>
      </div>
      <p className="mt-0.5 text-micro text-ink-mute">
        {s.location} · {s.data_direction}
      </p>
      <p className="mt-1.5 text-micro leading-relaxed text-ink-dim">{s.description}</p>
      {outbound && null}
    </div>
  );
}

export const DataSovereigntyModal: React.FC = () => {
  const [isOpen, setIsOpen] = React.useState(false);
  const dialog = useDialog(isOpen, () => setIsOpen(false), 'Data sovereignty');

  // Only while it is open. This is a static manifest describing how the
  // deployment is put together, not a live figure, so polling it on every
  // page for a dialog nobody has opened would be pure traffic.
  const { data, error } = useSWR<SovereigntyManifest>(
    isOpen ? '/system/sovereignty' : null,
    fetcher,
    { refreshInterval: POLL.rare },
  );

  const guarantees = data?.cryptographic_guarantees;
  const chain = guarantees?.audit_chain_status
    ? (CHAIN[guarantees.audit_chain_status] ?? {
        tone: 'tone-muted',
        Icon: IconPending,
        label: guarantees.audit_chain_status,
      })
    : null;

  return (
    <>
      {/* The trigger said "Data Sovereignty: 100% Local" as a literal. The
          figure is served, so it is read -- and until it has been read the
          control does not quote a number at all. */}
      <button
        onClick={() => setIsOpen(true)}
        className="inline-flex cursor-pointer items-center gap-1.5 rounded-lg border border-line px-3 py-1 text-xs font-medium text-ink-dim transition-colors hover:border-line-strong hover:text-ink"
        title="How this deployment handles data, as the platform reports it"
      >
        <IconShield className="shrink-0" />
        <span>
          Data sovereignty
          {data ? `: ${formatNumber(data.sovereignty_score_pct, { decimals: 0 })}% local` : ''}
        </span>
      </button>

      {isOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4"
          {...dialog.overlayProps}
        >
          <div
            className="flex max-h-[90vh] w-full max-w-2xl flex-col gap-4 overflow-y-auto rounded-2xl border border-line-strong bg-raised p-6 text-ink shadow-2xl"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-line pb-4">
              <div className="flex items-center gap-2.5">
                <IconShield className="text-accent" size={20} />
                <div>
                  <h2 className="text-head font-semibold">Data sovereignty</h2>
                  <p className="text-micro text-ink-mute">
                    {data?.architecture_model ?? 'Reading the manifest…'}
                  </p>
                </div>
              </div>
              <button
                onClick={() => setIsOpen(false)}
                aria-label="Close"
                className="cursor-pointer rounded p-1 text-ink-dim hover:text-ink"
              >
                <IconClose />
              </button>
            </div>

            {error ? (
              <EmptyState
                kind="error"
                title="Manifest unavailable"
                detail={describeApiError(error) ?? undefined}
              />
            ) : !data ? (
              <EmptyState kind="loading" title="Reading the manifest" />
            ) : (
              <>
                {/* The integrity block, first, because it is the only part of
                    this manifest that can change without anyone editing code
                    -- and the only part that can be false. */}
                <div className="space-y-2 rounded-xl border border-line bg-page p-3.5">
                  <span className="stat-label flex items-center gap-1.5">
                    <IconLock />
                    Integrity
                  </span>
                  <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                    {chain && (
                      <span
                        className={`flex items-center gap-1.5 text-xs font-semibold ${chain.tone}`}
                      >
                        <chain.Icon />
                        {chain.label}
                      </span>
                    )}
                    <span className="font-mono text-micro text-ink-mute">
                      {guarantees?.audit_entries_verified === undefined
                        ? ABSENT
                        : `${formatNumber(guarantees.audit_entries_verified, { decimals: 0 })} entries verified`}
                    </span>
                  </div>
                  <dl className="grid grid-cols-1 gap-x-4 gap-y-1 text-micro sm:grid-cols-2">
                    {(
                      [
                        ['Audit trail', guarantees?.audit_trail],
                        ['Sessions', guarantees?.session_security],
                        ['Analytics trackers', guarantees?.analytics_telemetry_trackers],
                      ] as const
                    ).map(([label, value]) => (
                      <React.Fragment key={label}>
                        <dt className="text-ink-mute">{label}</dt>
                        <dd className="text-ink-dim">{value ?? ABSENT}</dd>
                      </React.Fragment>
                    ))}
                  </dl>
                </div>

                <div className="space-y-2">
                  <span className="stat-label flex items-center gap-1.5">
                    <IconStore />
                    Runs here ({data.local_subsystems.length})
                  </span>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                    {data.local_subsystems.map((s) => (
                      <SubsystemRow key={s.service_name} s={s} />
                    ))}
                  </div>
                </div>

                <div className="space-y-2">
                  <span className="stat-label flex items-center gap-1.5">
                    <IconRadar />
                    Reaches outward ({data.external_ingest_feeds.length})
                  </span>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                    {data.external_ingest_feeds.map((s) => (
                      <SubsystemRow key={s.service_name} s={s} outbound />
                    ))}
                  </div>
                </div>

                <div className="space-y-2 rounded-xl border border-negative/30 bg-page p-3.5">
                  <span className="stat-label flex items-center gap-1.5 tone-negative">
                    <IconBlocked />
                    Never collected ({data.prohibited_categories.length})
                  </span>
                  <ul className="grid grid-cols-1 gap-x-4 gap-y-0.5 text-micro text-ink-dim sm:grid-cols-2">
                    {data.prohibited_categories.map((c) => (
                      <li key={c}>{humanize(c)}</li>
                    ))}
                  </ul>
                </div>

                <p className="text-micro leading-relaxed text-ink-mute">
                  {data.statement_of_intent}
                </p>
              </>
            )}
          </div>
        </div>
      )}
    </>
  );
};

export default DataSovereigntyModal;
