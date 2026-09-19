'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { ABSENT } from '../lib/format';
import { fetcher } from '../lib/api';
import { DataGrid } from './ui/DataGrid';
import { IconAlert, IconCheck, IconClose, IconRadar, IconStore } from './ui/icons';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';

/** The shape /api/v1/health/ actually returns.
 *
 * This interface previously declared five fields the endpoint published none
 * of -- it serves `system_status`, this read `status` -- and because
 * useSWR<T> asserts over parsed JSON rather than checking it, the mismatch
 * compiled cleanly and rendered three red DISCONNECTED tiles against three
 * healthy datastores, with a header stuck on "CONNECTING...". The endpoint now
 * publishes the connection states and the running configuration; `system_status`
 * is read here under its real name, with `status` kept as the alias the
 * endpoint also sets.
 */
interface SystemHealthResponse {
  system_status?: string;
  status: string;
  redis_connected: boolean;
  timescale_connected: boolean;
  neo4j_connected: boolean;
  active_configuration: {
    maritime_dark_thresholds?: number;
    tracked_financial_instruments?: number;
  };
}

interface MetricsSummary {
  [key: string]: unknown;
}

/** One datastore, and whether it is answering. */
interface StoreProps {
  name: string;
  connected: boolean | undefined;
}

function Store({ name, connected }: StoreProps) {
  // Three states, not two. `undefined` is "the health probe has not answered",
  // which is not the same as "this store is down" -- and rendering it red was
  // how this component previously reported three healthy datastores as failed.
  const unknown = connected === undefined;
  return (
    <div
      className={`flex flex-col items-center gap-1 rounded-lg border p-3 ${
        unknown
          ? 'border-line text-ink-mute'
          : connected
            ? 'border-positive/40 text-positive'
            : 'border-negative/40 text-negative'
      }`}
    >
      <IconStore size={16} />
      <span className="text-micro font-semibold text-ink">{name}</span>
      <span className="text-micro">{unknown ? ABSENT : connected ? 'Connected' : 'Down'}</span>
    </div>
  );
}

export default function SystemHealthHUD() {
  const [isOpen, setIsOpen] = useState(false);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(isOpen, () => setIsOpen(false), 'Platform health');

  // No trailing slash, despite the gateway wanting one.
  //
  // The gateway answers `/api/v1/health/` and 307s `/api/v1/health`. Reaching
  // for the slash here looks right and is wrong: Next normalises trailing
  // slashes on the proxy route, so `/api/proxy/api/v1/health/` 308s to the
  // slashless form in the *browser* before anything is forwarded -- a client
  // round trip, on a refresh timer. Without it the proxy's own server-side
  // fetch follows the gateway's 307 internally and the browser sees one 200.
  //
  // Measured through the deployment: `health` -> 200, `health/` -> 308.
  const { data: healthData } = useSWR<SystemHealthResponse>('/health', fetcher, {
    refreshInterval: POLL.live,
  });

  const { data: metricsData } = useSWR<MetricsSummary>('/health/metrics/json', fetcher, {
    refreshInterval: POLL.live,
  });

  const systemStatus = healthData?.system_status ?? healthData?.status;
  const isHealthy =
    typeof systemStatus === 'string' &&
    ['online', 'healthy', 'operational'].includes(systemStatus.toLowerCase());

  const config = healthData?.active_configuration;
  const darkHours = config?.maritime_dark_thresholds;
  const instruments = config?.tracked_financial_instruments;

  return (
    <>
      {/* The status control in the header.
          This was the loudest element on every page: an amber pill in bold
          uppercase monospace reading "CONNECTING...", larger and brighter than
          the page title beside it. A status that is fine should be quiet --
          the whole point of a status light is that you notice it when it
          changes, and a permanently shouting one trains you not to look. */}
      <button
        onClick={() => setIsOpen(true)}
        aria-label="Platform status"
        className={`flex cursor-pointer items-center gap-2 rounded-lg border border-line px-2.5 py-1.5 text-micro transition-colors hover:border-line-strong ${
          isHealthy ? 'text-ink-dim' : 'text-caution'
        }`}
      >
        <span
          aria-hidden
          className={`h-1.5 w-1.5 rounded-full ${isHealthy ? 'bg-positive' : 'bg-caution'}`}
        />
        <span className="font-medium">{systemStatus ? 'Systems normal' : 'Connecting'}</span>
      </button>

      {isOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4"
          {...dialog.overlayProps}
        >
          <div
            className="panel w-full max-w-lg space-y-4 p-5 text-xs text-ink"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-line pb-3">
              <div className="flex items-center gap-2">
                <IconRadar className="text-accent" size={16} />
                <span className="text-head font-semibold">Platform health</span>
              </div>
              <button
                onClick={() => setIsOpen(false)}
                aria-label="Close"
                className="cursor-pointer rounded p-1 text-ink-dim hover:text-ink"
              >
                <IconClose />
              </button>
            </div>

            <div className="space-y-2">
              <span className="stat-label">Datastores</span>
              <div className="grid grid-cols-3 gap-2">
                <Store name="Redis" connected={healthData?.redis_connected} />
                <Store name="TimescaleDB" connected={healthData?.timescale_connected} />
                <Store name="Neo4j" connected={healthData?.neo4j_connected} />
              </div>
            </div>

            {/* The units were eaten along with the emoji: a whitespace regex
                turned `' hours'` into `'hours'`, so this read "4Hours" and
                "26Assets". */}
            <div className="space-y-1.5 rounded-lg border border-line bg-page p-3">
              <span className="stat-label block">Running configuration</span>
              <div className="flex items-center justify-between text-micro">
                <span className="text-ink-dim">Maritime dark threshold</span>
                <span className="font-mono font-semibold text-ink">
                  {darkHours == null ? ABSENT : `${darkHours} hours`}
                </span>
              </div>
              <div className="flex items-center justify-between text-micro">
                <span className="text-ink-dim">Instruments tracked</span>
                <span className="font-mono font-semibold text-ink">
                  {instruments == null ? ABSENT : `${instruments} symbols`}
                </span>
              </div>
            </div>

            <div className="space-y-1.5 rounded-lg border border-line bg-page p-3">
              <span className="stat-label flex items-center gap-1.5">
                {metricsData ? <IconCheck /> : <IconAlert />}
                Telemetry
              </span>
              <div className="max-h-32 overflow-y-auto">
                {/* Rendered even when absent, so "no telemetry" is stated
                    rather than shown as a missing section. */}
                <DataGrid data={metricsData ?? {}} emptyLabel="No telemetry reported" />
              </div>
            </div>

            <button
              onClick={() => setIsOpen(false)}
              className="w-full cursor-pointer rounded-lg border border-line py-2 text-xs font-medium text-ink-dim transition-colors hover:border-line-strong hover:text-ink"
            >
              Close
            </button>
          </div>
        </div>
      )}
    </>
  );
}
