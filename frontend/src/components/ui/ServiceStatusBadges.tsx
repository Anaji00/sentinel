'use client';

import React from 'react';
import useSWR from 'swr';
import { Database, Zap, Network } from 'lucide-react';
import { fetcher } from '../../lib/api';
import { formatAge } from '../../lib/format';

/**
 * Live backend status badges.
 *
 * These were previously three hardcoded pills that rendered green with a
 * pulsing dot regardless of backend state -- reassuring, and wrong precisely
 * when it mattered. They now reflect the health endpoint, and degrade to an
 * explicit unknown state rather than defaulting to healthy.
 */

type Health = 'healthy' | 'degraded' | 'down' | 'unknown';

interface ComponentEntry {
  status?: string;
  age_seconds?: number | null;
  last_seen?: string | null;
}

interface HealthPayload {
  system_status?: string;
  components?: Record<string, ComponentEntry>;
}

const TONE: Record<Health, string> = {
  healthy: 'bg-emerald-500/15 border-emerald-500/30 text-emerald-400',
  degraded: 'bg-amber-500/15 border-amber-500/30 text-amber-400',
  down: 'bg-rose-500/15 border-rose-500/30 text-rose-400',
  unknown: 'bg-slate-700/20 border-slate-600/40 text-slate-400',
};

const LABEL: Record<Health, string> = {
  healthy: 'LIVE',
  degraded: 'DEGRADED',
  down: 'DOWN',
  unknown: 'UNKNOWN',
};

function toHealth(status?: string): Health {
  switch (status) {
    case 'HEALTHY':
      return 'healthy';
    case 'DEGRADED':
      return 'degraded';
    case 'DEAD':
    case 'OFFLINE':
    case 'ERROR':
      return 'down';
    default:
      return 'unknown';
  }
}

const Badge: React.FC<{
  icon: React.ReactNode;
  name: string;
  health: Health;
  detail?: string | null;
}> = ({ icon, name, health, detail }) => (
  <div
    className={`flex items-center gap-1.5 px-3 py-1 border rounded-lg text-xs font-bold ${TONE[health]}`}
    title={detail ?? undefined}
  >
    {health === 'healthy' ? (
      <span className="w-2 h-2 rounded-full bg-current animate-pulse" />
    ) : (
      icon
    )}
    <span>
      {name} {LABEL[health]}
    </span>
  </div>
);

/**
 * @param components Heartbeat component names to surface, in display order.
 */
export const ServiceStatusBadges: React.FC<{ components?: string[] }> = ({
  components = ['enrichment', 'correlation'],
}) => {
  const { data, error, isLoading } = useSWR<HealthPayload>('/health/data', fetcher, {
    refreshInterval: 15_000,
    revalidateOnFocus: false,
  });

  const entries = data?.components ?? {};

  // Datastore reachability is inferred from the services that depend on them:
  // if enrichment and correlation are healthy, their upstreams are answering.
  const overall: Health = error
    ? 'down'
    : isLoading
    ? 'unknown'
    : toHealth(
        components.every((c) => entries[c]?.status === 'HEALTHY')
          ? 'HEALTHY'
          : components.some((c) => entries[c]?.status === 'HEALTHY')
          ? 'DEGRADED'
          : 'OFFLINE',
      );

  const freshest = components
    .map((c) => entries[c]?.age_seconds)
    .filter((v): v is number => typeof v === 'number')
    .sort((a, b) => a - b)[0];

  const detail =
    typeof freshest === 'number'
      ? `Last heartbeat ${formatAge(new Date(Date.now() - freshest * 1000).toISOString())} ago`
      : null;

  return (
    <div className="flex items-center gap-2 flex-wrap" aria-live="polite">
      <Badge
        icon={<Network className="w-3.5 h-3.5" />}
        name="PIPELINE"
        health={overall}
        detail={detail}
      />
      {components.map((c) => (
        <Badge
          key={c}
          icon={c === 'correlation' ? <Zap className="w-3.5 h-3.5" /> : <Database className="w-3.5 h-3.5" />}
          name={c.toUpperCase()}
          health={toHealth(entries[c]?.status)}
          detail={entries[c]?.last_seen ?? null}
        />
      ))}
    </div>
  );
};

export default ServiceStatusBadges;
