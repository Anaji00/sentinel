'use client';

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { AlertTriangle, CheckCircle2, ExternalLink, KeyRound, ShieldAlert } from 'lucide-react';

/**
 * Data-source configuration status.
 *
 * The platform degrades honestly when an upstream key is missing — that
 * collector reports nothing rather than inventing data. The cost is that an
 * empty panel looks the same whether the market is quiet, a collector crashed,
 * or nobody ever set the key. This panel separates the third case from the
 * other two and says exactly what to set.
 *
 * No key is ever displayed or entered here. Credentials are read from the
 * environment at startup, so the browser never sees them and the application
 * never becomes their custodian.
 */

interface IntegrationStatus {
  key: string;
  label: string;
  domain: string;
  configured: boolean;
  required: boolean;
  free_tier: boolean;
  missing_env_vars: string[];
  signup_url: string;
  impact_when_unset: string;
}

interface KeylessSource {
  key: string;
  label: string;
  domain: string;
}

interface IntegrationsResponse {
  configured_count: number;
  total_count: number;
  keyless_sources: KeylessSource[];
  integrations: IntegrationStatus[];
  needs_attention: Array<Pick<
    IntegrationStatus,
    'key' | 'label' | 'missing_env_vars' | 'signup_url' | 'impact_when_unset'
  >>;
  guidance: string;
}

interface ExecutionStatus {
  broker_type: string;
  venue: string;
  is_live_capital: boolean;
  credentials_present: boolean;
  operational: boolean;
  warning: string | null;
}

export default function IntegrationsPanel() {
  const { data, isLoading } = useSWR<IntegrationsResponse>('/integrations', fetcher, {
    refreshInterval: 60_000,
  });
  const { data: exec } = useSWR<ExecutionStatus>('/integrations/execution', fetcher, {
    refreshInterval: 60_000,
  });

  return (
    <Card
      title="DATA SOURCE INTEGRATIONS"
      badge={
        data ? (
          <Badge variant={data.needs_attention.length === 0 ? 'success' : 'warning'}>
            {data.configured_count}/{data.total_count} CONFIGURED
          </Badge>
        ) : (
          <Badge variant="neutral">CHECKING…</Badge>
        )
      }
      noPadding
    >
      <div className="p-3.5 space-y-3 font-mono text-xs">
        {/* Execution venue. Stated before anything else: the difference between
            the simulator and real capital is one environment variable, and an
            operator should never infer it from the absence of a warning. */}
        {exec && (
          <div
            className={`p-3 rounded-xl border flex items-start gap-2.5 ${
              exec.is_live_capital
                ? 'bg-rose-950/40 border-rose-500/50'
                : 'bg-slate-950/70 border-slate-800'
            }`}
          >
            {exec.is_live_capital ? (
              <ShieldAlert className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
            ) : (
              <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0 mt-0.5" />
            )}
            <div className="min-w-0">
              <div className="text-[10px] uppercase tracking-wider text-slate-400">
                Order execution venue
              </div>
              <div
                className={`font-bold mt-0.5 ${
                  exec.is_live_capital ? 'text-rose-300' : 'text-emerald-300'
                }`}
              >
                {exec.venue}
              </div>
              {exec.warning && (
                <div className="text-[11px] text-rose-300 mt-1">{exec.warning}</div>
              )}
              {!exec.operational && (
                <div className="text-[11px] text-amber-300 mt-1">
                  Broker selected but credentials are absent — orders will fail at submission.
                </div>
              )}
            </div>
          </div>
        )}

        {isLoading && <div className="text-slate-500 py-4 text-center">Checking integrations…</div>}

        {/* Unconfigured first: this is the actionable half. */}
        {data && data.needs_attention.length > 0 && (
          <div className="space-y-2">
            <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-amber-400 font-bold">
              <AlertTriangle className="w-3.5 h-3.5" />
              {data.needs_attention.some((n) =>
                data.integrations.some((i) => i.key === n.key && i.required)
              )
                ? 'Action needed — something is blocked'
                : 'Unconfigured — coverage is reduced'}
            </div>
            {data.needs_attention.map((item) => {
              // A required integration is a different kind of problem: the
              // others narrow coverage, this one blocks people. Email delivery
              // is the case that matters -- without it nobody can confirm an
              // address or recover a password.
              const isRequired = data.integrations.some(
                (i) => i.key === item.key && i.required
              );
              return (
              <div
                key={item.key}
                className={`p-2.5 bg-slate-950/70 border rounded-xl space-y-1.5 ${
                  isRequired ? 'border-rose-500/50' : 'border-amber-500/25'
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="font-bold text-slate-200">
                    {item.label}
                    {isRequired && (
                      <span className="ml-2 px-1.5 py-0.5 rounded bg-rose-500/15 text-rose-400 text-[9px] font-black tracking-widest align-middle">
                        REQUIRED
                      </span>
                    )}
                  </span>
                  {item.signup_url && (
                    <a
                      href={item.signup_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-1 text-[10px] text-cyan-400 hover:text-cyan-300 shrink-0"
                    >
                      GET A FREE KEY <ExternalLink className="w-3 h-3" />
                    </a>
                  )}
                </div>
                <p className="text-[11px] text-slate-400 leading-relaxed">
                  {item.impact_when_unset}
                </p>
                <div className="flex items-center gap-1.5 flex-wrap">
                  <KeyRound className="w-3 h-3 text-slate-500" />
                  {item.missing_env_vars.map((v) => (
                    <code
                      key={v}
                      className="px-1.5 py-0.5 bg-slate-900 border border-slate-700 rounded text-[10px] text-amber-300"
                    >
                      {v}
                    </code>
                  ))}
                </div>
              </div>
              );
            })}
            <p className="text-[10px] text-slate-500 leading-relaxed pt-1">{data.guidance}</p>
          </div>
        )}

        {/* Configured, compact — confirmation, not detail. */}
        {data && data.integrations.some((i) => i.configured) && (
          <div className="space-y-1.5">
            <div className="text-[10px] uppercase tracking-wider text-emerald-400 font-bold">
              Configured
            </div>
            <div className="flex flex-wrap gap-1.5">
              {data.integrations
                .filter((i) => i.configured)
                .map((i) => (
                  <span
                    key={i.key}
                    className="flex items-center gap-1 px-2 py-0.5 bg-emerald-950/40 border border-emerald-500/30 rounded text-[10px] text-emerald-300"
                  >
                    <CheckCircle2 className="w-3 h-3" />
                    {i.label.split('—')[0].trim()}
                  </span>
                ))}
            </div>
          </div>
        )}

        {/* Keyless sources, so an absent entry above is not read as broken. */}
        {data && data.keyless_sources.length > 0 && (
          <div className="space-y-1.5 pt-1 border-t border-slate-800/70">
            <div className="text-[10px] uppercase tracking-wider text-slate-500 font-bold">
              Active without a key
            </div>
            <div className="flex flex-wrap gap-1.5">
              {data.keyless_sources.map((s) => (
                <span
                  key={s.key}
                  className="px-2 py-0.5 bg-slate-900/70 border border-slate-800 rounded text-[10px] text-slate-400"
                >
                  {s.label.split('—')[0].trim()}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
