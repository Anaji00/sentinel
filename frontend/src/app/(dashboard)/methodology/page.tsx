'use client';

/**
 * Reads the catalog from /methodology, its one source. This page used to hold its
 * own copy, which went on publishing claims the endpoint had already corrected
 * and tests/test_published_methodology_matches_code.py pins: a 5-year backtest
 * that never ran, a 15% position cap enforced at 10%.
 */

import React, { useState } from 'react';
import useSWR from 'swr';
import { PageHeader } from '@/components/ui/PageHeader';
import { EmptyState } from '@/components/ui/EmptyState';
import { IconAlert, IconShield } from '@/components/ui/icons';
import { fetcher } from '@/lib/api';

interface Parameter {
  name: string;
  description: string;
  default_value: string;
  is_learned: boolean;
  calibration_method: string;
}

interface MethodSummary {
  id: string;
  name: string;
  category: string;
}

interface SignalMethodology extends MethodSummary {
  /** What it does, for someone who has never met the maths. */
  plain: string;
  epistemic_tier: string;
  description: string;
  formula_plain: string;
  assumptions: string[];
  parameters: Parameter[];
  validation_gate: string;
  falsifiability_condition: string;
}

const STATIC_CONTENT = { revalidateOnFocus: false, revalidateIfStale: false };

function MethodDetail({ current }: { current: SignalMethodology }) {
  const noGate = current.validation_gate.startsWith('None');
  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-line pb-4">
        <div>
          <span className="px-2 py-0.5 text-micro font-semibold rounded bg-accent-dim text-accent border border-line-accent uppercase tracking-wide">
            {current.epistemic_tier}
          </span>
          <h2 className="text-xl font-bold text-ink mt-1.5">{current.name}</h2>
          <p className="text-xs text-ink-dim mt-1">{current.description}</p>
        </div>
      </div>

      {/* Plain words first: most readers stop here, and should be able to. */}
      <div className="rounded-lg border border-accent/30 bg-accent-dim px-4 py-3">
        <div className="text-micro font-semibold uppercase tracking-wider text-accent">
          In plain words
        </div>
        <p className="mt-1 text-sm text-ink">{current.plain}</p>
      </div>

      {/* Mathematical Formula Card */}
      <div className="flex flex-col gap-2 p-4 bg-page/90 border border-line rounded-lg">
        <span className="text-xs font-semibold text-accent flex items-center gap-1.5">
          <span>∑</span>
          <span>The formula (for specialists)</span>
        </span>
        <div className="text-sm text-positive bg-raised/90 p-3 rounded border border-line/80 overflow-x-auto">
          {current.formula_plain}
        </div>
      </div>

      {/* Theoretical Assumptions */}
      <div className="flex flex-col gap-2">
        <h3 className="text-xs font-bold uppercase tracking-wider text-ink-dim">
          What it assumes
        </h3>
        <ul className="list-disc list-inside text-xs text-ink-dim space-y-1 bg-page/40 p-3 rounded-lg border border-line/60">
          {current.assumptions.map((assump, i) => (
            <li key={i}>{assump}</li>
          ))}
        </ul>
      </div>

      {/* Parameter Calibration Grid */}
      <div className="flex flex-col gap-2">
        <h3 className="text-xs font-bold uppercase tracking-wider text-ink-dim">
          Its settings, and how each was chosen
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse border border-line">
            <thead>
              <tr className="bg-page text-ink-dim border-b border-line">
                <th className="p-2.5">Setting</th>
                <th className="p-2.5">Value</th>
                <th className="p-2.5">How it was chosen</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-line">
              {current.parameters.map((p) => (
                <tr key={p.name} className="hover:bg-overlay/30">
                  <td className="p-2.5">
                    <span className="font-semibold text-ink">{p.name}</span>
                    <span className="mt-0.5 block text-ink-mute">{p.description}</span>
                  </td>
                  <td className="p-2.5 font-mono text-positive">
                    {p.default_value}
                    {p.is_learned && (
                      <span className="mt-0.5 block font-sans text-ink-mute">Learned from data</span>
                    )}
                  </td>
                  <td className="p-2.5 text-ink-dim">{p.calibration_method}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Validation Gate & Falsifiability */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
        <div
          className={`p-3.5 rounded-lg border ${
            noGate ? 'bg-caution/10 border-caution/30' : 'bg-positive/10 border-positive/30'
          }`}
        >
          <span
            className={`text-xs font-bold flex items-center gap-1 ${noGate ? 'text-caution' : 'text-positive'}`}
          >
            <span>
              <IconShield className="inline-block shrink-0" />
            </span>
            <span>What it must pass before we use it</span>
          </span>
          <p className={`text-xs mt-1 ${noGate ? 'text-caution/90' : 'text-positive/90'}`}>
            {current.validation_gate}
          </p>
        </div>
        <div className="p-3.5 bg-negative/10 border border-negative/30 rounded-lg">
          <span className="text-xs font-bold text-negative flex items-center gap-1">
            <span>
              <IconAlert className="inline-block shrink-0" />
            </span>
            <span>What would tell us it has stopped working</span>
          </span>
          <p className="text-xs text-negative/90 mt-1">{current.falsifiability_condition}</p>
        </div>
      </div>
    </>
  );
}

export default function MethodologyPage() {
  const [pickedId, setPickedId] = useState<string | null>(null);
  const { data: methods, error: listError } = useSWR<MethodSummary[]>(
    '/methodology',
    fetcher,
    STATIC_CONTENT,
  );
  const selectedId = pickedId ?? methods?.[0]?.id ?? null;
  const { data: current, error: detailError } = useSWR<SignalMethodology>(
    selectedId ? `/methodology/${selectedId}` : null,
    fetcher,
    STATIC_CONTENT,
  );
  const failed = Boolean(listError || detailError);

  return (
    <div className="flex flex-col gap-6 p-2 max-w-7xl mx-auto text-ink">
      {/* Header */}
      <PageHeader
        title="How it works"
        description="Five of the methods behind the platform: each formula, what it assumes, how its settings were chosen, and what result would prove it wrong. Results from the nightly stock model are under Performance."
        related={[
          { href: '/performance#years', label: 'What drives signals' },
          { href: '/calls?view=signals', label: 'Trade signals' },
          { href: '/performance#years', label: 'Backtests' },
        ]}
      />

      {/* Main Layout */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {/* Sidebar Selector */}
        <div className="flex flex-col gap-2">
          <span className="text-xs font-bold text-ink-dim uppercase tracking-wider px-1">
            The methods
          </span>
          {(methods ?? []).map((m) => {
            const isSelected = m.id === selectedId;
            return (
              <button
                key={m.id}
                onClick={() => setPickedId(m.id)}
                className={`flex flex-col items-start p-3 rounded-lg border text-left transition-all ${
                  isSelected
                    ? 'bg-accent/20 border-accent text-accent shadow-md shadow-accent/10'
                    : 'bg-raised/60 border-line text-ink-dim hover:border-line-strong hover:text-ink'
                }`}
              >
                <span className="text-xs font-bold text-ink">{m.name}</span>
                <span className="text-micro text-ink-dim mt-0.5">{m.category}</span>
              </button>
            );
          })}
        </div>

        {/* Detailed Methodology View */}
        <div className="md:col-span-3 flex flex-col gap-5 bg-raised/80 border border-line rounded-xl p-6">
          {failed ? (
            <EmptyState
              kind="error"
              title="Could not load this method"
              detail="The page shows only what the server publishes, so nothing older is shown in its place."
            />
          ) : current ? (
            <MethodDetail current={current} />
          ) : (
            <EmptyState kind="loading" title="Loading method" />
          )}
        </div>
      </div>
    </div>
  );
}
