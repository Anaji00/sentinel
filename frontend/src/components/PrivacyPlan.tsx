'use client';

/**
 * A personal privacy plan (/civic/plan; shared/utils/privacy_plan.py).
 * A few answers in, a ranked list of free steps out. Answers go in the request
 * body and are not stored; until a plan is made, the general steps show.
 */

import React, { useState } from 'react';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';

export interface PlanStep {
  id: string;
  title: string;
  why?: string;
  how?: string;
  detail?: string;
  link?: string;
  links?: { label: string; url: string }[];
}

export const CONCERN_LABEL: Record<string, string> = {
  immigration: 'Immigration enforcement',
  reproductive: 'Reproductive health care',
  activism_press: 'Activism or journalism',
  domestic_violence: 'Someone who may try to find me',
};

const STATES = 'AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY'.split(' ');

/** The request body: only what was chosen, nothing else. */
export function planBody(state: string, phone: string, hasCar: boolean, doorbell: boolean, concerns: string[]) {
  return {
    state: state || null,
    phone: phone === 'android' || phone === 'iphone' ? phone : null,
    has_car: hasCar,
    has_doorbell_camera: doorbell,
    concerns: concerns.filter((c) => c in CONCERN_LABEL),
  };
}

function StepItem({ s }: { s: PlanStep }) {
  const links = s.links ?? (s.link ? [{ label: s.title, url: s.link }] : []);
  return (
    <li>
      <div className="text-ink">{s.title}</div>
      {s.why && <div className="text-micro text-ink-dim">{s.why}</div>}
      <div className="text-micro text-ink-mute">{s.how ?? s.detail}</div>
      {links.length > 0 && (
        <div className="flex flex-wrap gap-x-2 text-micro">
          {links.slice(0, 30).map((l) => (
            <a key={l.url} href={l.url} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
              {l.label}
            </a>
          ))}
        </div>
      )}
    </li>
  );
}

export function PrivacyPlan() {
  const { data: general } = useSWR<{ steps: PlanStep[] }>('/civic/privacy', fetcher);
  const [state, setState] = useState('');
  const [phone, setPhone] = useState('');
  const [hasCar, setHasCar] = useState(false);
  const [doorbell, setDoorbell] = useState(false);
  const [concerns, setConcerns] = useState<string[]>([]);
  const [plan, setPlan] = useState<{ steps: PlanStep[]; note: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);

  const make = async () => {
    setBusy(true);
    setFailed(false);
    try {
      const res = await apiClient.post('/civic/plan', planBody(state, phone, hasCar, doorbell, concerns));
      setPlan(res.data);
    } catch {
      setFailed(true);
    } finally {
      setBusy(false);
    }
  };

  const steps = plan?.steps ?? general?.steps ?? [];
  return (
    <div className="space-y-2">
      <div className="text-ink font-bold">{plan ? 'Your privacy plan' : 'Protect yourself'}</div>
      <div className="grid grid-cols-2 gap-1 text-micro text-ink-dim">
        <label>
          State{' '}
          <select value={state} onChange={(e) => setState(e.target.value)} className="bg-base border border-line rounded px-1">
            <option value="">—</option>
            {STATES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <label>
          Phone{' '}
          <select value={phone} onChange={(e) => setPhone(e.target.value)} className="bg-base border border-line rounded px-1">
            <option value="">—</option>
            <option value="iphone">iPhone</option>
            <option value="android">Android</option>
          </select>
        </label>
        <label>
          <input type="checkbox" checked={hasCar} onChange={(e) => setHasCar(e.target.checked)} /> I drive
        </label>
        <label>
          <input type="checkbox" checked={doorbell} onChange={(e) => setDoorbell(e.target.checked)} /> Doorbell camera
        </label>
      </div>
      <fieldset className="text-micro text-ink-dim">
        <legend className="text-ink-mute">Anything you are especially concerned about? (optional)</legend>
        <div className="grid grid-cols-2 gap-1">
          {Object.entries(CONCERN_LABEL).map(([k, label]) => (
            <label key={k}>
              <input
                type="checkbox"
                checked={concerns.includes(k)}
                onChange={(e) => setConcerns(e.target.checked ? [...concerns, k] : concerns.filter((c) => c !== k))}
              />{' '}
              {label}
            </label>
          ))}
        </div>
      </fieldset>
      <button
        type="button"
        onClick={() => void make()}
        disabled={busy}
        className="px-2 py-0.5 rounded border border-accent text-accent text-micro font-bold disabled:opacity-50"
      >
        {busy ? 'Making your plan…' : 'Make my plan'}
      </button>
      {failed && <p className="text-micro text-negative">The plan could not be made; the general steps are below.</p>}
      <ol className="space-y-1 list-decimal pl-4">
        {steps.map((s) => (
          <StepItem key={s.id} s={s} />
        ))}
      </ol>
      <p className="text-micro text-ink-mute">
        {plan?.note ?? 'Answers are used for your plan only and are not saved.'}
      </p>
    </div>
  );
}

export default PrivacyPlan;
