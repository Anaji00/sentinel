'use client';

import React, { useEffect, useState } from 'react';
import { IconDocument } from './ui/icons';
import { formatNumber } from '../lib/format';

// Who owns what, and who owns whom, from 13F filings
// (services/collector-filings/ownership.py). Stakes are long positions as
// filed -- 45 days after quarter end, aggregated per manager -- so each row
// shows its period rather than implying a live position.

interface Holder {
  filer_id: string;
  filer: string;
  pct_outstanding: number | null;
  shares: number;
  value_usd: number;
  period: string;
}

interface HoldersResponse {
  ticker: string;
  holders: Holder[];
  tracked_institutions_pct: number;
  big_three_pct: number;
  note: string;
}

interface CrossHolding {
  holder: string;
  holder_ticker: string | null;
  held: string;
  held_ticker: string | null;
  pct: number;
  reverse_pct: number | null;
}

interface SharedHolder {
  filer_id: string;
  filer: string;
  pct_a: number;
  pct_b: number;
  big_three: boolean;
}

interface CommonOwnership {
  a: string;
  b: string;
  common_ownership_pct: number;
  shared_holders: SharedHolder[];
}

interface Interlock {
  person: string;
  companies: string[];
  roles: string[];
}

interface ContractRow {
  agency: string;
  amount_usd: number;
  period: string | null;
}

interface FilerHolding {
  ticker: string;
  issuer: string;
  value_usd: number;
  pct_outstanding: number | null;
  pct_of_portfolio: number;
}

interface FilerDetail {
  filer_id: string;
  name: string;
  period: string;
  listed_as: string | null;
  portfolio_value_usd: number;
  positions_total: number;
  positions_kept: number;
  holdings: FilerHolding[];
}

function pct(v: number | null | undefined, digits = 2): string {
  return v === null || v === undefined ? '—' : `${v.toFixed(digits)}%`;
}

function usd(v: number): string {
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  return `$${(v / 1e6).toFixed(0)}M`;
}

async function getJson<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(path);
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

export default function OwnershipPanel() {
  const [ticker, setTicker] = useState('XOM');
  const [compareTo, setCompareTo] = useState('CVX');
  const [holders, setHolders] = useState<HoldersResponse | null>(null);
  const [common, setCommon] = useState<CommonOwnership | null>(null);
  const [cross, setCross] = useState<CrossHolding[]>([]);
  const [filer, setFiler] = useState<FilerDetail | null>(null);
  const [interlocks, setInterlocks] = useState<Interlock[]>([]);
  const [contracts, setContracts] = useState<ContractRow[]>([]);
  const [loading, setLoading] = useState(false);

  async function load(t: string, c: string) {
    setLoading(true);
    const [h, co] = await Promise.all([
      getJson<HoldersResponse>(`/api/proxy/api/v1/ownership/holders/${encodeURIComponent(t)}`),
      c ? getJson<CommonOwnership>(`/api/proxy/api/v1/ownership/common?a=${encodeURIComponent(t)}&b=${encodeURIComponent(c)}`) : null,
    ]);
    setHolders(h);
    setCommon(co);
    const [il, ct] = await Promise.all([
      getJson<Interlock[]>(`/api/proxy/api/v1/ownership/interlocks?ticker=${encodeURIComponent(t)}`),
      getJson<ContractRow[]>(`/api/proxy/api/v1/ownership/contracts/${encodeURIComponent(t)}`),
    ]);
    setInterlocks(il || []);
    setContracts(ct || []);
    setLoading(false);
  }

  useEffect(() => {
    load(ticker, compareTo);
    getJson<CrossHolding[]>('/api/proxy/api/v1/ownership/cross-holdings').then((rows) => setCross(rows || []));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function openFiler(id: string) {
    setFiler(await getJson<FilerDetail>(`/api/proxy/api/v1/ownership/filer/${encodeURIComponent(id)}?limit=25`));
  }

  return (
    <div className="flex flex-col gap-4 bg-raised/90 border border-line rounded-xl p-5 shadow-2xl text-ink">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-line pb-4">
        <div>
          <div className="flex items-center gap-2">
            <IconDocument className="text-cyan-400" size={20} />
            <h2 className="text-lg font-bold tracking-wide text-white">Follow the Money: Who Owns Whom</h2>
          </div>
          <p className="text-xs text-ink-dim mt-0.5">
            Institutional stakes as a share of each company, from 13F filings. Long positions as filed,
            45 days after quarter end, per manager.
          </p>
        </div>
        <form
          className="flex items-center gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            load(ticker.trim().toUpperCase(), compareTo.trim().toUpperCase());
          }}
        >
          <input
            value={ticker}
            onChange={(e) => setTicker(e.target.value)}
            aria-label="Ticker"
            className="w-24 px-2 py-1.5 text-sm rounded-md bg-page border border-line text-white uppercase"
          />
          <span className="text-xs text-ink-dim">vs</span>
          <input
            value={compareTo}
            onChange={(e) => setCompareTo(e.target.value)}
            aria-label="Compare with ticker"
            className="w-24 px-2 py-1.5 text-sm rounded-md bg-page border border-line text-white uppercase"
          />
          <button
            type="submit"
            className="px-3 py-1.5 text-xs font-semibold rounded-md bg-sky-500/10 text-sky-300 border border-sky-500/30 hover:bg-sky-500/20"
          >
            {loading ? 'Loading…' : 'Look up'}
          </button>
        </form>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <section className="p-3 bg-page/70 border border-line rounded-lg">
          <div className="flex items-baseline justify-between">
            <h3 className="text-sm font-bold text-white">Who holds {holders?.ticker || ticker.toUpperCase()}</h3>
            {holders && (
              <span className="text-xs text-ink-dim">
                Big Three {pct(holders.big_three_pct, 1)} · tracked institutions{' '}
                {pct(holders.tracked_institutions_pct, 1)}
              </span>
            )}
          </div>
          {!holders || holders.holders.length === 0 ? (
            <p className="text-xs text-ink-dim mt-2">No reported holders collected for this ticker yet.</p>
          ) : (
            <table className="w-full mt-2 text-xs">
              <thead className="text-ink-dim">
                <tr>
                  <th className="text-left font-medium py-1">Institution</th>
                  <th className="text-right font-medium">% of shares</th>
                  <th className="text-right font-medium">Value</th>
                  <th className="text-right font-medium">Period</th>
                </tr>
              </thead>
              <tbody>
                {holders.holders.map((h) => (
                  <tr key={h.filer_id} className="border-t border-line/60">
                    <td className="py-1">
                      <button onClick={() => openFiler(h.filer_id)} className="text-sky-300 hover:underline text-left">
                        {h.filer}
                      </button>
                    </td>
                    <td className="text-right text-white">{pct(h.pct_outstanding)}</td>
                    <td className="text-right">{usd(h.value_usd)}</td>
                    <td className="text-right text-ink-dim">{h.period}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section className="p-3 bg-page/70 border border-line rounded-lg">
          <div className="flex items-baseline justify-between">
            <h3 className="text-sm font-bold text-white">
              Common ownership {common ? `${common.a} · ${common.b}` : ''}
            </h3>
            {common && <span className="text-xs text-amber-300">{pct(common.common_ownership_pct, 1)} held by the same institutions</span>}
          </div>
          {!common || common.shared_holders.length === 0 ? (
            <p className="text-xs text-ink-dim mt-2">No shared holders collected for this pair yet.</p>
          ) : (
            <table className="w-full mt-2 text-xs">
              <thead className="text-ink-dim">
                <tr>
                  <th className="text-left font-medium py-1">Institution</th>
                  <th className="text-right font-medium">{common.a}</th>
                  <th className="text-right font-medium">{common.b}</th>
                </tr>
              </thead>
              <tbody>
                {common.shared_holders.slice(0, 10).map((s) => (
                  <tr key={s.filer_id} className="border-t border-line/60">
                    <td className="py-1">
                      {s.filer}
                      {s.big_three && <span className="ml-1 text-micro text-amber-300">Big Three</span>}
                    </td>
                    <td className="text-right">{pct(s.pct_a)}</td>
                    <td className="text-right">{pct(s.pct_b)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <section className="p-3 bg-page/70 border border-line rounded-lg">
          <h3 className="text-sm font-bold text-white">Board interlocks</h3>
          <p className="text-xs text-ink-dim">
            Insiders of {holders?.ticker || ticker.toUpperCase()} who also report at other companies (Form 4).
          </p>
          {interlocks.length === 0 ? (
            <p className="text-xs text-ink-dim mt-2">None recorded yet; this builds up as Form 4s arrive.</p>
          ) : (
            <ul className="mt-2 text-xs space-y-1">
              {interlocks.slice(0, 12).map((i) => (
                <li key={i.person}>
                  <span className="text-white">{i.person}</span>{' '}
                  <span className="text-ink-dim">({i.roles.slice(0, 2).join(', ')})</span>: {i.companies.join(', ')}
                </li>
              ))}
            </ul>
          )}
        </section>
        <section className="p-3 bg-page/70 border border-line rounded-lg">
          <h3 className="text-sm font-bold text-white">Federal money in</h3>
          <p className="text-xs text-ink-dim">Agencies obligating contract money to this company (USAspending).</p>
          {contracts.length === 0 ? (
            <p className="text-xs text-ink-dim mt-2">No federal obligations recorded.</p>
          ) : (
            <ul className="mt-2 text-xs space-y-1">
              {contracts.map((c) => (
                <li key={c.agency} className="flex justify-between gap-2">
                  <span>{c.agency}</span>
                  <span className="text-white">{usd(c.amount_usd)}</span>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>

      <section className="p-3 bg-page/70 border border-line rounded-lg">
        <h3 className="text-sm font-bold text-white">Institutions that own each other</h3>
        <p className="text-xs text-ink-dim">
          A listed manager or bank holding another listed one. Where both directions are reported, the
          two own each other.
        </p>
        {cross.length === 0 ? (
          <p className="text-xs text-ink-dim mt-2">No cross-holdings collected yet.</p>
        ) : (
          <table className="w-full mt-2 text-xs">
            <thead className="text-ink-dim">
              <tr>
                <th className="text-left font-medium py-1">Holder</th>
                <th className="text-left font-medium">Holds</th>
                <th className="text-right font-medium">Stake</th>
                <th className="text-right font-medium">Back the other way</th>
              </tr>
            </thead>
            <tbody>
              {cross.slice(0, 30).map((r) => (
                <tr key={`${r.holder}-${r.held}`} className="border-t border-line/60">
                  <td className="py-1">
                    {r.holder} {r.holder_ticker && <span className="text-ink-dim">({r.holder_ticker})</span>}
                  </td>
                  <td>
                    {r.held} {r.held_ticker && <span className="text-ink-dim">({r.held_ticker})</span>}
                  </td>
                  <td className="text-right text-white">{pct(r.pct)}</td>
                  <td className={`text-right ${r.reverse_pct ? 'text-amber-300' : 'text-ink-dim'}`}>
                    {r.reverse_pct ? pct(r.reverse_pct) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {filer && (
        <section className="p-3 bg-page/70 border border-line rounded-lg">
          <div className="flex items-baseline justify-between">
            <h3 className="text-sm font-bold text-white">
              {filer.name} {filer.listed_as && <span className="text-ink-dim">({filer.listed_as})</span>}
            </h3>
            <span className="text-xs text-ink-dim">
              {usd(filer.portfolio_value_usd)} across {formatNumber(filer.positions_total, { decimals: 0 })} positions ·{' '}
              {filer.period}
            </span>
          </div>
          <table className="w-full mt-2 text-xs">
            <thead className="text-ink-dim">
              <tr>
                <th className="text-left font-medium py-1">Company</th>
                <th className="text-right font-medium">Value</th>
                <th className="text-right font-medium">% of company</th>
                <th className="text-right font-medium">% of portfolio</th>
              </tr>
            </thead>
            <tbody>
              {filer.holdings.map((h) => (
                <tr key={h.ticker} className="border-t border-line/60">
                  <td className="py-1">
                    <span className="text-white">{h.ticker}</span> <span className="text-ink-dim">{h.issuer}</span>
                  </td>
                  <td className="text-right">{usd(h.value_usd)}</td>
                  <td className="text-right text-white">{pct(h.pct_outstanding)}</td>
                  <td className="text-right">{pct(h.pct_of_portfolio)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
