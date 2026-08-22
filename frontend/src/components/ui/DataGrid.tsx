import React from 'react';
import {
  ABSENT,
  formatNumber,
  formatTimestamp,
  formatSymbol,
  isPresent,
} from '../../lib/format';

/**
 * Structured renderer for arbitrary record payloads.
 *
 * Replaces `<pre>{JSON.stringify(obj, null, 2)}</pre>`, which shipped raw
 * transport shape to the operator: snake_case keys, 17-digit floats, epoch
 * integers, and whatever nesting the producer happened to emit. A detail panel
 * is read under time pressure; it should present fields, not a serialization.
 *
 * Values are formatted by inferred type, keys are humanized, and nesting is
 * flattened into dotted paths so the grid stays scannable at any depth.
 */

/** Keys whose values are timestamps regardless of their runtime type. */
const TEMPORAL_KEY = /(^|_)(at|ts|time|timestamp|date|expiry|expires|updated|created|seen)$/i;

/** Keys that carry identifiers -- rendered monospace, never numeric-formatted. */
const IDENTIFIER_KEY = /(^|_)(id|uuid|hash|key|symbol|ticker|mmsi|cik|icao)$/i;

/** Epoch values below this are seconds; above, milliseconds. */
const EPOCH_MS_THRESHOLD = 1e11;

export interface DataGridProps {
  data: unknown;
  /** Fields to hide, e.g. bulky nested blobs already shown elsewhere. */
  omit?: string[];
  /** Maximum nesting depth to flatten. Deeper values render as a summary. */
  maxDepth?: number;
  /** Rendered when there is nothing to show. */
  emptyLabel?: string;
  className?: string;
}

interface Row {
  path: string;
  label: string;
  value: unknown;
}

/** "open_interest" -> "Open Interest"; "z_score" -> "Z Score". */
function humanizeKey(key: string): string {
  return key
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/** Flattens nested records into dotted-path rows. */
function flatten(input: unknown, omit: Set<string>, maxDepth: number, depth = 0, prefix = ''): Row[] {
  if (!isPlainObject(input)) return [];

  const rows: Row[] = [];
  for (const [key, value] of Object.entries(input)) {
    if (omit.has(key)) continue;
    const path = prefix ? `${prefix}.${key}` : key;

    if (isPlainObject(value) && depth < maxDepth) {
      const nested = flatten(value, omit, maxDepth, depth + 1, path);
      if (nested.length > 0) {
        rows.push(...nested);
        continue;
      }
    }
    rows.push({ path, label: humanizeKey(key), value });
  }
  return rows;
}

/** Renders one value according to its inferred semantic type. */
const Value: React.FC<{ path: string; value: unknown }> = ({ path, value }) => {
  const key = path.split('.').pop() ?? path;

  if (value === null || value === undefined || value === '') {
    return <span className="text-slate-600">{ABSENT}</span>;
  }

  if (typeof value === 'boolean') {
    return (
      <span
        className={`px-1.5 py-0.5 rounded text-[9px] font-bold tracking-wider border ${
          value
            ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
            : 'bg-slate-700/20 text-slate-400 border-slate-600/40'
        }`}
      >
        {value ? 'TRUE' : 'FALSE'}
      </span>
    );
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-slate-600">{ABSENT}</span>;
    const scalars = value.filter((v) => typeof v !== 'object' || v === null);
    if (scalars.length === value.length) {
      return (
        <span className="flex flex-wrap gap-1 justify-end">
          {value.slice(0, 8).map((v, i) => (
            <span key={i} className="px-1.5 py-0.5 bg-slate-800/70 rounded text-[9px] text-slate-300">
              {String(v)}
            </span>
          ))}
          {value.length > 8 && <span className="text-slate-500 text-[9px]">+{value.length - 8}</span>}
        </span>
      );
    }
    return <span className="text-slate-400 text-[10px]">{value.length} records</span>;
  }

  // Identifiers stay verbatim -- formatting a CIK or MMSI as a number inserts
  // thousands separators into something that is not a quantity.
  if (IDENTIFIER_KEY.test(key)) {
    return <span className="font-mono text-slate-300 break-all">{String(value)}</span>;
  }

  if (TEMPORAL_KEY.test(key)) {
    if (typeof value === 'number') {
      const ms = value > EPOCH_MS_THRESHOLD ? value : value * 1000;
      return <span className="text-slate-300 tabular-nums">{formatTimestamp(ms)}</span>;
    }
    const rendered = formatTimestamp(String(value));
    if (rendered !== ABSENT) return <span className="text-slate-300 tabular-nums">{rendered}</span>;
  }

  if (typeof value === 'number') {
    if (!isPresent(value)) return <span className="text-slate-600">{ABSENT}</span>;
    // Integers keep their exact value; fractional values are bounded so a
    // 17-digit float cannot reach the panel.
    const decimals = Number.isInteger(value) ? 0 : Math.abs(value) < 1 ? 4 : 2;
    return <span className="text-cyan-300 tabular-nums">{formatNumber(value, { decimals })}</span>;
  }

  if (isPlainObject(value)) {
    const n = Object.keys(value).length;
    return <span className="text-slate-500 text-[10px] italic">{n} field{n === 1 ? '' : 's'}</span>;
  }

  const text = String(value);
  if (key.toLowerCase() === 'ticker' || key.toLowerCase() === 'symbol') {
    return <span className="font-mono text-slate-200">{formatSymbol(text)}</span>;
  }
  return <span className="text-slate-300 break-words">{text}</span>;
};

export const DataGrid: React.FC<DataGridProps> = ({
  data,
  omit = [],
  maxDepth = 2,
  emptyLabel = 'No structured detail available',
  className = '',
}) => {
  const rows = React.useMemo(
    () => flatten(data, new Set(omit), maxDepth),
    [data, omit, maxDepth],
  );

  if (rows.length === 0) {
    return (
      <div className={`px-3 py-4 text-center text-[10px] text-slate-500 border border-slate-800 rounded-xl bg-slate-950/60 ${className}`}>
        {emptyLabel}
      </div>
    );
  }

  return (
    <dl
      className={`divide-y divide-slate-800/70 border border-slate-800 rounded-xl bg-slate-950/60 overflow-hidden ${className}`}
    >
      {rows.map(({ path, label, value }) => (
        <div key={path} className="flex items-start justify-between gap-3 px-3 py-1.5 text-[10px]">
          <dt className="text-slate-500 shrink-0" title={path}>
            {label}
          </dt>
          <dd className="text-right min-w-0 font-medium">
            <Value path={path} value={value} />
          </dd>
        </div>
      ))}
    </dl>
  );
};

export default DataGrid;
