/**
 * @file Canonical display formatters.
 *
 * Every number the operator reads passes through here. Before this module the
 * codebase carried 130+ ad-hoc `toFixed()` calls, 25 `toLocaleString()` calls
 * and 25 hand-rolled `* 100` conversions, so the same quantity could render
 * three different ways on three panels.
 *
 * Two rules hold throughout:
 *
 *   1. ABSENCE IS NOT ZERO. A null, undefined or NaN input renders as an em
 *      dash, never as "0", "0.00" or "NaN". This is the display-layer half of
 *      the platform's zero-fabrication rule -- see ProvenanceValue, which
 *      refuses to print a number it cannot attribute.
 *
 *   2. PERCENT CONVENTION IS EXPLICIT. The backend emits some fields already
 *      scaled (`var_95_pct = 2.4` meaning 2.4%) and others as ratios
 *      (`confidence = 0.87`). Callers must say which they have, so a value can
 *      never be silently scaled twice.
 */

/** Rendered in place of any value that is absent, unmeasured, or not finite. */
export const ABSENT = '—';

/** True when a value is a real, finite number safe to display. */
export function isPresent(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function coerce(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === '') return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

export interface NumberOptions {
  /** Fixed decimal places. Default 2. */
  decimals?: number;
  /** Rendered when the value is absent. Default ABSENT. */
  fallback?: string;
  /** Prefix a "+" on positive values, for deltas. */
  signed?: boolean;
}

/**
 * Plain number with thousands separators.
 *
 *   formatNumber(1234.5)            // "1,234.50"
 *   formatNumber(1234.5, {decimals: 0}) // "1,235"
 *   formatNumber(null)              // "—"
 */
export function formatNumber(
  value: number | string | null | undefined,
  { decimals = 2, fallback = ABSENT, signed = false }: NumberOptions = {},
): string {
  const n = coerce(value);
  if (n === null) return fallback;
  const body = n.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
  return signed && n > 0 ? `+${body}` : body;
}

/**
 * Compact magnitude for large counts: 1.2K, 3.4M, 5.6B, 7.8T.
 * Values below 1000 render in full.
 */
export function formatCompact(
  value: number | string | null | undefined,
  { decimals = 1, fallback = ABSENT, signed = false }: NumberOptions = {},
): string {
  const n = coerce(value);
  if (n === null) return fallback;

  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : signed ? '+' : '';

  const units: Array<[number, string]> = [
    [1e12, 'T'],
    [1e9, 'B'],
    [1e6, 'M'],
    [1e3, 'K'],
  ];
  for (const [scale, suffix] of units) {
    if (abs >= scale) return `${sign}${(abs / scale).toFixed(decimals)}${suffix}`;
  }
  return `${sign}${abs.toFixed(decimals)}`;
}

export interface CurrencyOptions extends NumberOptions {
  /** Abbreviate large values as $1.2B rather than $1,200,000,000. Default true. */
  compact?: boolean;
  /** Currency symbol. Default "$". */
  symbol?: string;
}

/**
 * Monetary value.
 *
 *   formatCurrency(247_100_000_000)              // "$247.10B"
 *   formatCurrency(1234.5, {compact: false})     // "$1,234.50"
 *   formatCurrency(undefined)                    // "—"
 */
export function formatCurrency(
  value: number | string | null | undefined,
  { decimals, fallback = ABSENT, signed = false, compact = true, symbol = '$' }: CurrencyOptions = {},
): string {
  const n = coerce(value);
  if (n === null) return fallback;

  const sign = n < 0 ? '-' : signed ? '+' : '';
  const abs = Math.abs(n);

  // Precision differs by notation: abbreviated magnitudes conventionally carry
  // one decimal ($247.1B), full amounts carry two ($1,234.50). Resolved per
  // path so an unset `decimals` does not force cent-precision onto billions.
  if (compact && abs >= 1e3) {
    return `${sign}${symbol}${formatCompact(abs, { decimals: decimals ?? 1 })}`;
  }
  const places = decimals ?? 2;
  return `${sign}${symbol}${abs.toLocaleString(undefined, {
    minimumFractionDigits: places,
    maximumFractionDigits: places,
  })}`;
}

export interface PercentOptions extends NumberOptions {
  /**
   * Which convention the input uses.
   *   'percent' (default) -- already scaled, 12.5 means 12.5%
   *   'ratio'             -- 0..1, 0.125 means 12.5%
   *
   * Stated explicitly because the backend emits both, and guessing produces
   * values wrong by a factor of 100 that still look plausible on screen.
   */
  from?: 'percent' | 'ratio';
}

/**
 * Percentage.
 *
 *   formatPercent(12.5)                      // "12.50%"
 *   formatPercent(0.125, {from: 'ratio'})    // "12.50%"
 *   formatPercent(null)                      // "—"
 */
export function formatPercent(
  value: number | string | null | undefined,
  { decimals = 2, fallback = ABSENT, signed = false, from = 'percent' }: PercentOptions = {},
): string {
  const n = coerce(value);
  if (n === null) return fallback;
  const scaled = from === 'ratio' ? n * 100 : n;
  const body = scaled.toFixed(decimals);
  return `${scaled > 0 && signed ? '+' : ''}${body}%`;
}

/**
 * Basis points, for spreads and funding rates where percent loses resolution.
 *   formatBps(0.0025, {from: 'ratio'})  // "25.0 bps"
 */
export function formatBps(
  value: number | string | null | undefined,
  { decimals = 1, fallback = ABSENT, from = 'ratio' }: PercentOptions = {},
): string {
  const n = coerce(value);
  if (n === null) return fallback;
  const bps = from === 'ratio' ? n * 10_000 : n * 100;
  return `${bps.toFixed(decimals)} bps`;
}

/**
 * A dimensionless ratio such as Sharpe, beta or a hedge ratio. Deliberately
 * carries no unit suffix -- appending "x" to a Sharpe ratio misrepresents it as
 * a multiple.
 */
export function formatRatio(
  value: number | string | null | undefined,
  opts: NumberOptions = {},
): string {
  return formatNumber(value, { decimals: 2, ...opts });
}

/** Absolute UTC timestamp, e.g. "2026-08-21 14:32:05Z". */
export function formatTimestamp(
  value: string | number | Date | null | undefined,
  { fallback = ABSENT, seconds = true }: { fallback?: string; seconds?: boolean } = {},
): string {
  if (value === null || value === undefined || value === '') return fallback;
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return fallback;

  const iso = d.toISOString();
  return seconds ? `${iso.slice(0, 10)} ${iso.slice(11, 19)}Z` : `${iso.slice(0, 10)} ${iso.slice(11, 16)}Z`;
}

/** Calendar date only, e.g. "2026-08-21". */
export function formatDate(
  value: string | number | Date | null | undefined,
  { fallback = ABSENT }: { fallback?: string } = {},
): string {
  if (value === null || value === undefined || value === '') return fallback;
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? fallback : d.toISOString().slice(0, 10);
}

/**
 * Compact age relative to now: "12s", "4m", "3h", "6d".
 * Future timestamps render as "now" rather than a negative age.
 */
export function formatAge(
  value: string | number | Date | null | undefined,
  { fallback = ABSENT, now = Date.now() }: { fallback?: string; now?: number } = {},
): string {
  if (value === null || value === undefined || value === '') return fallback;
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return fallback;

  const secs = Math.floor((now - d.getTime()) / 1000);
  if (secs < 0) return 'now';
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  if (secs < 86_400) return `${Math.floor(secs / 3600)}h`;
  return `${Math.floor(secs / 86_400)}d`;
}

/** Elapsed duration from a second count: "45s", "12m 30s", "2h 05m". */
export function formatDuration(
  seconds: number | null | undefined,
  { fallback = ABSENT }: { fallback?: string } = {},
): string {
  const n = coerce(seconds);
  if (n === null || n < 0) return fallback;

  if (n < 60) return `${Math.round(n)}s`;
  if (n < 3600) {
    const m = Math.floor(n / 60);
    const s = Math.round(n % 60);
    return `${m}m ${String(s).padStart(2, '0')}s`;
  }
  const h = Math.floor(n / 3600);
  const m = Math.floor((n % 3600) / 60);
  return `${h}h ${String(m).padStart(2, '0')}m`;
}

/** Ticker/symbol normalization for display: trimmed, uppercased. */
export function formatSymbol(value: string | null | undefined, fallback = ABSENT): string {
  const s = (value ?? '').trim().toUpperCase();
  return s || fallback;
}

/**
 * Masks a secret for display, revealing only the last `visible` characters.
 * Returns a value that is safe to render but NOT safe to copy as a credential.
 */
export function maskSecret(
  value: string | null | undefined,
  { visible = 4, fallback = ABSENT }: { visible?: number; fallback?: string } = {},
): string {
  const s = (value ?? '').trim();
  if (!s) return fallback;
  if (s.length <= visible) return '•'.repeat(s.length);
  return `${'•'.repeat(Math.min(16, s.length - visible))}${s.slice(-visible)}`;
}
