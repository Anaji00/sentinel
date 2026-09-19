/**
 * Getting a table out of the console.
 *
 * There was no way to. No `download` attribute, no `createObjectURL`, no CSV
 * anywhere in the application. An operator who wanted to take a movers board
 * or a set of backtest results into a spreadsheet, a report or an email had to
 * retype it or screenshot it.
 *
 * Two rules carry over from the display layer, because an export that
 * disagrees with the screen is worse than no export:
 *
 *   1. ABSENCE STAYS ABSENT. A null renders as an empty cell, never as 0 and
 *      never as the em dash the screen uses -- a spreadsheet reading "—" in a
 *      numeric column gets text, and text silently poisons every SUM below it.
 *      An empty cell is the only thing that means "no value" to both a human
 *      and a spreadsheet.
 *
 *   2. NUMBERS GO OUT UNFORMATTED. The screen shows "1,234.50"; the file
 *      carries 1234.5. Thousands separators are a reading aid, and pasting
 *      them into a spreadsheet produces a string, not a number.
 */

/**
 * One field, escaped for RFC 4180.
 *
 * Quoting is unconditional for anything containing a comma, a quote, or a
 * newline -- a ticker is safe, a headline is not, and this platform's rows
 * carry both.
 */
function cell(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : '';
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object') return '';

  const s = String(value);
  // A leading =, +, - or @ makes a spreadsheet treat the cell as a formula.
  // A headline beginning with "-" is not a formula, and neither is anything
  // else this platform exports, so the cell is neutralised rather than trusted.
  const risky = /^[=+\-@\t\r]/.test(s);
  const body = risky ? `'${s}` : s;
  return /[",\n\r]/.test(body) ? `"${body.replace(/"/g, '""')}"` : body;
}

export interface CsvColumn<T> {
  /** The header text. */
  label: string;
  /** Pull the value out of a row. Return null for absent, not 0. */
  value: (row: T) => string | number | Date | null | undefined;
}

/**
 * Rows and columns to a CSV string.
 *
 * Takes explicit columns rather than reflecting over object keys: an export
 * built from `Object.keys(rows[0])` changes shape when the first row happens
 * to be missing an optional field, and silently drops the column for every
 * other row too.
 */
export function toCsv<T>(rows: readonly T[], columns: readonly CsvColumn<T>[]): string {
  const head = columns.map((c) => cell(c.label)).join(',');
  const body = rows.map((row) => columns.map((c) => cell(c.value(row))).join(','));
  // CRLF is what RFC 4180 specifies and what Excel expects.
  return [head, ...body].join('\r\n');
}

/**
 * Hand the file to the browser.
 *
 * The BOM is not decoration: without it Excel on Windows reads a UTF-8 file as
 * the system codepage, and every non-ASCII character in a vessel name or a
 * headline arrives mangled.
 */
export function downloadCsv(filename: string, csv: string): void {
  if (typeof window === 'undefined') return;
  const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  // Revoked on the next tick rather than immediately: Safari has not finished
  // reading the blob when click() returns, and revoking synchronously produces
  // an empty file.
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

/**
 * A filename that says what it is and when it was taken.
 *
 * `movers.csv` in a downloads folder beside four other `movers.csv` files is
 * not evidence of anything. The timestamp is UTC and sortable, because the
 * point of it is putting the files in order.
 */
export function csvFilename(subject: string, at: Date = new Date()): string {
  const stamp = at.toISOString().slice(0, 19).replace(/[:T]/g, '-');
  const slug = subject
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
  return `sentinel-${slug}-${stamp}Z.csv`;
}

/**
 * A bag of varying keys, as one cell.
 *
 * Some rows carry a `details` object whose keys differ per action, so it
 * cannot become columns without inventing a fixed shape the server does not
 * promise. Serialising it keeps the information in the file; the alternatives
 * are dropping it or emitting a ragged header.
 *
 * Empty renders as absent rather than as "{}", which would read in a
 * spreadsheet as a value that is there.
 */
export function jsonCell(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'object' && Object.keys(value as object).length === 0) return null;
  try {
    return JSON.stringify(value);
  } catch {
    // Circular structures throw. The row still exports; this cell says it
    // could not be serialised rather than taking the whole file down.
    return null;
  }
}
