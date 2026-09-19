'use client';

/**
 * The control that gets a table out.
 *
 * Deliberately refuses rather than exporting nothing: a zero-row CSV is
 * indistinguishable from a broken export once it is sitting in a downloads
 * folder, and the panel already knows whether it has rows.
 */

import React from 'react';
import { csvFilename, downloadCsv, toCsv, type CsvColumn } from '../../lib/csv';
import { IconDocument } from './icons';

interface ExportButtonProps<T> {
  /** What is being exported, used for the filename. e.g. "strategy results". */
  subject: string;
  rows: readonly T[];
  columns: readonly CsvColumn<T>[];
  className?: string;
}

export function ExportButton<T>({ subject, rows, columns, className = '' }: ExportButtonProps<T>) {
  const empty = rows.length === 0;
  return (
    <button
      type="button"
      disabled={empty}
      onClick={() => downloadCsv(csvFilename(subject), toCsv(rows, columns))}
      title={empty ? 'Nothing to export yet' : `Export ${rows.length} rows as CSV`}
      className={`flex items-center gap-1.5 rounded-md border border-line px-2 py-0.5 text-micro font-medium text-ink-dim transition-colors enabled:cursor-pointer enabled:hover:border-line-strong enabled:hover:text-ink disabled:opacity-40 ${className}`}
    >
      <IconDocument />
      Export
    </button>
  );
}

export default ExportButton;
