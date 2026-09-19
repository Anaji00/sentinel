'use client';

/**
 * The report generator, which had a catalogue nobody could choose from.
 *
 * `/reports/templates` advertises three documents -- a daily executive brief, a
 * weekly review, an incident flash report -- each with its own sections and
 * default window. `/reports/generate` produces one. Neither had a caller, so
 * the catalogue described choices no person could make.
 *
 * This document has already recorded the other half of that: the generator once
 * had no `template_id` field at all, so the three ids were advertised with
 * prose describing different content and picking one could not change
 * anything. It takes the field now, and refuses an unknown id rather than
 * quietly serving the default -- a caller who asked for an incident flash
 * report and got a daily brief has no way to tell.
 *
 * The markdown is rendered and also offered as a file. The rendering is for
 * reading; the file is the artefact, and an analyst circulating a brief needs
 * the second.
 */

import React from 'react';
import useSWR from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { Markdown } from './ui/Markdown';
import { ClockTime } from './ui/ClockTime';
import { useFeedback } from './ui/Feedback';
import { POLL } from './ui/DataProvider';
import { IconDocument } from './ui/icons';
import { csvFilename } from '../lib/csv';
import { formatNumber } from '../lib/format';

interface Template {
  id: string;
  name: string;
  description: string;
  default_timeframe_hours: number;
  sections: string[];
}

interface GeneratedReport {
  report_id: string;
  title: string;
  generated_at: string;
  timeframe_hours: number;
  template_id: string;
  sections: string[];
  author: string;
  markdown: string;
  events_count: number;
  correlations_count: number;
  scenarios_count: number;
  system_status: string;
}

/** The server accepts 1–168; these are the ones worth a button. */
const WINDOWS = [6, 24, 72, 168] as const;

function downloadMarkdown(report: GeneratedReport) {
  if (typeof window === 'undefined') return;
  const blob = new Blob([report.markdown], { type: 'text/markdown;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = csvFilename(report.report_id).replace(/\.csv$/, '.md');
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

export default function ReportBuilder() {
  const { toast } = useFeedback();
  const { data: catalogue, error: catalogueError } = useSWR<{ templates: Template[] }>(
    '/reports/templates',
    fetcher,
    // A catalogue changes when someone deploys, not while someone reads it.
    { refreshInterval: POLL.rare },
  );

  const templates = catalogue?.templates ?? [];
  const [templateId, setTemplateId] = React.useState<string | null>(null);
  const [hours, setHours] = React.useState<number | null>(null);
  const [report, setReport] = React.useState<GeneratedReport | null>(null);
  const [busy, setBusy] = React.useState(false);

  const selected = templates.find((t) => t.id === templateId) ?? templates[0] ?? null;
  // The template's own default until the operator overrides it, so choosing a
  // weekly review does not silently keep a 24-hour window.
  const effectiveHours = hours ?? selected?.default_timeframe_hours ?? 24;

  const generate = async () => {
    if (!selected) return;
    setBusy(true);
    try {
      const res = await apiClient.post<GeneratedReport>('/reports/generate', {
        template_id: selected.id,
        timeframe_hours: effectiveHours,
      });
      setReport(res.data);
    } catch (err) {
      toast('error', 'The report was not generated.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  if (catalogueError) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Report templates unavailable"
          detail={describeApiError(catalogueError) ?? undefined}
        />
      </Card>
    );
  }

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title flex items-center gap-1.5">
            <IconDocument />
            Intelligence briefs
          </h2>
          <p className="panel-subtitle">
            {templates.length
              ? `${templates.length} templates over the platform's own window`
              : 'Reading the catalogue…'}
          </p>
        </div>
        {report && (
          <button
            onClick={() => downloadMarkdown(report)}
            className="cursor-pointer rounded-md border border-line px-2 py-0.5 text-micro font-medium text-ink-dim transition-colors hover:border-line-strong hover:text-ink"
          >
            Download .md
          </button>
        )}
      </div>

      <div className="shrink-0 space-y-2.5 border-b border-line px-3.5 py-3">
        <div className="space-y-1">
          <span className="stat-label">Template</span>
          <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-3">
            {templates.map((t) => (
              <button
                key={t.id}
                onClick={() => {
                  setTemplateId(t.id);
                  // Back to that template's own default window.
                  setHours(null);
                }}
                aria-pressed={selected?.id === t.id}
                className={`cursor-pointer rounded-lg border p-2.5 text-left transition-colors ${
                  selected?.id === t.id
                    ? 'border-line-accent bg-accent-dim'
                    : 'border-line hover:border-line-strong'
                }`}
              >
                <span
                  className={`block text-xs font-medium ${
                    selected?.id === t.id ? 'text-accent' : 'text-ink'
                  }`}
                >
                  {t.name}
                </span>
                <span className="mt-0.5 block text-micro leading-relaxed text-ink-mute">
                  {t.description}
                </span>
                <span className="mt-1 block text-micro text-ink-mute">
                  {t.sections.join(' · ')}
                </span>
              </button>
            ))}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-1.5">
          <span className="stat-label mr-0.5">Window</span>
          {WINDOWS.map((h) => (
            <button
              key={h}
              onClick={() => setHours(h)}
              aria-pressed={effectiveHours === h}
              className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
                effectiveHours === h
                  ? 'border-line-accent bg-accent-dim text-accent'
                  : 'border-line text-ink-mute hover:text-ink-dim'
              }`}
            >
              {h}h
            </button>
          ))}
          {hours === null && selected && (
            <span className="text-micro text-ink-mute">this template&rsquo;s default</span>
          )}
          <button
            onClick={generate}
            disabled={!selected || busy}
            className="ml-auto rounded-md border border-line-accent bg-accent-dim px-2.5 py-0.5 text-micro font-semibold text-accent transition-colors enabled:cursor-pointer enabled:hover:border-accent disabled:opacity-40"
          >
            {busy ? 'Generating…' : 'Generate'}
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-3.5 py-3">
        {busy && !report ? (
          <EmptyState kind="loading" title="Assembling the brief" />
        ) : !report ? (
          <EmptyState
            kind="empty"
            title="No brief generated"
            detail="Pick a template and a window. The report is built from what the platform actually recorded in that period, so an empty window produces a short report rather than a padded one."
          />
        ) : (
          <>
            <div className="mb-3 flex flex-wrap items-baseline gap-x-4 gap-y-1 rounded-lg border border-line bg-inset px-3 py-2 text-micro text-ink-mute">
              <span className="font-mono text-ink-dim">{report.report_id}</span>
              <span>
                <ClockTime value={report.generated_at} seconds={false} withZone />
              </span>
              <span>{report.timeframe_hours}h window</span>
              {/* The counts the brief was built from. A report over a quiet
                  window and a report over a broken pipeline read the same
                  unless the inputs are stated. */}
              <span>
                {formatNumber(report.events_count, { decimals: 0 })} events ·{' '}
                {formatNumber(report.correlations_count, { decimals: 0 })} correlations ·{' '}
                {formatNumber(report.scenarios_count, { decimals: 0 })} scenarios
              </span>
              <span>{report.author}</span>
            </div>
            <Markdown source={report.markdown} />
          </>
        )}
      </div>
    </Card>
  );
}
