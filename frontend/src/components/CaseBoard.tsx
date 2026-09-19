'use client';

/**
 * Investigation cases: the feature that was built and had no screen.
 *
 * Five routes -- list, read, create, amend, annotate -- with an audit-ledger
 * entry written on creation, a status lifecycle, a priority, a lead analyst,
 * seven kinds of linked evidence and a note thread. None of it was reachable
 * from the product. An analyst who found something had nowhere to put it, so
 * the platform's memory of an investigation was whatever that person kept
 * elsewhere.
 *
 * The status and priority vocabularies are the server's enums, read from
 * `shared/models/cases.py` and pinned by a test. A sixth status invented here
 * would be rejected by the PATCH validator, and a missing one would make a
 * case unreachable through the only UI that can move it.
 *
 * Creating a case is a write that a person makes deliberately and that the
 * audit ledger records, so it asks before it writes and says what happened
 * afterwards -- the same rule the operations console follows for replays.
 */

import React from 'react';
import useSWR, { mutate as globalMutate } from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ExportButton } from './ui/ExportButton';
import { ClockTime } from './ui/ClockTime';
import { useFeedback } from './ui/Feedback';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';
import { IconChevronDown, IconClose, IconDocument, IconUrgent } from './ui/icons';
import { ABSENT, formatNumber } from '../lib/format';
import type { CsvColumn } from '../lib/csv';

/** `CaseStatus` in shared/models/cases.py. */
const STATUSES = ['OPEN', 'IN_INVESTIGATION', 'ESCALATED', 'RESOLVED', 'DISMISSED'] as const;
/** `CasePriority` in shared/models/cases.py. */
const PRIORITIES = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'] as const;

type CaseStatus = (typeof STATUSES)[number];
type CasePriority = (typeof PRIORITIES)[number];

interface CaseNote {
  note_id: string;
  author: string;
  content: string;
  created_at: string;
}

interface InvestigationCase {
  case_id: string;
  title: string;
  description: string;
  status: CaseStatus;
  priority: CasePriority;
  lead_analyst: string;
  created_at: string;
  updated_at: string;
  linked_event_ids: string[];
  linked_correlation_ids: string[];
  linked_entities: string[];
  notes: CaseNote[];
  tags: string[];
}

interface CaseList {
  count: number;
  cases: InvestigationCase[];
}

/** Priority is severity, so it takes the severity palette rather than a hue. */
const PRIORITY_TONE: Record<CasePriority, string> = {
  CRITICAL: 'tone-negative',
  HIGH: 'tone-caution',
  MEDIUM: 'tone-info',
  LOW: 'tone-muted',
};

/**
 * Open and closed are the distinction that matters on a board.
 *
 * Resolved and dismissed are both endings and different ones -- resolved means
 * the question was answered, dismissed means it should not have been asked --
 * so they are shown apart rather than merged into "closed".
 */
const STATUS_TONE: Record<CaseStatus, string> = {
  OPEN: 'tone-info',
  IN_INVESTIGATION: 'tone-caution',
  ESCALATED: 'tone-negative',
  RESOLVED: 'tone-positive',
  DISMISSED: 'tone-muted',
};

const CSV_COLUMNS: CsvColumn<InvestigationCase>[] = [
  { label: 'case_id', value: (c) => c.case_id },
  { label: 'title', value: (c) => c.title },
  { label: 'status', value: (c) => c.status },
  { label: 'priority', value: (c) => c.priority },
  { label: 'lead_analyst', value: (c) => c.lead_analyst },
  { label: 'created_at', value: (c) => c.created_at },
  { label: 'updated_at', value: (c) => c.updated_at },
  { label: 'linked_events', value: (c) => c.linked_event_ids.length },
  { label: 'linked_correlations', value: (c) => c.linked_correlation_ids.length },
  {
    label: 'linked_entities',
    value: (c) => (c.linked_entities.length ? c.linked_entities.join('; ') : null),
  },
  { label: 'notes', value: (c) => c.notes.length },
  { label: 'tags', value: (c) => (c.tags.length ? c.tags.join('; ') : null) },
  { label: 'description', value: (c) => c.description },
];

const LIST_KEY = '/cases?limit=100';

function humanize(token: string): string {
  const words = token.replace(/_/g, ' ').toLowerCase();
  return words.charAt(0).toUpperCase() + words.slice(1);
}

// ── creating one ────────────────────────────────────────────────────────────

function NewCaseDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const dialog = useDialog(open, onClose, 'New case');
  const { toast } = useFeedback();
  const [title, setTitle] = React.useState('');
  const [description, setDescription] = React.useState('');
  const [priority, setPriority] = React.useState<CasePriority>('MEDIUM');
  const [busy, setBusy] = React.useState(false);

  // The server requires three characters; refusing here means the operator
  // finds out before the round trip rather than through a 422.
  const valid = title.trim().length >= 3 && description.trim().length > 0;

  const submit = async () => {
    setBusy(true);
    try {
      await apiClient.post('/cases', {
        title: title.trim(),
        description: description.trim(),
        priority,
      });
      toast('success', 'Case opened', 'It is recorded in the audit ledger.');
      setTitle('');
      setDescription('');
      setPriority('MEDIUM');
      await globalMutate(LIST_KEY);
      onClose();
    } catch (err) {
      toast('error', 'The case was not created.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4"
      {...dialog.overlayProps}
    >
      <div
        className="w-full max-w-lg space-y-4 rounded-2xl border border-line-strong bg-raised p-6 text-ink shadow-2xl"
        {...dialog.panelProps}
      >
        <div className="flex items-center justify-between border-b border-line pb-3">
          <h2 className="text-head font-semibold">Open a case</h2>
          <button
            onClick={onClose}
            aria-label="Close"
            className="cursor-pointer rounded p-1 text-ink-dim hover:text-ink"
          >
            <IconClose />
          </button>
        </div>

        <label className="block space-y-1">
          <span className="stat-label">Title</span>
          <input
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            maxLength={200}
            placeholder="What is being investigated"
            className="w-full rounded-lg border border-line bg-page px-2.5 py-1.5 text-xs text-ink outline-none focus:border-line-accent"
          />
        </label>

        <label className="block space-y-1">
          <span className="stat-label">What you saw</span>
          <textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            rows={4}
            placeholder="The observation, and why it is worth a case."
            className="w-full resize-y rounded-lg border border-line bg-page px-2.5 py-1.5 text-xs text-ink outline-none focus:border-line-accent"
          />
        </label>

        <div className="space-y-1">
          <span className="stat-label">Priority</span>
          <div className="flex flex-wrap gap-1.5">
            {PRIORITIES.map((p) => (
              <button
                key={p}
                onClick={() => setPriority(p)}
                aria-pressed={priority === p}
                className={`cursor-pointer rounded-md border px-2.5 py-1 text-micro font-medium transition-colors ${
                  priority === p
                    ? `border-line-accent bg-accent-dim ${PRIORITY_TONE[p]}`
                    : 'border-line text-ink-mute hover:text-ink-dim'
                }`}
              >
                {humanize(p)}
              </button>
            ))}
          </div>
        </div>

        <div className="flex items-center justify-between border-t border-line pt-3">
          <span className="text-micro text-ink-mute">
            Opening a case writes an entry to the audit trail.
          </span>
          <button
            onClick={submit}
            disabled={!valid || busy}
            className="rounded-lg border border-line-accent bg-accent-dim px-3 py-1.5 text-micro font-semibold text-accent transition-colors enabled:cursor-pointer enabled:hover:border-accent disabled:opacity-40"
          >
            {busy ? 'Opening…' : 'Open case'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── one case, expanded ──────────────────────────────────────────────────────

function CaseDetail({ c }: { c: InvestigationCase }) {
  const { toast } = useFeedback();
  const [note, setNote] = React.useState('');
  const [busy, setBusy] = React.useState<string | null>(null);

  const patch = async (body: Record<string, unknown>, what: string) => {
    setBusy(what);
    try {
      await apiClient.patch(`/cases/${encodeURIComponent(c.case_id)}`, body);
      toast('success', `${c.case_id} updated`);
      await globalMutate(LIST_KEY);
    } catch (err) {
      toast('error', `${c.case_id} was not updated.`, describeApiError(err) ?? undefined);
    }
    setBusy(null);
  };

  const addNote = async () => {
    const content = note.trim();
    if (!content) return;
    setBusy('note');
    try {
      await apiClient.post(`/cases/${encodeURIComponent(c.case_id)}/notes`, { content });
      setNote('');
      toast('success', 'Note added');
      await globalMutate(LIST_KEY);
    } catch (err) {
      toast('error', 'The note was not saved.', describeApiError(err) ?? undefined);
    }
    setBusy(null);
  };

  const linked =
    c.linked_event_ids.length + c.linked_correlation_ids.length + c.linked_entities.length;

  return (
    <div className="space-y-3 border-l-2 border-line pl-3">
      <p className="text-xs leading-relaxed text-ink-dim">{c.description}</p>

      <div className="flex flex-wrap items-center gap-1.5">
        <span className="stat-label mr-0.5">Status</span>
        {STATUSES.map((st) => (
          <button
            key={st}
            disabled={busy !== null || st === c.status}
            onClick={() => patch({ status: st }, 'status')}
            className={`rounded-md border px-2 py-0.5 text-micro font-medium transition-colors enabled:cursor-pointer disabled:opacity-100 ${
              st === c.status
                ? `border-line-accent bg-accent-dim ${STATUS_TONE[st]}`
                : 'border-line text-ink-mute enabled:hover:text-ink-dim'
            }`}
          >
            {humanize(st)}
          </button>
        ))}
      </div>

      <div className="flex flex-wrap gap-x-4 gap-y-1 text-micro text-ink-mute">
        <span>
          Lead <span className="text-ink-dim">{c.lead_analyst || ABSENT}</span>
        </span>
        <span>
          Opened <ClockTime value={c.created_at} seconds={false} />
        </span>
        <span>
          Last touched <ClockTime value={c.updated_at} seconds={false} />
        </span>
        <span>
          {/* Linked evidence is the point of a case. Zero is worth saying out
              loud: a case with nothing attached is a note, not an
              investigation. */}
          {linked === 0
            ? 'no evidence linked yet'
            : `${formatNumber(linked, { decimals: 0 })} linked`}
        </span>
      </div>

      {c.notes.length > 0 && (
        <ul className="space-y-1.5">
          {c.notes.map((n) => (
            <li key={n.note_id} className="rounded border border-line bg-page px-2.5 py-1.5">
              <div className="flex items-baseline justify-between gap-2 text-micro text-ink-mute">
                <span>{n.author}</span>
                <ClockTime value={n.created_at} seconds={false} />
              </div>
              <p className="mt-0.5 text-micro leading-relaxed text-ink-dim">{n.content}</p>
            </li>
          ))}
        </ul>
      )}

      <div className="flex items-start gap-1.5">
        <textarea
          value={note}
          onChange={(e) => setNote(e.target.value)}
          rows={2}
          placeholder="Add what you found."
          className="flex-1 resize-y rounded-lg border border-line bg-page px-2.5 py-1.5 text-micro text-ink outline-none focus:border-line-accent"
        />
        <button
          onClick={addNote}
          disabled={!note.trim() || busy !== null}
          className="rounded-md border border-line px-2.5 py-1 text-micro font-medium text-ink-dim transition-colors enabled:cursor-pointer enabled:hover:border-line-strong enabled:hover:text-ink disabled:opacity-40"
        >
          {busy === 'note' ? 'Saving…' : 'Add note'}
        </button>
      </div>
    </div>
  );
}

// ── the board ───────────────────────────────────────────────────────────────

export default function CaseBoard() {
  const [statusFilter, setStatusFilter] = React.useState<CaseStatus | 'ALL'>('ALL');
  const [openId, setOpenId] = React.useState<string | null>(null);
  const [creating, setCreating] = React.useState(false);

  const { data, error, isLoading } = useSWR<CaseList>(LIST_KEY, fetcher, {
    refreshInterval: POLL.standard,
  });

  const cases = React.useMemo(() => {
    const rows = data?.cases ?? [];
    return statusFilter === 'ALL' ? rows : rows.filter((c) => c.status === statusFilter);
  }, [data, statusFilter]);

  const counts = React.useMemo(() => {
    const out: Partial<Record<CaseStatus, number>> = {};
    for (const c of data?.cases ?? []) out[c.status] = (out[c.status] ?? 0) + 1;
    return out;
  }, [data]);

  if (error) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Cases unavailable"
          detail={describeApiError(error) ?? undefined}
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
            Investigation cases
          </h2>
          <p className="panel-subtitle">
            {data
              ? `${formatNumber(data.count, { decimals: 0 })} open on this deployment`
              : 'What the platform remembers about an investigation'}
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          <ExportButton subject="cases" rows={cases} columns={CSV_COLUMNS} />
          <button
            onClick={() => setCreating(true)}
            className="cursor-pointer rounded-md border border-line-accent bg-accent-dim px-2.5 py-0.5 text-micro font-semibold text-accent transition-colors hover:border-accent"
          >
            New case
          </button>
        </div>
      </div>

      <div className="flex shrink-0 flex-wrap items-center gap-1.5 border-b border-line px-3.5 py-2">
        <span className="stat-label mr-0.5">Show</span>
        {(['ALL', ...STATUSES] as const).map((st) => (
          <button
            key={st}
            onClick={() => setStatusFilter(st)}
            aria-pressed={statusFilter === st}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
              statusFilter === st
                ? 'border-line-accent bg-accent-dim text-accent'
                : 'border-line text-ink-mute hover:text-ink-dim'
            }`}
          >
            {st === 'ALL' ? 'All' : humanize(st)}
            {st !== 'ALL' && counts[st] ? (
              <span className="ml-1 text-ink-mute">{counts[st]}</span>
            ) : null}
          </button>
        ))}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {isLoading && !data ? (
          <EmptyState kind="loading" title="Reading the case file" />
        ) : cases.length === 0 ? (
          <EmptyState
            kind="empty"
            title={
              statusFilter === 'ALL'
                ? 'No cases yet'
                : `Nothing ${humanize(statusFilter).toLowerCase()}`
            }
            detail={
              statusFilter === 'ALL'
                ? 'Nothing has been opened. A case is how the platform remembers an investigation across sessions and analysts.'
                : 'Cases in other states are still listed under All.'
            }
          />
        ) : (
          <ul className="divide-y divide-line/60">
            {cases.map((c) => (
              <li key={c.case_id} className="px-3.5 py-2.5">
                <button
                  onClick={() => setOpenId(openId === c.case_id ? null : c.case_id)}
                  aria-expanded={openId === c.case_id}
                  className="flex w-full cursor-pointer items-start gap-2 text-left"
                >
                  <IconChevronDown
                    className={`mt-0.5 shrink-0 text-ink-mute transition-transform ${
                      openId === c.case_id ? '' : '-rotate-90'
                    }`}
                  />
                  <span className="min-w-0 flex-1">
                    <span className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                      <span className="font-mono text-micro text-ink-mute">{c.case_id}</span>
                      <span className="text-xs font-medium text-ink">{c.title}</span>
                    </span>
                    <span className="mt-1 flex flex-wrap items-center gap-1.5">
                      <span className={`badge ${STATUS_TONE[c.status]}`}>{humanize(c.status)}</span>
                      <span className={`badge ${PRIORITY_TONE[c.priority]}`}>
                        {c.priority === 'CRITICAL' && <IconUrgent />}
                        {humanize(c.priority)}
                      </span>
                      {c.notes.length > 0 && (
                        <span className="text-micro text-ink-mute">
                          {c.notes.length} {c.notes.length === 1 ? 'note' : 'notes'}
                        </span>
                      )}
                      {c.tags.map((t) => (
                        <span key={t} className="text-micro text-ink-mute">
                          #{t}
                        </span>
                      ))}
                    </span>
                  </span>
                </button>

                {openId === c.case_id && (
                  <div className="mt-2.5">
                    <CaseDetail c={c} />
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>

      <NewCaseDialog open={creating} onClose={() => setCreating(false)} />
    </Card>
  );
}
