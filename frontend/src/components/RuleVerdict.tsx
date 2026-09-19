'use client';

/**
 * Telling the platform a correlation was wrong.
 *
 * `POST /feedback` has existed the whole time, `/feedback/rules` aggregates it
 * worst-first, the operations console renders that aggregate, and the agent
 * that decides which rules survive is handed the names it produces. Every part
 * of the loop was built except the one where a person says anything: no
 * component in the application has ever posted a verdict.
 *
 * So the rule scorecard has always been empty, "needs_review" has always been
 * false, and the retirement agent has always been asked to judge rules on no
 * evidence -- which it correctly declines to do. The loop was not broken in
 * any single place; it simply had no first step.
 *
 * The four verdicts are the server's own vocabulary, not a rating. "Wrong" and
 * "not useful" are different claims -- one says the rule fired on something
 * that did not happen, the other that it happened and did not matter -- and
 * collapsing them into stars would throw away the distinction the scorecard is
 * built on.
 */

import React from 'react';
import { apiClient, describeApiError } from '../lib/api';
import { useFeedback } from './ui/Feedback';
import { IconBlocked, IconCheck, IconClose, IconDocument } from './ui/icons';

/** `VERDICTS` in services/api_gateway/routes/feedback.py. */
const VERDICTS = [
  {
    id: 'useful',
    label: 'Useful',
    Icon: IconCheck,
    tone: 'hover:border-positive/50 hover:text-positive',
    hint: 'This told me something I would have missed.',
  },
  {
    id: 'not_useful',
    label: 'Not useful',
    Icon: IconClose,
    tone: 'hover:border-line-strong hover:text-ink',
    hint: 'Real, and not worth surfacing.',
  },
  {
    id: 'wrong',
    label: 'Wrong',
    Icon: IconBlocked,
    tone: 'hover:border-negative/50 hover:text-negative',
    hint: 'The rule fired on something that did not happen.',
  },
  {
    id: 'duplicate',
    label: 'Duplicate',
    Icon: IconDocument,
    tone: 'hover:border-caution/50 hover:text-caution',
    hint: 'Already surfaced by something else.',
  },
] as const;

type VerdictId = (typeof VERDICTS)[number]['id'];

interface RuleVerdictProps {
  correlationId: string;
  ruleName: string;
}

export function RuleVerdict({ correlationId, ruleName }: RuleVerdictProps) {
  const { toast } = useFeedback();
  const [sent, setSent] = React.useState<VerdictId | null>(null);
  const [busy, setBusy] = React.useState(false);

  const submit = async (verdict: VerdictId) => {
    setBusy(true);
    try {
      // `rule_id` is the rule's name as the cluster carries it: the gateway
      // requires one of rule_id or correlation_id and keys the scorecard on
      // the first, so sending only the correlation would file every verdict
      // under "unattributed" and the per-rule aggregate would stay empty.
      await apiClient.post('/feedback', {
        correlation_id: correlationId,
        rule_id: ruleName,
        verdict,
      });
      setSent(verdict);
      toast('success', `Recorded: ${verdict.replace('_', ' ')}`);
    } catch (err) {
      // Said out loud. A silent POST is the UI version of a swallowed
      // exception, and this one is the only way the platform ever hears that
      // a rule is wrong.
      toast('error', 'That verdict was not recorded.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  if (sent) {
    const chosen = VERDICTS.find((v) => v.id === sent)!;
    return (
      <p className="flex items-center gap-1.5 text-micro text-ink-mute">
        <chosen.Icon />
        Recorded as <span className="text-ink-dim">{chosen.label.toLowerCase()}</span>. It counts
        towards this rule&rsquo;s standing.
      </p>
    );
  }

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <span className="stat-label mr-0.5">Was this useful?</span>
      {VERDICTS.map((v) => (
        <button
          key={v.id}
          disabled={busy}
          onClick={() => submit(v.id)}
          title={v.hint}
          className={`flex items-center gap-1 rounded-md border border-line px-2 py-0.5 text-micro font-medium text-ink-mute transition-colors enabled:cursor-pointer disabled:opacity-40 ${v.tone}`}
        >
          <v.Icon />
          {v.label}
        </button>
      ))}
    </div>
  );
}

export default RuleVerdict;
