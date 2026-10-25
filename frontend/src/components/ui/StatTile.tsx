import React from 'react';

/**
 * One headline figure with its label, as the newer pages show them: a quiet
 * sentence-case label under a large number. The older panels set these as
 * uppercase bold labels over coloured numbers, with decorative "engine" tiles
 * that measured nothing.
 */
export function StatTile({
  value,
  label,
  tone = 'text-ink',
  hint,
}: {
  value: React.ReactNode;
  label: string;
  tone?: string;
  hint?: string;
}) {
  return (
    <div className="rounded-xl border border-line bg-inset px-3.5 py-2.5" title={hint}>
      <div className={`font-mono text-head leading-none ${tone}`}>{value}</div>
      <div className="mt-1 text-micro text-ink-mute">{label}</div>
    </div>
  );
}

/** A short explanation in the place a tile would go: what the panel's terms mean. */
export function NoteTile({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-dashed border-line px-3.5 py-2.5">
      <div className="text-micro font-semibold text-ink-dim">{title}</div>
      <div className="mt-0.5 text-micro leading-snug text-ink-mute">{children}</div>
    </div>
  );
}

export default StatTile;
