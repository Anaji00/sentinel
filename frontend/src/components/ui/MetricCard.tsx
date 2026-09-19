import React from 'react';
import { IconDown, IconFlow, IconUp } from './icons';

interface MetricCardProps {
  label: string;
  value: string | number;
  change?: string;
  /** Which way. Omit when the direction is genuinely unknown. */
  isPositive?: boolean;
  subtext?: string;
  className?: string;
}

export const MetricCard: React.FC<MetricCardProps> = ({
  label,
  value,
  change,
  isPositive,
  subtext,
  className = '',
}) => {
  return (
    <div
      className={`p-3 rounded-lg bg-raised/60 border border-cyan-500/10 flex flex-col justify-between ${className}`}
    >
      <span className="text-micro font-mono uppercase tracking-wider text-ink-dim">{label}</span>
      <div className="flex items-baseline justify-between mt-1">
        {/* `text-lead` is the scale step named for exactly this: the one number
            a panel exists to show. It was declared and referenced nowhere, which
            is the same unused-token problem the scale was introduced to end. */}
        <span className="text-lead font-semibold text-ink font-mono tracking-tight">{value}</span>
        {change && (
          <span
            className={`flex items-center gap-1 text-xs font-semibold tabular-nums ${
              isPositive === undefined
                ? 'text-ink-dim'
                : isPositive
                  ? 'text-emerald-400'
                  : 'text-rose-400'
            }`}
          >
            {isPositive === undefined ? (
              <IconFlow label="direction not known" />
            ) : isPositive ? (
              <IconUp label="up" />
            ) : (
              <IconDown label="down" />
            )}
            {change}
          </span>
        )}
      </div>
      {subtext && <span className="text-micro text-ink-mute mt-1 truncate">{subtext}</span>}
    </div>
  );
};
