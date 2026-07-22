import React from 'react';

interface MetricCardProps {
  label: string;
  value: string | number;
  change?: string;
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
    <div className={`p-3 rounded-lg bg-slate-900/60 border border-cyan-500/10 backdrop-blur-md flex flex-col justify-between ${className}`}>
      <span className="text-[10px] font-mono uppercase tracking-wider text-slate-400">
        {label}
      </span>
      <div className="flex items-baseline justify-between mt-1">
        <span className="text-lg font-bold text-slate-100 font-mono tracking-tight">
          {value}
        </span>
        {change && (
          <span
            className={`text-xs font-mono font-semibold ${
              isPositive ? 'text-emerald-400' : 'text-rose-400'
            }`}
          >
            {isPositive ? '↑' : '↓'} {change}
          </span>
        )}
      </div>
      {subtext && (
        <span className="text-[9px] text-slate-500 mt-1 truncate">
          {subtext}
        </span>
      )}
    </div>
  );
};
