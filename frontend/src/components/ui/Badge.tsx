import React from 'react';

export type BadgeVariant = 'live' | 'anomaly' | 'success' | 'warning' | 'info' | 'neutral';

interface BadgeProps {
  variant?: BadgeVariant;
  children: React.ReactNode;
  pulse?: boolean;
  className?: string;
}

export const Badge: React.FC<BadgeProps> = ({
  variant = 'neutral',
  children,
  pulse = false,
  className = '',
}) => {
  const variantStyles: Record<
    BadgeVariant,
    { bg: string; text: string; border: string; dot: string }
  > = {
    live: {
      bg: 'bg-accent-dim',
      text: 'text-accent',
      border: 'border-accent/40',
      dot: 'bg-accent',
    },
    anomaly: {
      bg: 'bg-negative/10',
      text: 'text-negative',
      border: 'border-negative/30',
      dot: 'bg-rose-500',
    },
    success: {
      bg: 'bg-positive/10',
      text: 'text-positive',
      border: 'border-positive/30',
      dot: 'bg-emerald-400',
    },
    warning: {
      bg: 'bg-caution/10',
      text: 'text-caution',
      border: 'border-caution/30',
      dot: 'bg-amber-400',
    },
    info: {
      bg: 'bg-sky-950/60',
      text: 'text-sky-400',
      border: 'border-sky-500/40',
      dot: 'bg-sky-400',
    },
    neutral: {
      bg: 'bg-overlay/60',
      text: 'text-ink-dim',
      border: 'border-line-strong/50',
      dot: 'bg-slate-400',
    },
  };

  const style = variantStyles[variant];

  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-micro font-medium border transition-all duration-200 ${style.bg} ${style.text} ${style.border} ${className}`}
    >
      {pulse && (
        <span className="relative flex h-2 w-2">
          <span
            className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${style.dot}`}
          />
          <span className={`relative inline-flex rounded-full h-2 w-2 ${style.dot}`} />
        </span>
      )}
      {children}
    </span>
  );
};
