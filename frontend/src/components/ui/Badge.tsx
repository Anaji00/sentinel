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
  const variantStyles: Record<BadgeVariant, { bg: string; text: string; border: string; dot: string }> = {
    live: {
      bg: 'bg-cyan-950/60',
      text: 'text-[#66fcf1]',
      border: 'border-[#66fcf1]/40',
      dot: 'bg-[#66fcf1]',
    },
    anomaly: {
      bg: 'bg-rose-950/60',
      text: 'text-rose-400',
      border: 'border-rose-500/40',
      dot: 'bg-rose-500',
    },
    success: {
      bg: 'bg-emerald-950/60',
      text: 'text-emerald-400',
      border: 'border-emerald-500/40',
      dot: 'bg-emerald-400',
    },
    warning: {
      bg: 'bg-amber-950/60',
      text: 'text-amber-400',
      border: 'border-amber-500/40',
      dot: 'bg-amber-400',
    },
    info: {
      bg: 'bg-sky-950/60',
      text: 'text-sky-400',
      border: 'border-sky-500/40',
      dot: 'bg-sky-400',
    },
    neutral: {
      bg: 'bg-slate-800/60',
      text: 'text-slate-300',
      border: 'border-slate-700/50',
      dot: 'bg-slate-400',
    },
  };

  const style = variantStyles[variant];

  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-mono uppercase tracking-widest border backdrop-blur-md transition-all duration-200 ${style.bg} ${style.text} ${style.border} ${className}`}
    >
      {pulse && (
        <span className="relative flex h-2 w-2">
          <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${style.dot}`} />
          <span className={`relative inline-flex rounded-full h-2 w-2 ${style.dot}`} />
        </span>
      )}
      {children}
    </span>
  );
};
