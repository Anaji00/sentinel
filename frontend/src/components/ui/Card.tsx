import React from 'react';

interface CardProps {
  title?: string;
  subtitle?: string;
  badge?: React.ReactNode;
  headerAction?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

export const Card: React.FC<CardProps> = ({
  title,
  subtitle,
  badge,
  headerAction,
  children,
  className = '',
  noPadding = false,
}) => {
  return (
    <div
      className={`glass-panel glass-panel-hover rounded-xl flex flex-col h-full w-full overflow-hidden ${className}`}
    >
      {(title || badge || headerAction) && (
        <div className="flex items-center justify-between px-4 py-3 border-b border-cyan-500/10 bg-slate-950/40">
          <div className="flex items-center gap-2.5">
            {title && (
              <h3 className="text-xs font-mono font-bold tracking-widest text-[#66fcf1] uppercase">
                {title}
              </h3>
            )}
            {subtitle && (
              <span className="text-[10px] text-slate-400 font-sans">
                {subtitle}
              </span>
            )}
            {badge}
          </div>
          {headerAction && <div>{headerAction}</div>}
        </div>
      )}
      <div className={`flex-1 flex flex-col min-h-0 overflow-y-auto ${noPadding ? '' : 'p-4'}`}>
        {children}
      </div>
    </div>
  );
};
