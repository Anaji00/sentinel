import React from 'react';

/**
 * The container every panel renders through.
 *
 * Titles were set in uppercase monospace with `tracking-widest` in bright cyan
 * (#66fcf1) -- the least legible way to set a phrase, in the colour reserved
 * for things you can click. Sixteen of them competed with each other and with
 * the data underneath. A panel title is a label, not a signal: it is quiet, and
 * cyan is left to mean interactive.
 */

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
    <div className={`panel flex flex-col h-full w-full overflow-hidden ${className}`}>
      {(title || badge || headerAction) && (
        <div className="panel-header shrink-0">
          <div className="flex items-baseline gap-2.5 min-w-0">
            {title && <h3 className="panel-title truncate">{title}</h3>}
            {subtitle && <span className="panel-subtitle truncate">{subtitle}</span>}
            {badge}
          </div>
          {headerAction && <div className="shrink-0">{headerAction}</div>}
        </div>
      )}
      <div className={`flex-1 flex flex-col min-h-0 overflow-y-auto ${noPadding ? '' : 'p-4'}`}>
        {children}
      </div>
    </div>
  );
};
