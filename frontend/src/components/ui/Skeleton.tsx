import React from 'react';

interface SkeletonProps {
  className?: string;
  height?: string;
  width?: string;
}

export const Skeleton: React.FC<SkeletonProps> = ({
  className = '',
  height,
  width,
}) => {
  return (
    <div
      style={{ height, width }}
      className={`animate-shimmer rounded bg-slate-800/40 border border-slate-700/20 ${className}`}
    />
  );
};

export const PanelSkeleton: React.FC<{ title?: string }> = ({ title = 'Loading Feed...' }) => {
  return (
    <div className="w-full h-full p-4 glass-panel rounded-xl flex flex-col gap-4 animate-pulse">
      <div className="flex items-center justify-between border-b border-cyan-500/10 pb-3">
        <Skeleton height="16px" width="140px" />
        <Skeleton height="20px" width="60px" />
      </div>
      <div className="flex-1 flex flex-col gap-3">
        <Skeleton height="60px" width="100%" />
        <Skeleton height="60px" width="100%" />
        <Skeleton height="60px" width="100%" />
        <Skeleton height="60px" width="100%" />
      </div>
    </div>
  );
};
