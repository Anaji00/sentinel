'use client';

import React, { Component, ErrorInfo, ReactNode } from 'react';
import { IconAlert } from '@/components/ui/icons';

interface Props {
  children: ReactNode;
  fallbackTitle?: string;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error('[Sentinel UI ErrorBoundary Caught Exception]:', error, errorInfo);
  }

  private handleReset = () => {
    this.setState({ hasError: false, error: null });
  };

  public render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center h-full w-full p-6 bg-[#090b10] border border-amber-500/30 rounded-xl text-center">
          <div className="h-10 w-10 rounded-full bg-amber-500/20 text-amber-400 flex items-center justify-center font-bold text-lg mb-3 border border-amber-500/40 animate-pulse">
            <IconAlert className="inline-block shrink-0" />
          </div>
          <h3 className="text-sm font-bold text-ink mb-1">
            {this.props.fallbackTitle || 'Visualizer Render Failure'}
          </h3>
          <p className="text-xs text-ink-dim max-w-sm mb-4 leading-relaxed">
            {this.state.error?.message ||
              'WebGL or DOM Context lost during hardware accelerated rendering.'}
          </p>
          <button
            onClick={this.handleReset}
            className="px-3.5 py-1.5 bg-amber-500/20 hover:bg-amber-500/30 text-amber-300 border border-amber-500/50 rounded text-xs font-bold uppercase tracking-wider transition-all cursor-pointer shadow-panel"
          >
            Reset Visualizer Context
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}
