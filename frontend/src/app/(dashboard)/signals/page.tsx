import React from 'react';
import TradingSignals from '@/components/TradingSignals';

export default function SignalsPage() {
  return (
    <div className="h-full w-full overflow-hidden p-2">
      <h1 className="sr-only">Trading signals</h1>
      <TradingSignals />
    </div>
  );
}
