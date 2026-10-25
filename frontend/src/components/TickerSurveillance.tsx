'use client';

/** The linked stock's surveillance business, when it has one (?symbol=AXON). */

import React from 'react';
import { useSearchParams } from 'next/navigation';
import { SurveillanceBusiness } from './SurveillanceBusiness';

export default function TickerSurveillance() {
  const symbol = (useSearchParams()?.get('symbol') || '').trim().toUpperCase().slice(0, 10);
  return symbol ? <SurveillanceBusiness ticker={symbol} /> : null;
}
