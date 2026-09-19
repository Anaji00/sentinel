'use client';

/**
 * A time, in the zone the operator selected.
 *
 * Eleven sites wrote `{new Date(e.occurred_at).toLocaleTimeString()}`, which
 * renders in the browser's zone, in the browser's locale, and says which of
 * neither. This is the replacement, and it is a component rather than a hook
 * call so that a row rendered three levels down inside a list does not have to
 * thread a zone through to get the right answer.
 *
 * The `title` carries the full UTC instant. A wall-clock time is what an
 * operator reads; the instant is what they need when they are reconciling this
 * screen against a log, and losing it to make room for the readable form would
 * be trading one of them for the other.
 */

import React from 'react';
import { formatClock, formatTimestamp } from '../../lib/format';
import { useTimeZone } from './TimeZoneContext';

interface ClockTimeProps {
  value: string | number | Date | null | undefined;
  /** Append the zone abbreviation. Worth it once per panel, not once per row. */
  withZone?: boolean;
  /** Drop the seconds, for places where the minute is the resolution. */
  seconds?: boolean;
  className?: string;
}

export function ClockTime({ value, withZone = false, seconds = true, className }: ClockTimeProps) {
  const { zone } = useTimeZone();
  return (
    <time
      dateTime={value ? new Date(value).toISOString() : undefined}
      title={formatTimestamp(value)}
      className={className}
    >
      {formatClock(value, zone, { seconds, withZone })}
    </time>
  );
}

export default ClockTime;
