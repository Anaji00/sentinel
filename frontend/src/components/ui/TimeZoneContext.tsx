'use client';

/**
 * Which zone the operator is reading times in.
 *
 * The header had a timezone selector, and it was `useState` local to the
 * header. Nothing else in the application read it. Meanwhile eleven event
 * timestamps across nine components called `toLocaleTimeString()` with no
 * zone, which means the browser's.
 *
 * So the clock could read 14:32 EDT because the operator chose New York, while
 * the event row immediately beneath it read 20:32 because the machine is in
 * Berlin -- two times on one screen, in different zones, neither labelled. On
 * a platform whose entire job is correlating events in time, that is not a
 * formatting preference. It is a wrong answer.
 *
 * The choice lives here, is persisted, and is the single input to every time
 * the application renders.
 */

import React from 'react';

/** The offered zones. Moved out of the header, which is now one consumer. */
export const TIMEZONES: Array<[string, string]> = [
  ['America/New_York', 'US EST'],
  ['UTC', 'UTC'],
  ['America/Chicago', 'US CST'],
  ['America/Denver', 'US MST'],
  ['America/Los_Angeles', 'US PST'],
  ['Europe/London', 'GMT'],
  ['Europe/Paris', 'CET'],
  ['Asia/Tokyo', 'JST'],
];

/**
 * New York, because the instruments this platform tracks are priced there.
 *
 * Not the browser's zone: a default that varies per machine means two
 * operators comparing the same screen are reading different clocks, and
 * neither can tell.
 */
export const DEFAULT_ZONE = 'America/New_York';

const STORAGE_KEY = 'sentinel.timezone';

interface TimeZoneState {
  zone: string;
  setZone: (zone: string) => void;
}

const TimeZoneContext = React.createContext<TimeZoneState | null>(null);

export function TimeZoneProvider({ children }: { children: React.ReactNode }) {
  // Starts at the default on both server and client, so the first render
  // matches and hydration does not warn; the stored choice is applied in an
  // effect. Reading localStorage during render would make the markup depend on
  // a value the server cannot see.
  const [zone, setZoneState] = React.useState(DEFAULT_ZONE);

  React.useEffect(() => {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY);
      if (stored && TIMEZONES.some(([tz]) => tz === stored)) setZoneState(stored);
    } catch {
      // Private windows and blocked site data throw here. A remembered
      // preference is not worth failing a page render over.
    }
  }, []);

  const setZone = React.useCallback((next: string) => {
    setZoneState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      /* see above */
    }
  }, []);

  const value = React.useMemo(() => ({ zone, setZone }), [zone, setZone]);
  return <TimeZoneContext.Provider value={value}>{children}</TimeZoneContext.Provider>;
}

/**
 * The selected zone.
 *
 * Falls back to the default rather than throwing, because these components are
 * also rendered in tests and in isolation, and a clock is not worth crashing a
 * page over. The fallback is the same constant the provider starts from, so an
 * unprovided component and a provided one agree until the operator changes it.
 */
export function useTimeZone(): TimeZoneState {
  return React.useContext(TimeZoneContext) ?? { zone: DEFAULT_ZONE, setZone: () => {} };
}
