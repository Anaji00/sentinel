import { useEffect, useMemo, useSyncExternalStore } from 'react';
import { NormalizedEvent } from './types';
import { useTelemetryStore } from './store';
import { resolveEventDomain } from './domain';
import {
  EMPTY_LIVE_EVENTS,
  LiveStreamDeps,
  liveFeedUrl,
  sharedLiveStream,
} from './liveStream';

// The pure parts moved to `liveStream.ts` with the socket they serve; re-exported
// so existing imports of them keep working.
export {
  MAX_LIVE_EVENTS,
  RECONNECT_INITIAL_MS,
  RECONNECT_MAX_MS,
  dedupePositionBatch,
  reconnectDelay,
} from './liveStream';

function browserDeps(): LiveStreamDeps {
  const telemetry = () => useTelemetryStore.getState();
  return {
    url: () =>
      liveFeedUrl(window.location, {
        wsUrl: process.env.NEXT_PUBLIC_WS_URL,
        apiUrl: process.env.NEXT_PUBLIC_API_URL,
      }),
    openSocket: (url) => new WebSocket(url),
    checkSession: async () => {
      try {
        const res = await fetch('/api/auth/session', { credentials: 'same-origin' });
        return res.ok;
      } catch {
        return null;
      }
    },
    setConnected: (connected) => telemetry().setConnected(connected),
    setAuthRequired: (required) => telemetry().setAuthRequired(required),
    touched: () => telemetry().updateTelemetry(),
  };
}

const stream = () => sharedLiveStream(browserDeps);
const subscribe = (listener: () => void) => stream().subscribe(listener);
const getSnapshot = () => stream().getSnapshot();
const getServerSnapshot = () => EMPTY_LIVE_EVENTS;

export function useLiveEvents(selectedDomain: string = 'all') {
  const liveEvents = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);

  // Counts mounted panels, not sockets: the header reads "idle" on pages
  // with no live panel at all.
  useEffect(() => {
    useTelemetryStore.getState().addStreamSubscriber(1);
    return () => useTelemetryStore.getState().addStreamSubscriber(-1);
  }, []);

  return useMemo(() => filterLiveEvents(liveEvents, selectedDomain), [liveEvents, selectedDomain]);
}

/** One domain's view of the shared buffer; `all` (or nothing) passes everything. */
export function filterLiveEvents(
  events: NormalizedEvent[],
  selectedDomain: string,
): NormalizedEvent[] {
  return events.filter((e) => {
    if (!selectedDomain || selectedDomain === 'all') return true;

    // The server's answer, when it has one.
    //
    // Everything below this was the whole filter, and it is the substring guess
    // the gateway's `domain` field was added to replace. `domainMetaFor()` in
    // IntelligenceFeed already reads that field -- so a Polymarket row, whose
    // type `prediction_market_trade` contains "market", was badged PREDICTION
    // by one function and listed under the TRADFI tab by this one, in the same
    // view. Measured: 24 such rows in twelve hours. The socket sends the payload
    // columns too, so `resolveEventDomain` can answer for almost every row.
    const resolved = resolveEventDomain(e);
    if (resolved) return resolved === selectedDomain;

    // Nothing declared and no payload. The type is all there is, which is what
    // the rest of this function has always been for; it is now the fallback
    // rather than the rule.
    const t = (e.type || '').toLowerCase();
    const s = (e.source || '').toLowerCase();

    if (selectedDomain === 'tradfi') {
      return (
        t.includes('tradfi') ||
        t.includes('option') ||
        t.includes('dark_pool') ||
        t.includes('equity') ||
        t.includes('price') ||
        t.includes('market') ||
        t.includes('insider') ||
        t.includes('futures') ||
        t.includes('earnings') ||
        s.includes('finnhub') ||
        s.includes('alphavantage')
      );
    }
    if (selectedDomain === 'crypto') {
      return (
        t.includes('crypto') ||
        t.includes('funding') ||
        t.includes('interest') ||
        s.includes('binance') ||
        s.includes('coingecko') ||
        s.includes('coinbase')
      );
    }
    if (selectedDomain === 'prediction') {
      return t.includes('pred') || s.includes('polymarket') || s.includes('kalshi');
    }
    if (selectedDomain === 'maritime') {
      return (
        t.includes('vessel') ||
        t.includes('maritime') ||
        t.includes('ais') ||
        s.includes('ais') ||
        s.includes('aisstream')
      );
    }
    if (selectedDomain === 'aviation') {
      return (
        t.includes('flight') ||
        t.includes('aviation') ||
        t.includes('adsb') ||
        s.includes('opensky')
      );
    }
    return false;
  });
}
