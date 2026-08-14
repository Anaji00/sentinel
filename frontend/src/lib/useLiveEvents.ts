import { useEffect, useState, useRef } from 'react';
import { NormalizedEvent } from './types';
import { useTelemetryStore } from './store';

// Max events kept in memory per hook instance
const MAX_LIVE_EVENTS = 300;
// Reconnect backoff limits (ms)
const RECONNECT_INITIAL_MS = 1000;
const RECONNECT_MAX_MS = 30000;

export function useLiveEvents(selectedDomain: string = 'all') {
  const [liveEvents, setLiveEvents] = useState<NormalizedEvent[]>([]);
  const wsRef = useRef<WebSocket | null>(null);
  const seenIds = useRef(new Set<string>());
  const backoffRef = useRef(RECONNECT_INITIAL_MS);
  const flushTimerRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    // Determine WebSocket URL dynamically based on current window location
    const protocol = typeof window !== 'undefined' && window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    let baseHost = typeof window !== 'undefined' ? `${window.location.hostname}:8000` : 'localhost:8000';
    if (process.env.NEXT_PUBLIC_WS_URL) {
      baseHost = process.env.NEXT_PUBLIC_WS_URL.replace(/^wss?:\/\//, '').replace(/\/+$/, '');
    } else if (process.env.NEXT_PUBLIC_API_URL) {
      baseHost = process.env.NEXT_PUBLIC_API_URL.replace(/^https?:\/\//, '').replace(/\/api\/v1\/?$/, '').replace(/\/+$/, '');
    }
    const wsUrl = `${protocol}//${baseHost}/api/v1/events/ws/live-feed`;

    let isMounted = true;
    let reconnectTimer: NodeJS.Timeout | null = null;

    function connect() {
      if (!isMounted) return;
      try {
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
          // Reset backoff on successful connection & sync Zustand store
          backoffRef.current = RECONNECT_INITIAL_MS;
          useTelemetryStore.getState().setConnected(true);
          useTelemetryStore.getState().updateTelemetry();
        };

        let pendingBatch: NormalizedEvent[] = [];

        const flushBatch = () => {
          if (pendingBatch.length > 0) {
            // Deduplicate high-frequency position reports (AIS vessels & ADS-B flights) by primary entity id
            const positionMap = new Map<string, NormalizedEvent>();
            const nonPositionEvents: NormalizedEvent[] = [];

            for (const item of pendingBatch) {
              const t = (item.type || '').toLowerCase();
              const isHighFreqPos = t.includes('vessel_position') || t.includes('adsb_position') || t.includes('ais') || t.includes('adsb');
              if (isHighFreqPos && item.primary_entity?.id) {
                if (!positionMap.has(item.primary_entity.id)) {
                  positionMap.set(item.primary_entity.id, item);
                }
              } else {
                nonPositionEvents.push(item);
              }
            }

            const batchToAdd = [...Array.from(positionMap.values()), ...nonPositionEvents];
            pendingBatch = [];

            setLiveEvents((prev) => {
              const combined = [...batchToAdd, ...prev];
              return combined.slice(0, MAX_LIVE_EVENTS);
            });
            useTelemetryStore.getState().updateTelemetry();
          }
          flushTimerRef.current = null;
        };

        ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            if (data && data.event_id) {
              // O(1) dedup check via Set
              if (seenIds.current.has(data.event_id)) return;
              seenIds.current.add(data.event_id);
              
              if (seenIds.current.size > MAX_LIVE_EVENTS * 2) {
                const ids = Array.from(seenIds.current);
                seenIds.current = new Set(ids.slice(ids.length - MAX_LIVE_EVENTS));
              }

              pendingBatch.unshift(data);

              // 1000ms sliding window buffer to reduce React re-render flooding from AIS/ADS-B telemetry
              if (!flushTimerRef.current) {
                flushTimerRef.current = setTimeout(flushBatch, 1000);
              }
            }
          } catch (e) {
            console.error('Error parsing WebSocket message:', e);
          }
        };

        ws.onerror = () => {
          useTelemetryStore.getState().setConnected(false);
        };

        ws.onclose = () => {
          if (isMounted) {
            useTelemetryStore.getState().setConnected(false);
            // Exponential backoff with cap
            reconnectTimer = setTimeout(connect, backoffRef.current);
            backoffRef.current = Math.min(backoffRef.current * 2, RECONNECT_MAX_MS);
          }
        };
      } catch (err) {
        console.warn('WebSocket connection fallback to polling:', err);
        useTelemetryStore.getState().setConnected(false);
        if (isMounted) {
          reconnectTimer = setTimeout(connect, backoffRef.current);
          backoffRef.current = Math.min(backoffRef.current * 2, RECONNECT_MAX_MS);
        }
      }
    }

    connect();

    return () => {
      isMounted = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (flushTimerRef.current) clearTimeout(flushTimerRef.current);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.onerror = null;
        wsRef.current.close();
        useTelemetryStore.getState().setConnected(false);
      }
    };
  }, []);

  return liveEvents.filter((e) => {
    if (!selectedDomain || selectedDomain === 'all') return true;
    const t = (e.type || '').toLowerCase();
    const s = (e.source || '').toLowerCase();
    
    if (selectedDomain === 'tradfi') {
      return t.includes('tradfi') || t.includes('option') || t.includes('dark_pool') || t.includes('equity') || t.includes('price') || t.includes('market') || t.includes('insider') || t.includes('futures') || t.includes('earnings') || s.includes('finnhub') || s.includes('alphavantage');
    }
    if (selectedDomain === 'crypto') {
      return t.includes('crypto') || t.includes('funding') || t.includes('interest') || s.includes('binance') || s.includes('coingecko') || s.includes('coinbase');
    }
    if (selectedDomain === 'prediction') {
      return t.includes('pred') || s.includes('polymarket') || s.includes('kalshi');
    }
    if (selectedDomain === 'cyber') {
      return t.includes('cyber') || t.includes('bgp') || t.includes('breach') || t.includes('ransomware') || t.includes('infra') || t.includes('vulnerability') || s.includes('cisa');
    }
    if (selectedDomain === 'maritime') {
      return t.includes('vessel') || t.includes('maritime') || t.includes('ais') || s.includes('ais') || s.includes('aisstream');
    }
    if (selectedDomain === 'aviation') {
      return t.includes('flight') || t.includes('aviation') || t.includes('adsb') || s.includes('opensky');
    }
    return false;
  });
}
