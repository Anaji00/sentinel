import { useEffect, useState, useRef } from 'react';
import { NormalizedEvent } from './types';

export function useLiveEvents(selectedDomain: string = 'all') {
  const [liveEvents, setLiveEvents] = useState<NormalizedEvent[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    // Determine WebSocket URL dynamically based on current window location
    const protocol = typeof window !== 'undefined' && window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    let baseHost = 'localhost:8000';
    if (process.env.NEXT_PUBLIC_API_URL) {
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

        ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            if (data && data.event_id) {
              setLiveEvents((prev) => {
                // Deduplicate by event_id
                if (prev.some((e) => e.event_id === data.event_id)) return prev;
                return [data, ...prev].slice(0, 50);
              });
            }
          } catch (e) {
            console.error('Error parsing WebSocket message:', e);
          }
        };

        ws.onerror = () => {
          // Browser will automatically close socket and trigger onclose
        };

        ws.onclose = () => {
          if (isMounted) {
            reconnectTimer = setTimeout(connect, 5000);
          }
        };
      } catch (err) {
        console.warn('WebSocket connection fallback to polling:', err);
      }
    }

    connect();

    return () => {
      isMounted = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.onerror = null;
        wsRef.current.close();
      }
    };
  }, []);

  return liveEvents.filter((e) => {
    if (!selectedDomain || selectedDomain === 'all') return true;
    const t = (e.type || '').toLowerCase();
    const s = (e.source || '').toLowerCase();
    
    if (selectedDomain === 'tradfi') {
      return t.includes('tradfi') || t.includes('option') || t.includes('dark_pool') || t.includes('equity') || t.includes('price') || t.includes('market') || t.includes('insider') || t.includes('futures') || s.includes('finnhub') || s.includes('alphavantage');
    }
    if (selectedDomain === 'crypto') {
      return t.includes('crypto') || s.includes('binance') || s.includes('coingecko');
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
    return false;
  });
}
