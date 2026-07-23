import { useEffect, useState, useRef } from 'react';
import { NormalizedEvent } from './types';

export function useLiveEvents(selectedDomain: string = 'all') {
  const [liveEvents, setLiveEvents] = useState<NormalizedEvent[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    // Determine WebSocket URL dynamically based on current window location
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = process.env.NEXT_PUBLIC_API_URL 
      ? process.env.NEXT_PUBLIC_API_URL.replace(/^http/, 'ws')
      : `${protocol}//localhost:8000`;
    
    const wsUrl = `${host}/api/v1/events/ws/live-feed`;

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
    if (selectedDomain === 'all') return true;
    if (selectedDomain === 'tradfi' && (e.type?.includes('tradfi') || e.source?.includes('finnhub'))) return true;
    if (selectedDomain === 'crypto' && (e.type?.includes('crypto') || e.source?.includes('binance'))) return true;
    if (selectedDomain === 'prediction' && (e.type?.includes('pred') || e.source?.includes('polymarket'))) return true;
    if (selectedDomain === 'cyber' && (e.type?.includes('cyber') || e.source?.includes('cisa') || e.source?.includes('bgp'))) return true;
    if (selectedDomain === 'maritime' && (e.type?.includes('maritime') || e.source?.includes('ais'))) return true;
    return false;
  });
}
