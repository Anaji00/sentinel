import { NormalizedEvent } from './types';

/**
 * One live-feed socket per tab, shared by every `useLiveEvents()` caller.
 *
 * Each hook used to open its own. Six components call it, and a page such as
 * /charts mounted two or three at once -- each a separate gateway connection,
 * a separate Redis subscription, a separate opening backlog query, and its own
 * copy of the same 300 events. The socket carries every domain because the
 * consumers between them want every domain (the map and MarketCharts ask for
 * `all`); each hook filters its own view from the one shared buffer.
 */

// Max events kept in memory, shared by every subscriber
export const MAX_LIVE_EVENTS = 300;
// Reconnect backoff limits (ms)
export const RECONNECT_INITIAL_MS = 1000;
export const RECONNECT_MAX_MS = 30000;
// 1000ms sliding window buffer to reduce React re-render flooding from AIS/ADS-B telemetry
export const FLUSH_MS = 1000;
/**
 * How long the socket outlives its last subscriber.
 *
 * Navigating between two live pages unmounts one set of panels before the next
 * mounts, and StrictMode in development mounts everything twice. Closing at
 * zero would drop and reopen the connection -- and rerun the gateway's opening
 * backlog -- on every one of those.
 */
export const IDLE_CLOSE_MS = 5000;

/** One position report per entity, plus every non-position event, order kept.
 *
 * Exported so a test can exercise it rather than restate it. The previous test
 * copied this loop into its own body and asserted on the copy, which cannot
 * fail when the hook changes.
 */
export function dedupePositionBatch(batch: NormalizedEvent[]): NormalizedEvent[] {
  const positionMap = new Map<string, NormalizedEvent>();
  const nonPositionEvents: NormalizedEvent[] = [];

  for (const item of batch) {
    const t = (item.type || '').toLowerCase();
    const isHighFreqPos =
      t.includes('vessel_position') ||
      t.includes('adsb_position') ||
      t.includes('ais') ||
      t.includes('adsb');
    // An unattributed position report is still an event: collapsing those
    // together on a missing key would drop all but one of them.
    if (isHighFreqPos && item.primary_entity?.id) {
      if (!positionMap.has(item.primary_entity.id)) {
        positionMap.set(item.primary_entity.id, item);
      }
    } else {
      nonPositionEvents.push(item);
    }
  }
  return [...Array.from(positionMap.values()), ...nonPositionEvents];
}

/** The next reconnect delay, doubling up to the ceiling. */
export function reconnectDelay(current: number): number {
  return Math.min(current * 2, RECONNECT_MAX_MS);
}

/** Where the live feed lives, from the page's own location. */
export function liveFeedUrl(
  location: { protocol: string; host: string },
  env: { wsUrl?: string; apiUrl?: string } = {},
): string {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  // Default to the same origin the page is served from (through the ingress),
  // NOT a hardcoded :8000 — the gateway port is not published to the host in the
  // deployed stack, and http://...:8000 would be blocked as mixed content on HTTPS.
  // NEXT_PUBLIC_WS_URL / NEXT_PUBLIC_API_URL remain explicit overrides.
  let baseHost = location.host;
  if (env.wsUrl) {
    baseHost = env.wsUrl.replace(/^wss?:\/\//, '').replace(/\/+$/, '');
  } else if (env.apiUrl) {
    baseHost = env.apiUrl
      .replace(/^https?:\/\//, '')
      .replace(/\/api\/v1\/?$/, '')
      .replace(/\/+$/, '');
  }
  return `${protocol}//${baseHost}/api/v1/events/ws/live-feed`;
}

/** The part of a WebSocket the stream uses, so a test can hand it a fake. */
export interface LiveSocket {
  onopen: ((ev: Event) => void) | null;
  onmessage: ((ev: MessageEvent) => void) | null;
  onerror: ((ev: Event) => void) | null;
  onclose: ((ev: CloseEvent) => void) | null;
  close(): void;
}

export interface LiveStreamDeps {
  url: () => string;
  openSocket: (url: string) => LiveSocket;
  /** true signed in, false refused, null unknown (the check itself failed). */
  checkSession: () => Promise<boolean | null>;
  setConnected: (connected: boolean) => void;
  setAuthRequired: (required: boolean) => void;
  /** A batch reached the buffer. */
  touched: () => void;
}

const EMPTY: NormalizedEvent[] = [];

export class LiveStream {
  private events: NormalizedEvent[] = EMPTY;
  private listeners = new Set<() => void>();
  private socket: LiveSocket | null = null;
  private seenIds = new Set<string>();
  private pending: NormalizedEvent[] = [];
  private backoff = RECONNECT_INITIAL_MS;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
  // Bumped on every stop, so a session check still in flight from a socket
  // that has since been stopped cannot schedule a reconnect behind it.
  private generation = 0;

  constructor(private readonly deps: LiveStreamDeps) {}

  /** For `useSyncExternalStore`: the same array until a batch lands. */
  getSnapshot = (): NormalizedEvent[] => this.events;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
    if (!this.socket && !this.reconnectTimer) this.connect();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0 && !this.idleTimer) {
        this.idleTimer = setTimeout(() => this.stop(), IDLE_CLOSE_MS);
      }
    };
  };

  get subscriberCount(): number {
    return this.listeners.size;
  }

  get isOpen(): boolean {
    return this.socket !== null;
  }

  /** Close the socket and cancel every timer. The buffer is kept for the next subscriber. */
  stop(): void {
    this.generation += 1;
    for (const timer of [this.idleTimer, this.reconnectTimer]) {
      if (timer) clearTimeout(timer);
    }
    this.idleTimer = null;
    this.reconnectTimer = null;
    this.flush();
    const ws = this.socket;
    this.socket = null;
    this.backoff = RECONNECT_INITIAL_MS;
    if (ws) {
      ws.onclose = null;
      ws.onerror = null;
      ws.onmessage = null;
      ws.close();
      this.deps.setConnected(false);
    }
  }

  private connect(): void {
    this.reconnectTimer = null;
    if (this.listeners.size === 0) return;
    let ws: LiveSocket;
    try {
      ws = this.deps.openSocket(this.deps.url());
    } catch (err) {
      console.warn('WebSocket connection failed to open:', err);
      this.deps.setConnected(false);
      this.scheduleReconnect();
      return;
    }
    this.socket = ws;

    ws.onopen = () => {
      if (this.socket !== ws) return;
      // Reset backoff on successful connection & sync Zustand store
      this.backoff = RECONNECT_INITIAL_MS;
      this.deps.setConnected(true);
      this.deps.touched();
    };

    ws.onmessage = (event) => {
      if (this.socket !== ws) return;
      try {
        const data = JSON.parse(event.data);
        if (data && data.event_id) {
          // O(1) dedup check via Set
          if (this.seenIds.has(data.event_id)) return;
          this.seenIds.add(data.event_id);

          if (this.seenIds.size > MAX_LIVE_EVENTS * 2) {
            const ids = Array.from(this.seenIds);
            this.seenIds = new Set(ids.slice(ids.length - MAX_LIVE_EVENTS));
          }

          this.pending.unshift(data);
          if (!this.flushTimer) {
            this.flushTimer = setTimeout(() => this.flush(), FLUSH_MS);
          }
        }
      } catch (e) {
        console.error('Error parsing WebSocket message:', e);
      }
    };

    ws.onerror = () => {
      if (this.socket !== ws) return;
      this.deps.setConnected(false);
    };

    ws.onclose = () => {
      if (this.socket !== ws) return;
      this.socket = null;
      this.deps.setConnected(false);
      const generation = this.generation;

      // The gateway refuses the handshake before accepting it, so an
      // unauthenticated browser sees an abnormal close (1006) rather than
      // the server's 4003 policy code. Retrying on that forever is what left
      // the dashboard reading "CONNECTING..." when the real cause was a
      // missing session. Ask the session endpoint directly instead of
      // inferring from a close code that never arrives.
      void this.deps.checkSession().then((signedIn) => {
        if (generation !== this.generation || this.socket || this.listeners.size === 0) return;
        if (signedIn === false) {
          this.deps.setAuthRequired(true);
          return; // Reconnecting cannot succeed until the user signs in.
        }
        // null: the session check itself failed. Treat it as a transient
        // network fault and fall through to the normal backoff.
        if (signedIn) this.deps.setAuthRequired(false);
        this.scheduleReconnect();
      });
    };
  }

  private scheduleReconnect(): void {
    if (this.listeners.size === 0 || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => this.connect(), this.backoff);
    this.backoff = reconnectDelay(this.backoff);
  }

  private flush(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.pending.length === 0) return;
    const batch = dedupePositionBatch(this.pending);
    this.pending = [];
    this.events = [...batch, ...this.events].slice(0, MAX_LIVE_EVENTS);
    this.deps.touched();
    for (const listener of this.listeners) listener();
  }
}

/** The page's own stream, created on first use in the browser. */
let shared: LiveStream | null = null;

export function sharedLiveStream(deps: () => LiveStreamDeps): LiveStream {
  if (!shared) shared = new LiveStream(deps());
  return shared;
}

export const EMPTY_LIVE_EVENTS = EMPTY;
