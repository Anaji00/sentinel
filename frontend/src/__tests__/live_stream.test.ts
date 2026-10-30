import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  FLUSH_MS,
  IDLE_CLOSE_MS,
  LiveSocket,
  LiveStream,
  LiveStreamDeps,
  RECONNECT_INITIAL_MS,
  liveFeedUrl,
} from '../lib/liveStream';
import { filterLiveEvents } from '../lib/useLiveEvents';
import type { NormalizedEvent } from '../lib/types';

/**
 * The shared live-feed socket.
 *
 * Each `useLiveEvents()` call used to open its own connection: six components,
 * two or three on one page, each a gateway connection, a Redis subscription and
 * an opening backlog query of its own. These pin that they now share one.
 */

class FakeSocket implements LiveSocket {
  onopen: LiveSocket['onopen'] = null;
  onmessage: LiveSocket['onmessage'] = null;
  onerror: LiveSocket['onerror'] = null;
  onclose: LiveSocket['onclose'] = null;
  closed = false;
  close() {
    this.closed = true;
  }
  open() {
    this.onopen?.({} as Event);
  }
  send(event: Partial<NormalizedEvent>) {
    this.onmessage?.({ data: JSON.stringify(event) } as MessageEvent);
  }
  drop() {
    this.onclose?.({} as CloseEvent);
  }
}

function harness(session: boolean | null = true) {
  const sockets: FakeSocket[] = [];
  const state = { connected: false, authRequired: false, touches: 0 };
  const deps: LiveStreamDeps = {
    url: () => 'wss://example.test/api/v1/events/ws/live-feed',
    openSocket: () => {
      const s = new FakeSocket();
      sockets.push(s);
      return s;
    },
    checkSession: vi.fn(async () => session),
    setConnected: (c) => {
      state.connected = c;
    },
    setAuthRequired: (r) => {
      state.authRequired = r;
    },
    touched: () => {
      state.touches += 1;
    },
  };
  return { stream: new LiveStream(deps), sockets, state, deps };
}

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

describe('LiveStream', () => {
  it('serves every subscriber from one socket', () => {
    const { stream, sockets } = harness();
    const a = vi.fn();
    const b = vi.fn();
    stream.subscribe(a);
    stream.subscribe(b);
    expect(sockets).toHaveLength(1);

    sockets[0].open();
    sockets[0].send({ event_id: 'e1', type: 'cyber_attack' });
    vi.advanceTimersByTime(FLUSH_MS);

    expect(a).toHaveBeenCalledTimes(1);
    expect(b).toHaveBeenCalledTimes(1);
    expect(stream.getSnapshot().map((e) => e.event_id)).toEqual(['e1']);
  });

  it('keeps the same snapshot until a batch lands', () => {
    // useSyncExternalStore re-renders forever if getSnapshot returns a new
    // array on every call.
    const { stream, sockets } = harness();
    stream.subscribe(() => {});
    const before = stream.getSnapshot();
    expect(stream.getSnapshot()).toBe(before);
    sockets[0].send({ event_id: 'e1', type: 'x' });
    expect(stream.getSnapshot()).toBe(before);
    vi.advanceTimersByTime(FLUSH_MS);
    expect(stream.getSnapshot()).not.toBe(before);
  });

  it('gives a late subscriber what is already buffered, without a second socket', () => {
    const { stream, sockets } = harness();
    stream.subscribe(() => {});
    sockets[0].send({ event_id: 'e1', type: 'x' });
    vi.advanceTimersByTime(FLUSH_MS);

    stream.subscribe(() => {});
    expect(sockets).toHaveLength(1);
    expect(stream.getSnapshot()).toHaveLength(1);
  });

  it('closes only after the last subscriber has been gone a while', () => {
    const { stream, sockets, state } = harness();
    const off = stream.subscribe(() => {});
    sockets[0].open();
    off();
    vi.advanceTimersByTime(IDLE_CLOSE_MS - 1);
    expect(sockets[0].closed).toBe(false);
    vi.advanceTimersByTime(1);
    expect(sockets[0].closed).toBe(true);
    expect(state.connected).toBe(false);
    expect(stream.isOpen).toBe(false);
  });

  it('keeps the socket across a page change', () => {
    // One page's panels unmount before the next page's mount.
    const { stream, sockets } = harness();
    const off = stream.subscribe(() => {});
    off();
    vi.advanceTimersByTime(IDLE_CLOSE_MS / 2);
    stream.subscribe(() => {});
    vi.advanceTimersByTime(IDLE_CLOSE_MS * 2);
    expect(sockets).toHaveLength(1);
    expect(sockets[0].closed).toBe(false);
  });

  it('ignores the opening backlog it has already seen after a reconnect', async () => {
    const { stream, sockets } = harness(true);
    stream.subscribe(() => {});
    sockets[0].send({ event_id: 'e1', type: 'x' });
    vi.advanceTimersByTime(FLUSH_MS);

    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(RECONNECT_INITIAL_MS);
    expect(sockets).toHaveLength(2);
    sockets[1].send({ event_id: 'e1', type: 'x' });
    sockets[1].send({ event_id: 'e2', type: 'x' });
    vi.advanceTimersByTime(FLUSH_MS);
    expect(stream.getSnapshot().map((e) => e.event_id).sort()).toEqual(['e1', 'e2']);
  });

  it('stops reconnecting when the session is gone', async () => {
    const { stream, sockets, state } = harness(false);
    stream.subscribe(() => {});
    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(RECONNECT_INITIAL_MS * 10);
    expect(state.authRequired).toBe(true);
    expect(sockets).toHaveLength(1);
  });

  it('backs off on a failed session check rather than giving up', async () => {
    const { stream, sockets, state } = harness(null);
    stream.subscribe(() => {});
    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(RECONNECT_INITIAL_MS);
    expect(state.authRequired).toBe(false);
    expect(sockets).toHaveLength(2);
  });

  it('does not reconnect from a session check that outlived a stop', async () => {
    const { stream, sockets, deps } = harness(true);
    let answer: (v: boolean) => void = () => {};
    deps.checkSession = () => new Promise((resolve) => (answer = resolve));
    stream.subscribe(() => {});
    sockets[0].drop(); // session check now in flight
    stream.stop(); // a subscriber is still there, so only the stop can veto it
    answer(true);
    await vi.advanceTimersByTimeAsync(RECONNECT_INITIAL_MS * 4);
    expect(sockets).toHaveLength(1);
  });

  it('keeps at most 300 events', () => {
    const { stream, sockets } = harness();
    stream.subscribe(() => {});
    for (let i = 0; i < 450; i += 1) sockets[0].send({ event_id: `e${i}`, type: 'x' });
    vi.advanceTimersByTime(FLUSH_MS);
    expect(stream.getSnapshot()).toHaveLength(300);
    expect(stream.getSnapshot()[0].event_id).toBe('e449');
  });
});

describe('liveFeedUrl', () => {
  it('follows the page origin and its scheme', () => {
    expect(liveFeedUrl({ protocol: 'https:', host: '127.0.0.1' })).toBe(
      'wss://127.0.0.1/api/v1/events/ws/live-feed',
    );
    expect(liveFeedUrl({ protocol: 'http:', host: 'localhost:3000' })).toBe(
      'ws://localhost:3000/api/v1/events/ws/live-feed',
    );
  });

  it('takes the explicit overrides', () => {
    const loc = { protocol: 'https:', host: 'page.test' };
    expect(liveFeedUrl(loc, { wsUrl: 'wss://feed.test/' })).toBe(
      'wss://feed.test/api/v1/events/ws/live-feed',
    );
    expect(liveFeedUrl(loc, { apiUrl: 'https://api.test/api/v1' })).toBe(
      'wss://api.test/api/v1/events/ws/live-feed',
    );
  });
});

describe('filterLiveEvents', () => {
  const rows = [
    { event_id: '1', type: 'tradfi_price_tick', source: 'finnhub' },
    { event_id: '2', type: 'vessel_position', source: 'aisstream' },
  ] as unknown as NormalizedEvent[];

  it('passes everything for all', () => {
    expect(filterLiveEvents(rows, 'all')).toHaveLength(2);
  });

  it('narrows to one domain', () => {
    expect(filterLiveEvents(rows, 'maritime').map((e) => e.event_id)).toEqual(['2']);
  });
});
