import { readFileSync } from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { describe, expect, it, vi } from 'vitest';
import manifest from '@/app/manifest';
import { shouldRegisterServiceWorker } from '@/lib/pwa';

const PUBLIC = path.resolve(__dirname, '../../public');
const ORIGIN = 'https://sentinel.test';

describe('manifest', () => {
  const m = manifest();

  it('installs standalone from the root', () => {
    expect(m.display).toBe('standalone');
    expect(m.start_url).toBe('/');
    expect(m.scope).toBe('/');
  });

  it('has any and maskable icons at 192 and 512, all present on disk', () => {
    const icons = m.icons ?? [];
    for (const purpose of ['any', 'maskable']) {
      const sizes = icons.filter((i) => i.purpose === purpose).map((i) => i.sizes);
      expect(sizes).toEqual(['192x192', '512x512']);
    }
    for (const icon of icons) {
      expect(() => readFileSync(path.join(PUBLIC, icon.src))).not.toThrow();
    }
  });
});

describe('shouldRegisterServiceWorker', () => {
  const ready = { nodeEnv: 'production', hasServiceWorker: true, isSecureContext: true };

  it('registers in a secure production page', () => {
    expect(shouldRegisterServiceWorker(ready)).toBe(true);
  });

  it.each([
    ['under next dev', { ...ready, nodeEnv: 'development' }],
    ['without service worker support', { ...ready, hasServiceWorker: false }],
    ['on plain http', { ...ready, isSecureContext: false }],
  ])('does not register %s', (_label, env) => {
    expect(shouldRegisterServiceWorker(env)).toBe(false);
  });
});

/**
 * Runs the real public/sw.js against a fake worker global, so the caching
 * rules are tested as shipped rather than as a copy.
 */
function loadWorker(network: (req: FakeRequest) => Promise<Response>) {
  const listeners: Record<string, (event: unknown) => void> = {};
  const stores = new Map<string, Map<string, Response>>();
  const store = (name: string) => {
    if (!stores.has(name)) stores.set(name, new Map());
    return stores.get(name)!;
  };
  const keyOf = (r: FakeRequest | string) => (typeof r === 'string' ? new URL(r, ORIGIN).href : r.url);
  const caches = {
    open: async (name: string) => {
      const s = store(name);
      return {
        addAll: async (urls: string[]) => urls.forEach((u) => s.set(keyOf(u), new Response(`precached ${u}`))),
        put: async (req: FakeRequest, res: Response) => void s.set(keyOf(req), res),
        keys: async () => [...s.keys()],
        delete: async (k: string) => s.delete(k),
      };
    },
    match: async (req: FakeRequest | string) => {
      for (const s of stores.values()) {
        const hit = s.get(keyOf(req));
        if (hit) return hit.clone();
      }
      return undefined;
    },
    keys: async () => [...stores.keys()],
    delete: async (name: string) => stores.delete(name),
  };
  const self = {
    location: { origin: ORIGIN },
    addEventListener: (type: string, fn: (event: unknown) => void) => (listeners[type] = fn),
    skipWaiting: vi.fn(async () => undefined),
    clients: { claim: vi.fn(async () => undefined) },
  };
  const source = readFileSync(path.join(PUBLIC, 'sw.js'), 'utf8');
  vm.runInNewContext(source, { self, caches, fetch: network, URL, Response, Promise });

  async function dispatch(type: string): Promise<void> {
    let pending: Promise<unknown> = Promise.resolve();
    listeners[type]({ waitUntil: (p: Promise<unknown>) => (pending = p) });
    await pending;
  }

  /** Sends a fetch event; resolves to the worker's response, or null if it let the request through. */
  async function request(req: FakeRequest): Promise<Response | null> {
    let responded: Promise<Response> | null = null;
    const waits: Promise<unknown>[] = [];
    listeners.fetch({
      request: req,
      respondWith: (p: Promise<Response>) => (responded = p),
      waitUntil: (p: Promise<unknown>) => waits.push(p),
    });
    if (!responded) return null;
    const res = await responded;
    await Promise.all(waits);
    return res;
  }

  return { dispatch, request, stores };
}

interface FakeRequest {
  url: string;
  method: string;
  mode: string;
}

const get = (p: string, mode = 'cors'): FakeRequest => ({ url: `${ORIGIN}${p}`, method: 'GET', mode });
const offline = async (): Promise<Response> => {
  throw new TypeError('Failed to fetch');
};
const online = async (req: FakeRequest): Promise<Response> => {
  const res = new Response(`network ${req.url}`, { status: 200 });
  Object.defineProperty(res, 'type', { value: 'basic' });
  return res;
};

describe('service worker', () => {
  it('precaches the offline page on install', async () => {
    const sw = loadWorker(online);
    await sw.dispatch('install');
    expect([...sw.stores.get('sentinel-shell-v1')!.keys()]).toContain(`${ORIGIN}/offline.html`);
  });

  it('leaves /api requests to the network, online or not', async () => {
    const sw = loadWorker(offline);
    await sw.dispatch('install');
    expect(await sw.request(get('/api/proxy/positions'))).toBeNull();
    expect(await sw.request(get('/api/auth/session', 'navigate'))).toBeNull();
  });

  it('ignores non-GET and cross-origin requests', async () => {
    const sw = loadWorker(online);
    expect(await sw.request({ ...get('/_next/static/a.js'), method: 'POST' })).toBeNull();
    expect(await sw.request({ url: 'https://elsewhere.test/x.js', method: 'GET', mode: 'cors' })).toBeNull();
  });

  it('serves pages from the network and never stores them', async () => {
    const sw = loadWorker(online);
    await sw.dispatch('install');
    const res = await sw.request(get('/portfolio', 'navigate'));
    expect(await res!.text()).toBe(`network ${ORIGIN}/portfolio`);
    const stored = [...sw.stores.values()].flatMap((s) => [...s.keys()]);
    expect(stored).not.toContain(`${ORIGIN}/portfolio`);
  });

  it('shows the offline page when a navigation cannot reach the server', async () => {
    const sw = loadWorker(offline);
    await sw.dispatch('install');
    const res = await sw.request(get('/portfolio', 'navigate'));
    expect(await res!.text()).toBe('precached /offline.html');
  });

  it('caches hashed static files and serves them offline afterwards', async () => {
    let net = online;
    const sw = loadWorker((r) => net(r));
    await sw.request(get('/_next/static/chunks/app.js'));
    net = offline;
    const res = await sw.request(get('/_next/static/chunks/app.js'));
    expect(await res!.text()).toBe(`network ${ORIGIN}/_next/static/chunks/app.js`);
  });

  it('deletes caches from older worker versions on activate', async () => {
    const sw = loadWorker(online);
    sw.stores.set('sentinel-static-v0', new Map());
    sw.stores.set('unrelated', new Map());
    await sw.dispatch('install');
    await sw.dispatch('activate');
    expect([...sw.stores.keys()].sort()).toEqual(['sentinel-shell-v1', 'unrelated']);
  });
});
