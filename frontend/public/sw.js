/**
 * Sentinel's service worker: installability and an offline screen, nothing more.
 *
 * What it caches is deliberately narrow. Every page and every /api response is
 * a signed-in user's data -- positions, P&L, alerts -- and a copy of that in
 * the Cache Storage of a phone outlives sign-out and survives a lost device.
 * So the only things stored are files that are the same for everyone:
 *
 *   - /_next/static/*  content-hashed build output, cache-first, capped
 *   - /icons/*         the install icons
 *   - /offline.html    shown when a navigation cannot reach the server
 *
 * Pages are network-first with no copy kept: offline, a navigation gets the
 * offline screen rather than yesterday's dashboard presented as today's.
 * /api/* is not intercepted at all, so a 401 still reaches the client code
 * that handles it.
 *
 * Bump VERSION when this file's caching rules change; activate deletes every
 * sentinel-* cache that is not current.
 */
const VERSION = 'v1';
const SHELL_CACHE = `sentinel-shell-${VERSION}`;
const STATIC_CACHE = `sentinel-static-${VERSION}`;
const OFFLINE_URL = '/offline.html';
const PRECACHE = [OFFLINE_URL, '/icons/icon-192.png', '/icons/icon-512.png'];
// Each deploy adds a fresh set of hashed chunks under new names; the cap keeps
// old builds from accumulating on the device indefinitely.
const STATIC_LIMIT = 300;

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches
      .open(SHELL_CACHE)
      .then((cache) => cache.addAll(PRECACHE))
      // Safe to take over at once: pages are never served from cache, and
      // static files are keyed by content hash, so old and new builds cannot mix.
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((k) => k.startsWith('sentinel-') && k !== SHELL_CACHE && k !== STATIC_CACHE)
            .map((k) => caches.delete(k)),
        ),
      )
      .then(() => self.clients.claim()),
  );
});

/** Which strategy a request gets, or null to leave it to the network untouched. */
function route(request) {
  if (request.method !== 'GET') return null;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return null;
  if (url.pathname.startsWith('/api/')) return null;
  if (request.mode === 'navigate') return 'page';
  if (url.pathname.startsWith('/_next/static/') || url.pathname.startsWith('/icons/')) return 'static';
  return null;
}

async function page(request) {
  try {
    // A 5xx or a redirect to /login is a real answer from the server and is
    // passed through; only an unreachable network falls back.
    return await fetch(request);
  } catch {
    return (await caches.match(OFFLINE_URL)) || Response.error();
  }
}

async function trim(cache) {
  const keys = await cache.keys();
  // Keys come back in insertion order: the oldest go first.
  await Promise.all(keys.slice(0, Math.max(0, keys.length - STATIC_LIMIT)).map((k) => cache.delete(k)));
}

async function staticAsset(request, event) {
  const cached = await caches.match(request);
  if (cached) return cached;
  const response = await fetch(request);
  if (response.ok && response.type === 'basic') {
    const copy = response.clone();
    event.waitUntil(
      caches.open(STATIC_CACHE).then(async (cache) => {
        await cache.put(request, copy);
        await trim(cache);
      }),
    );
  }
  return response;
}

self.addEventListener('fetch', (event) => {
  const kind = route(event.request);
  if (kind === 'page') event.respondWith(page(event.request));
  else if (kind === 'static') event.respondWith(staticAsset(event.request, event));
});
