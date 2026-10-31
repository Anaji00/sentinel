/**
 * When the browser should be given the service worker.
 *
 * Production builds only. Under `next dev` the worker would hold on to chunks
 * that hot reload replaces, and a stale worker on localhost outlives the dev
 * server and confuses the next one. A secure context is required by the
 * browser anyway; checking it first turns a console error into a no-op on a
 * plain-http LAN address.
 */
export interface PwaEnvironment {
  nodeEnv: string | undefined;
  hasServiceWorker: boolean;
  isSecureContext: boolean;
}

export function shouldRegisterServiceWorker(env: PwaEnvironment): boolean {
  return env.nodeEnv === 'production' && env.hasServiceWorker && env.isSecureContext;
}

export const SERVICE_WORKER_URL = '/sw.js';
