'use client';

import { useEffect } from 'react';
import { SERVICE_WORKER_URL, shouldRegisterServiceWorker } from '@/lib/pwa';

/** Registers the offline/installability worker once, after hydration. Renders nothing. */
export function ServiceWorkerRegistrar(): null {
  useEffect(() => {
    const ok = shouldRegisterServiceWorker({
      nodeEnv: process.env.NODE_ENV,
      hasServiceWorker: 'serviceWorker' in navigator,
      isSecureContext: window.isSecureContext,
    });
    if (!ok) return;
    navigator.serviceWorker
      .register(SERVICE_WORKER_URL, { scope: '/' })
      // The app works without it; say so rather than failing quietly.
      .catch((err: unknown) => console.warn('Service worker registration failed', err));
  }, []);
  return null;
}
