import type { MetadataRoute } from 'next';

/**
 * The web app manifest, served at /manifest.webmanifest.
 *
 * The path has an extension, so the middleware's matcher skips it: a phone
 * adding the app to its home screen fetches this before anyone has signed in,
 * and a redirect to /login here would make the install fail silently.
 *
 * Colours are the dark theme's page background, which is the default theme
 * and what the splash screen shows before the boot script can read storage.
 */
export default function manifest(): MetadataRoute.Manifest {
  return {
    id: '/',
    name: 'Sentinel',
    short_name: 'Sentinel',
    description: 'Stock signals graded on real prices, and a public map of who is watching your town.',
    start_url: '/',
    scope: '/',
    display: 'standalone',
    orientation: 'portrait',
    background_color: '#0b0f17',
    theme_color: '#0b0f17',
    icons: [
      { src: '/icons/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/icons/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
      { src: '/icons/maskable-192.png', sizes: '192x192', type: 'image/png', purpose: 'maskable' },
      { src: '/icons/maskable-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' },
    ],
  };
}
