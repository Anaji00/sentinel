import './globals.css';
import type { Metadata, Viewport } from 'next';
import { SessionProvider } from '@/components/ui/SessionContext';
import { ServiceWorkerRegistrar } from '@/components/ServiceWorkerRegistrar';
import { THEME_BOOT } from '@/lib/theme';

export const metadata: Metadata = {
  title: 'Sentinel — markets and civic watch',
  description:
    'Stock signals graded on real prices, and a public map of who is watching your town.',
  applicationName: 'Sentinel',
  // iOS ignores the manifest's icons and standalone flag; it reads these.
  appleWebApp: { capable: true, title: 'Sentinel', statusBarStyle: 'black' },
  icons: {
    icon: [{ url: '/icons/icon-192.png', sizes: '192x192', type: 'image/png' }],
    apple: [{ url: '/icons/apple-touch-icon.png', sizes: '180x180' }],
  },
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  themeColor: '#0b0f17',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    // The boot script sets data-theme before React hydrates, so the attribute
    // differs from the server's markup by design.
    <html lang="en" className="dark" data-theme="dark" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOT }} />
      </head>
      <body className="bg-page text-ink font-sans antialiased overflow-hidden">
        <SessionProvider>{children}</SessionProvider>
        <ServiceWorkerRegistrar />
      </body>
    </html>
  );
}
