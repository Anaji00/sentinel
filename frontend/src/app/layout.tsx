import './globals.css';
import type { Metadata } from 'next';
import { SessionProvider } from '@/components/ui/SessionContext';

export const metadata: Metadata = {
  title: 'Sentinel — markets and civic watch',
  description:
    'Stock signals graded on real prices, and a public map of who is watching your town.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="bg-page text-ink font-sans antialiased overflow-hidden">
        <SessionProvider>{children}</SessionProvider>
      </body>
    </html>
  );
}
