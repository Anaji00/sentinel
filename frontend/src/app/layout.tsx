import './globals.css';
import type { Metadata } from 'next';
import { SessionProvider } from '@/components/ui/SessionContext';

export const metadata: Metadata = {
  title: 'SENTINEL — Autonomous Market Intelligence & Quantitative Operations',
  description:
    'Enterprise Autonomous Quantitative Trading, Macro Regime Tracking & Event-Driven Intelligence Platform.',
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
