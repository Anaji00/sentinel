import './globals.css';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'SENTINEL — Autonomous Market Intelligence & Quantitative Operations',
  description: 'Enterprise Autonomous Quantitative Trading, Macro Regime Tracking & Event-Driven Intelligence Platform.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#08090c] text-slate-100 font-sans antialiased overflow-hidden select-none">
        {children}
      </body>
    </html>
  );
}
