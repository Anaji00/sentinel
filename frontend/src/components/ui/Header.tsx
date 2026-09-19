'use client';

/**
 * Application header.
 *
 * Rebuilt because most of what it displayed was not true. The account button
 * showed "A. VANCE / INSTITUTIONAL" with the initials "AV" as literals, so
 * every user who signed in saw a fictional person's name instead of their own.
 * The telemetry strip asserted "AIS TANKERS: ACTIVE", "ADS-B FLIGHTS: TRACKING"
 * and "AGENT SWARM: ACTIVE (8)" as fixed text -- the deployment runs ten agents,
 * and none of those three statuses was read from anything. The version badge
 * "v2.4 EDA ACTIVE" was likewise hardcoded.
 *
 * What remains is measured: the stream state from the telemetry store, gateway
 * latency from a timed request, the live agent count from /agents/processes, and
 * the signed-in identity from the session. The visual treatment follows the same
 * rules as the panels -- no neon halo, no backdrop blur, cyan reserved for what
 * is interactive.
 */

import React, { useState, useEffect } from 'react';
import useSWR from 'swr';
import { Command, Menu } from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { IconLock } from './icons';
import SystemHealthHUD from '../SystemHealthHUD';
import { apiClient, fetcher } from '../../lib/api';
import { useTelemetryStore } from '../../lib/store';
import { AccountProfileModal } from '../AccountProfileModal';
import { useNav } from './NavContext';
import { TIMEZONES, useTimeZone } from './TimeZoneContext';
import { useSession } from './SessionContext';
import { POLL } from '../ui/DataProvider';

/** Initials for the avatar, from whatever the session actually gives us. */
function initialsFor(email?: string | null): string {
  if (!email) return '··';
  const local = email.split('@')[0] || '';
  const parts = local.split(/[._-]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return local.slice(0, 2).toUpperCase() || '··';
}

export const Header: React.FC = () => {
  const { drawerOpen, toggleDrawer } = useNav();
  const pathname = usePathname();
  const [time, setTime] = useState<string>('');
  // Shared, not local. This select governs every timestamp the application
  // renders -- it used to govern only the clock beside it, while the event
  // rows below rendered in whatever zone the machine happened to be in.
  const { zone: timezone, setZone: setTimezone } = useTimeZone();
  const [latency, setLatency] = useState<number | null>(null);
  const [isProfileOpen, setIsProfileOpen] = useState<boolean>(false);
  const [isMac, setIsMac] = useState<boolean>(false);

  const isConnected = useTelemetryStore((state) => state.isConnected);
  // Distinct from "connecting": the feed was refused for lack of a session.
  const authRequired = useTelemetryStore((state) => state.authRequired);

  // One shared session, rather than this component, the account modal and
  // the live-events hook each asking separately and disagreeing.
  const session = useSession();

  const { data: processes } = useSWR<{ active_agents_count: number }>(
    '/agents/processes',
    fetcher,
    { refreshInterval: POLL.standard },
  );

  useEffect(() => {
    setIsMac(/Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent));
  }, []);

  useEffect(() => {
    const updateClock = () => {
      const now = new Date();
      try {
        const parts = new Intl.DateTimeFormat('en-US', {
          timeZone: timezone,
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hourCycle: 'h23',
          timeZoneName: 'short',
        }).formatToParts(now);
        const p: Record<string, string> = {};
        parts.forEach((part) => {
          p[part.type] = part.value;
        });
        setTime(`${p.hour}:${p.minute}:${p.second} ${p.timeZoneName || ''}`);
      } catch {
        setTime(now.toISOString().substring(11, 19) + 'UTC');
      }
    };
    updateClock();
    const interval = setInterval(updateClock, 1000);
    return () => clearInterval(interval);
  }, [timezone]);

  useEffect(() => {
    const measureLatency = async () => {
      const start = Date.now();
      try {
        await apiClient.get('/health');
        setLatency(Date.now() - start);
      } catch {
        // A failed probe is not a latency reading. Reporting the elapsed time of
        // a request that never completed would present an outage as a number.
        setLatency(null);
      }
    };
    measureLatency();
    const timer = setInterval(measureLatency, 10000);
    return () => clearInterval(timer);
  }, []);

  const { email, role, status } = session;

  // Signing in returns the operator to the page they were on, which is the
  // whole reason this control exists in the header rather than only on the
  // login screen.
  const signInHref = `/login?next=${encodeURIComponent(pathname || '/')}`;
  const agentCount = processes?.active_agents_count;

  const streamTone = isConnected
    ? 'tone-positive'
    : authRequired
      ? 'tone-negative'
      : 'tone-caution';
  const streamDot = isConnected ? 'bg-emerald-400' : authRequired ? 'bg-rose-400' : 'bg-amber-400';

  return (
    <header
      className="h-14 min-h-[56px] w-full shrink-0 z-40 flex items-center justify-between gap-4
                 px-4 sm:px-6 bg-inset border-b border-line"
    >
      {/* Identity */}
      <div className="flex items-center gap-3 shrink-0 min-w-0">
        {/* The only way to the navigation below `md`, where the sidebar leaves
            the flow. Above it the sidebar is always visible and this is not. */}
        <button
          type="button"
          onClick={toggleDrawer}
          aria-label={drawerOpen ? 'Close navigation' : 'Open navigation'}
          aria-expanded={drawerOpen}
          className="md:hidden -ml-1 flex h-8 w-8 items-center justify-center rounded-md
                     text-ink-dim hover:text-ink hover:bg-overlay/60
                     transition-colors outline-none focus-visible:ring-1
                     focus-visible:ring-cyan-400/60"
        >
          <Menu className="h-4 w-4" />
        </button>
        <div
          className="h-8 w-8 rounded-lg bg-accent-dim border border-line-accent
                        flex items-center justify-center shrink-0"
        >
          <span className="text-accent font-semibold text-sm">S</span>
        </div>
        <div className="min-w-0">
          {/* Not an <h1>. The brand sits in the chrome on every route, so an
              h1 here gave each page two top-level headings and made the
              document outline start with the product name rather than with
              what the page is. */}
          <span className="block text-sm font-semibold text-ink leading-tight truncate">
            Sentinel
          </span>
          <p className="text-micro text-ink-mute leading-tight truncate hidden sm:block">
            Multi-domain intelligence
          </p>
        </div>
      </div>

      {/* Measured state */}
      <div className="hidden lg:flex items-center gap-4 text-micro min-w-0">
        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <span className={`h-1.5 w-1.5 rounded-full ${streamDot}`} />
          <span className="text-ink-mute">Stream</span>
          {authRequired ? (
            <Link
              href={signInHref}
              className="text-negative underline underline-offset-2 hover:text-ink"
            >
              sign in
            </Link>
          ) : (
            <span className={streamTone}>{isConnected ? 'live' : 'connecting'}</span>
          )}
        </span>

        <span className="text-ink-mute">·</span>

        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <span className="text-ink-mute">Agents</span>
          <span className="text-ink-dim tabular">
            {typeof agentCount === 'number' ? agentCount : '—'}
          </span>
        </span>

        <span className="text-ink-mute">·</span>

        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <span className="text-ink-mute">Gateway</span>
          <span className={latency === null ? 'tone-negative' : 'text-ink-dim tabular'}>
            {latency === null ? 'unreachable' : `${latency}ms`}
          </span>
        </span>
      </div>

      {/* Controls */}
      <div className="flex items-center gap-2 sm:gap-3 shrink-0">
        <span
          className="hidden xl:flex items-center gap-1 text-micro text-ink-mute border border-line
                     rounded px-1.5 py-1"
          title="Open the command palette"
        >
          {isMac ? <Command className="h-3 w-3" /> : <span className="font-medium">Ctrl</span>}
          <span className="font-medium">K</span>
        </span>

        <div className="hidden xl:flex items-center gap-2">
          <span
            className="text-micro text-ink-dim tabular whitespace-nowrap"
            suppressHydrationWarning
          >
            {time}
          </span>
          <select
            value={timezone}
            onChange={(e) => setTimezone(e.target.value)}
            className="bg-raised text-micro text-ink-dim border border-line
                       rounded px-1.5 py-1 outline-none cursor-pointer hover:text-ink transition-colors"
            aria-label="Clock timezone"
          >
            {TIMEZONES.map(([tz, label]) => (
              <option key={tz} value={tz}>
                {label}
              </option>
            ))}
          </select>
        </div>

        <SystemHealthHUD />

        {/* Three states, and the third is why this is not a ternary.
            While the session is still being established the control renders a
            quiet placeholder: flashing "Sign in" at an operator who is signed
            in, on every page load, is how an interface teaches someone to
            distrust what it says. */}
        {status === 'loading' ? (
          <div aria-hidden className="h-9 w-9 rounded-lg border border-line bg-raised sm:w-36" />
        ) : status === 'anonymous' ? (
          <Link
            href={signInHref}
            className="flex cursor-pointer items-center gap-2 rounded-lg border border-line-accent bg-accent-dim px-3 py-1.5 text-micro font-semibold text-accent transition-colors hover:border-accent"
          >
            <IconLock />
            Sign in
          </Link>
        ) : (
          <button
            onClick={() => setIsProfileOpen(true)}
            aria-label={`Account: ${email ?? 'signed in'}`}
            className="flex cursor-pointer items-center gap-2 rounded-lg border border-line py-1.5 pl-1.5 pr-2.5 transition-colors hover:border-line-strong hover:bg-raised"
          >
            <span className="flex h-6 w-6 items-center justify-center rounded border border-line-accent bg-accent-dim text-micro font-semibold text-accent">
              {initialsFor(email)}
            </span>
            <span className="hidden min-w-0 flex-col text-left sm:flex">
              <span className="max-w-[160px] truncate text-micro leading-tight text-ink">
                {email}
              </span>
              {role && (
                <span className="text-micro leading-tight tracking-wide text-ink-mute">{role}</span>
              )}
            </span>
          </button>
        )}
      </div>

      <AccountProfileModal isOpen={isProfileOpen} onClose={() => setIsProfileOpen(false)} />
    </header>
  );
};
