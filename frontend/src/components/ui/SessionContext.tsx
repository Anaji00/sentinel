'use client';

/**
 * Who is signed in, asked once.
 *
 * Three places fetched `/api/auth/session` independently -- the header, the
 * account modal, and the live-events hook -- each with its own error handling
 * and its own idea of what a failure means. So the header could be showing an
 * email while the live feed had already been refused, and nothing reconciled
 * them.
 *
 * More importantly, nothing in the application did anything with the answer.
 * There was no sign-in control: the account button opened a profile panel
 * whether or not there was a profile, and an anonymous visitor got a dashboard
 * of sixteen panels all failing with 401 and a header politely reading "Not
 * signed in". There was no sign-out control either -- `/api/auth/logout`
 * existed and had no caller anywhere in the codebase.
 *
 * Three states, not two. `loading` is distinct from `anonymous` because the
 * difference decides whether the header renders a "Sign in" button: flashing
 * one at a signed-in operator on every page load is how an interface teaches
 * someone to distrust it.
 */

import React from 'react';
import useSWR from 'swr';
import { useRouter } from 'next/navigation';
import { POLL } from './DataProvider';

export type SessionStatus = 'loading' | 'authenticated' | 'anonymous';

export interface Session {
  status: SessionStatus;
  email: string | null;
  role: string | null;
  /** Re-ask the server. Called after signing in or out. */
  refresh: () => void;
  /** Clear the cookie and go to the sign-in screen. */
  signOut: () => Promise<void>;
}

interface SessionResponse {
  authenticated: boolean;
  user?: { email: string; role: string };
}

const SessionContext = React.createContext<Session | null>(null);

/**
 * A 401 here is an answer, not a failure.
 *
 * `/api/auth/session` returns 401 to say "nobody is signed in", which is the
 * normal case on the login screen. Throwing on it would put SWR into an error
 * state, and the global handler in DataProvider would redirect the login page
 * to itself.
 */
async function fetchSession(url: string): Promise<SessionResponse> {
  const res = await fetch(url, { credentials: 'same-origin' });
  if (res.status === 401) return { authenticated: false };
  if (!res.ok) throw new Error(`session probe failed: ${res.status}`);
  return res.json();
}

export function SessionProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const { data, isLoading, mutate } = useSWR<SessionResponse>('/api/auth/session', fetchSession, {
    // Long enough not to be chatter, short enough that a session revoked
    // elsewhere stops looking valid within a minute.
    refreshInterval: POLL.slow,
    revalidateOnFocus: true,
    // The session is the one thing worth re-asking about after a network
    // blip, because everything else on screen depends on the answer.
    shouldRetryOnError: true,
    errorRetryCount: 2,
  });

  const signOut = React.useCallback(async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST', credentials: 'same-origin' });
    } catch {
      // The cookie may already be gone, or the network may be down. Either
      // way the right next move is the same: stop showing a signed-in shell.
    }
    await mutate({ authenticated: false }, { revalidate: false });
    router.push('/login');
  }, [mutate, router]);

  const value = React.useMemo<Session>(() => {
    const status: SessionStatus =
      isLoading && !data ? 'loading' : data?.authenticated ? 'authenticated' : 'anonymous';
    return {
      status,
      email: data?.user?.email ?? null,
      role: data?.user?.role ?? null,
      refresh: () => void mutate(),
      signOut,
    };
  }, [data, isLoading, mutate, signOut]);

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

/**
 * The current session.
 *
 * Defaults to `loading` rather than `anonymous` when unprovided: a component
 * rendered in isolation or in a test has not established that nobody is signed
 * in, and saying so would make it render a sign-in prompt it has no business
 * rendering.
 */
export function useSession(): Session {
  return (
    React.useContext(SessionContext) ?? {
      status: 'loading',
      email: null,
      role: null,
      refresh: () => {},
      signOut: async () => {},
    }
  );
}
