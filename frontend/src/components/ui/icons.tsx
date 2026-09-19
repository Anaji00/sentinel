'use client';

/**
 * The icon vocabulary, in one place.
 *
 * 116 emoji glyphs were doing this job across 19 components. Emoji are the
 * wrong tool for an instrument panel for four reasons that all showed up here:
 * they render as a different picture on every platform, they carry colour the
 * design system does not control, they cannot inherit `currentColor` so they
 * ignore severity, and they have no accessible name beyond whatever the font
 * vendor chose — a screen reader announced the anchor in a vessel row as
 * "anchor", which is a boat part, not "sanctioned vessel".
 *
 * Everything here is inline SVG that inherits colour and size from its parent,
 * so a caution icon is caution-coloured because it sits in caution-coloured
 * text, not because the glyph happens to be yellow.
 *
 * Most come from lucide, which is already a dependency. The three at the bottom
 * are drawn here because the concepts are specific to this platform and the
 * nearest stock icon would be a lie: a "dark vessel" is not a boat, it is a
 * boat that stopped transmitting.
 */

import React from 'react';
import {
  Activity,
  AlertTriangle,
  Anchor,
  Ban,
  Brain,
  Check,
  ChevronDown,
  Database,
  FileText,
  Fuel,
  Gauge,
  FlaskConical,
  Hourglass,
  Lock,
  Network,
  Plane,
  Radar,
  Ruler,
  Ship,
  Shield,
  ShieldAlert,
  Siren,
  Target,
  TrendingDown,
  TrendingUp,
  Waves,
  X,
  Zap,
} from 'lucide-react';

export interface IconProps {
  className?: string;
  /** Pixels. Defaults to 1em so an icon matches the text it sits beside. */
  size?: number;
  /** Announced name. Omit for icons that only repeat adjacent text. */
  label?: string;
}

/** Shared geometry, so every icon in the app has the same weight. */
const STROKE = 1.75;

function base({ className, size, label }: IconProps) {
  return {
    className,
    width: size ?? '1em',
    height: size ?? '1em',
    strokeWidth: STROKE,
    'aria-hidden': label ? undefined : (true as const),
    'aria-label': label,
    role: label ? ('img' as const) : undefined,
  };
}

// ── the general vocabulary ──────────────────────────────────────────────────

export const IconAlert = (p: IconProps) => <AlertTriangle {...base(p)} />;
export const IconBlocked = (p: IconProps) => <Ban {...base(p)} />;
export const IconCheck = (p: IconProps) => <Check {...base(p)} />;
export const IconChevronDown = (p: IconProps) => <ChevronDown {...base(p)} />;
export const IconClose = (p: IconProps) => <X {...base(p)} />;
export const IconDocument = (p: IconProps) => <FileText {...base(p)} />;
export const IconEnergy = (p: IconProps) => <Fuel {...base(p)} />;
export const IconFlow = (p: IconProps) => <Activity {...base(p)} />;
export const IconGraph = (p: IconProps) => <Network {...base(p)} />;
export const IconLock = (p: IconProps) => <Lock {...base(p)} />;
export const IconMethodology = (p: IconProps) => <Ruler {...base(p)} />;
export const IconModel = (p: IconProps) => <Brain {...base(p)} />;
export const IconRadar = (p: IconProps) => <Radar {...base(p)} />;
export const IconSimulated = (p: IconProps) => <FlaskConical {...base(p)} />;
export const IconPending = (p: IconProps) => <Hourglass {...base(p)} />;
export const IconRisk = (p: IconProps) => <Gauge {...base(p)} />;
export const IconSanctioned = (p: IconProps) => <ShieldAlert {...base(p)} />;
export const IconShield = (p: IconProps) => <Shield {...base(p)} />;
export const IconSignal = (p: IconProps) => <Zap {...base(p)} />;
export const IconStore = (p: IconProps) => <Database {...base(p)} />;
export const IconTarget = (p: IconProps) => <Target {...base(p)} />;
export const IconUrgent = (p: IconProps) => <Siren {...base(p)} />;
export const IconUp = (p: IconProps) => <TrendingUp {...base(p)} />;
export const IconDown = (p: IconProps) => <TrendingDown {...base(p)} />;
export const IconVessel = (p: IconProps) => <Ship {...base(p)} />;
export const IconAircraft = (p: IconProps) => <Plane {...base(p)} />;
export const IconAnchorage = (p: IconProps) => <Anchor {...base(p)} />;
export const IconSea = (p: IconProps) => <Waves {...base(p)} />;

/** Up, down or flat — the arrow that matches `directionColor`. */
export function IconDirection({ value, ...p }: IconProps & { value: number | null | undefined }) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value === 0) {
    return <IconFlow {...p} />;
  }
  return value > 0 ? <IconUp {...p} /> : <IconDown {...p} />;
}

// ── drawn here, because the concept is this platform's ──────────────────────

/**
 * A vessel that has stopped transmitting.
 *
 * Not a ship icon. The finding is the silence, so the mark is a hull with the
 * signal struck through — which is what `vessel_dark` actually means and what
 * an anchor emoji never said.
 */
export const IconDarkVessel = (p: IconProps) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    {...base(p)}
  >
    <path d="M3 17h18l-2 4H5l-2-4Z" />
    <path d="M6 17V9h12v8" />
    <line x1="4" y1="4" x2="20" y2="20" strokeWidth={2.25} />
  </svg>
);

/**
 * Two hulls alongside: a ship-to-ship transfer.
 *
 * The geometry is the claim — two shapes touching, both stationary — so the
 * icon draws exactly that rather than borrowing a generic transfer arrow.
 */
export const IconShipToShip = (p: IconProps) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    {...base(p)}
  >
    <path d="M2 15h9l-1.5 4H3.5L2 15Z" />
    <path d="M13 15h9l-1.5 4h-6L13 15Z" />
    <path d="M4.5 15V9h4v6" />
    <path d="M15.5 15V9h4v6" />
    <line x1="11" y1="12" x2="13" y2="12" strokeDasharray="1 1.5" />
  </svg>
);

/**
 * A strait or canal: a passage narrowing between two masses.
 *
 * Chokepoint risk is about the narrowing, not about water, so the mark is the
 * pinch rather than a wave.
 */
export const IconChokepoint = (p: IconProps) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    {...base(p)}
  >
    <path d="M3 3c3.5 4 3.5 5.5 6 9s2.5 5 6 9" />
    <path d="M21 3c-3.5 4-3.5 5.5-6 9s-2.5 5-6 9" />
    <circle cx="12" cy="12" r="1.25" fill="currentColor" stroke="none" />
  </svg>
);
