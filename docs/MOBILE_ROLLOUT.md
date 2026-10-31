# Mobile rollout — skeleton

Status: draft skeleton. Fill in owners, dates and thresholds before Phase 1.

## 0. Decisions to make first

| Decision | Options | Default proposal |
|---|---|---|
| Delivery vehicle | PWA · Capacitor wrapper · Expo/React Native rewrite | **PWA first**, Capacitor only if store presence is needed |
| Reachability | Stay on `https://localhost` · private tunnel (Tailscale) · public tunnel (Cloudflare Tunnel) | Tailscale for private beta (free, no open ports) |
| Audience | Operator only · invited users · public signup | Operator → invited beta → public |
| Mobile scope | Full dashboard · read-only subset + alerts | Read-only subset + push alerts |

Why PWA first: the frontend is Next.js 16 with a cookie-session BFF (`frontend/src/lib/session.ts`, `app/api/proxy`).
A PWA is served from the same origin, so auth, the proxy and every page work unchanged. A Capacitor shell loads the
app in a WebView from a different origin (`capacitor://localhost` / `https://localhost`), which breaks same-site cookie
assumptions and needs either a remote `server.url` or token auth. Expo means rebuilding the UI and sharing only logic.

## 1. Mobile scope (v1)

In:
- Alerts / intelligence feed (read)
- Auto-trader book B1: positions, P&L, kill-switch **status** (read-only)
- Factor-model picks and idea cards (read)
- Push notifications for graded, high-tier alerts

Out for v1:
- Any order entry or change to sizing, caps or the kill switch from mobile (needs explicit approval per CLAUDE.md)
- Global map, heavy charts, operations console, feature-flag manager
- Offline write paths

## 2. Phases

### Phase 0 — Prerequisites (web hardening)
- [ ] Responsive audit of the in-scope routes at 375px (no horizontal scroll, 44px tap targets, `viewport` export in `app/layout.tsx`)
- [ ] Lighthouse mobile pass on in-scope routes (performance, accessibility, best practices)
- [ ] Reachability decided and set up; HTTPS with a real certificate (service workers and push need a secure origin that the phone trusts)
- [ ] Session cookie review for mobile: lifetime, `Secure`/`SameSite`, re-auth on expiry, logout from all devices
- [ ] Rate limits on the gateway sized for mobile polling

### Phase 1 — Installable PWA
- [x] `app/manifest.ts` (name, icons 192/512 + maskable, `display: standalone`; dark theme colour, the default)
- [x] Apple touch icon and `appleWebApp` metadata (`app/layout.tsx`)
- [x] Service worker `public/sw.js`: caches only `/_next/static/*`, icons and the offline page; pages network-first and never stored; `/api/*` not intercepted
- [x] Offline fallback `public/offline.html` (static, so it precaches while signed out); reloads itself when back online
- [ ] Install prompt / "Add to Home Screen" instructions (iOS has no install prompt)
- [x] Tests: `src/__tests__/pwa.test.ts` runs the real `sw.js` against a fake worker global
- [ ] Verify on a production build and on a real phone (needs Phase 0 reachability + trusted HTTPS)

### Phase 2 — Push notifications
- [ ] Web Push (VAPID) subscription endpoint behind the BFF; subscriptions stored per user in Postgres (new numbered step in `shared/db/migrate.py`)
- [ ] Notifier service consumes the existing graded-alert stream; it is a new **consumer**, not a new producer, so tier and grading gates stay upstream
- [ ] Dedupe + per-user cap (e.g. N/hour) + quiet hours; failed sends counted with `dropped(...)`, expired subscriptions pruned on HTTP 404/410
- [ ] Persist before publish: a push references an alert row that is already committed
- [ ] User-facing notification settings (categories, quiet hours)
- [ ] iOS: push works only after Add to Home Screen (iOS 16.4+); document this in onboarding

### Phase 3 — Private beta
- [ ] Operator-only for 1–2 weeks, then a small invited group
- [ ] Feature flag `mobile_v1` gating mobile-only UI (existing `FeatureFlagManager`)
- [ ] Error reporting from the client (window errors + SW errors) into the existing telemetry
- [ ] Feedback channel in-app

### Phase 4 — General availability (web)
- [ ] Ramp the flag 10% → 50% → 100% of users, holding each step ≥ 48h
- [ ] Release notes / help page for install and notifications

### Phase 5 — Optional: store apps (only if Phase 4 shows demand)
- [ ] Capacitor shell pointed at the hosted origin, plus native APNs/FCM push bridged to the web layer
  (web push inside an iOS WebView is unreliable)
- [ ] Must add native value beyond the website (push, biometric unlock, widgets/share sheet) to avoid App Store
  guideline 4.2 "minimum functionality" rejection
- [ ] Finance-category review prep: paper-trading disclaimer, no investment advice, privacy nutrition labels, account deletion in-app
- [ ] TestFlight / Play internal testing → closed testing → production
- [ ] Staged rollout: Play 1% → 5% → 20% → 50% → 100%; App Store phased release (7-day automatic, pausable)
- [ ] Release CI (Fastlane or EAS), signing keys stored outside the repo

## 3. Release gates and metrics

| Metric | Gate to widen rollout | Halt / roll back |
|---|---|---|
| Crash-free sessions (store apps) / JS error-free sessions (PWA) | ≥ 99.5% and not worse than previous version | Drops below baseline − 0.5 pt |
| Push delivery success | ≥ 95% of sends accepted | < 90% or spike in 410s |
| p75 LCP on in-scope routes (mobile) | ≤ 2.5s | > 4s |
| Auth failures / forced re-logins | Flat versus web | 2× web baseline |
| Notifications per user per day | Within cap | Complaints or opt-out spike |

Set the real thresholds from a two-week web baseline before Phase 3.

## 4. Rollback

- PWA: turn off the `mobile_v1` flag; ship a service worker that calls `skipWaiting()` and clears old caches
  (keep a "kill switch" SW version ready). A broken SW can pin users to stale code, so test the update path before beta.
- Push: notifier service has a config switch to stop sending; subscriptions are kept.
- Store apps: halt the staged/phased release; server-side flags disable features without a new binary.

## 5. Security and compliance checklist

- [ ] No secrets in the client bundle or the native shell
- [ ] Notification payloads contain no position sizes or P&L on the lock screen (opt-in only)
- [ ] Session revocation works for lost devices
- [ ] Privacy policy covers push tokens and device data
- [ ] Clear "paper trading, not investment advice" disclosure on every trading view

## 6. Open questions

- Is public signup in scope for mobile, or invited users only?
- Which alert categories are push-worthy by default?
- Is App Store presence a goal, or is an installable PWA enough?

## References

- [PWA vs Capacitor vs Expo (stackshare)](https://stackshare.io/stackups/capacitor-vs-pwa), [React Native vs Expo vs Capacitor 2026 (pkgpulse)](https://www.pkgpulse.com/guides/react-native-vs-expo-vs-capacitor-cross-platform-mobile-2026)
- [iOS PWA limitations (MagicBell)](https://www.magicbell.com/blog/pwa-ios-limitations-safari-support-complete-guide), [iOS web push requirements (Pushpad)](https://pushpad.xyz/blog/ios-special-requirements-for-web-push-notifications)
- [App Store 4.2 minimum functionality](https://vp0.com/blogs/fix-app-store-rejection-4-2-minimum-functionality)
- [Monitoring mobile release health (Amplitude)](https://amplitude.com/blog/monitor-mobile-release-health), [Phased rollouts (AppsOnAir)](https://www.appsonair.com/blogs/how-phased-rollouts-prevent-the-next-bad-release-from-reaching-all-users), [Progressive rollout at Trendyol](https://medium.com/trendyol-tech/progressive-rollout-strategy-on-trendyol-android-app-3828d28ee27a)
