/**
 * Re-mounted on every navigation inside the dashboard, so each page eases in
 * instead of swapping in a single frame. The fade is 180ms and is switched off
 * under reduced motion by the global rule.
 */
export default function Template({ children }: { children: React.ReactNode }) {
  return <div className="fade-in flex h-full min-h-0 flex-col">{children}</div>;
}
