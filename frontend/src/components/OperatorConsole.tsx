'use client';

import React, { Activity, useState } from 'react';
import { CommandCenterGrid } from './CommandCenterGrid';

/**
 * The plumbing, folded: for anyone checking every feed is flowing.
 *
 * A closed `<details>` hides its children but React still mounts them, so the
 * home page ran the whole console -- the intelligence feed and its live socket,
 * and the radar, movers and portfolio panels polling -- for a person who never
 * opened it. Nothing mounts until the first open. After that, closing hides it
 * behind `<Activity>`: effects stop (sockets released, polling paused) and the
 * panel toggles are still as they were left when it is reopened.
 */
export function OperatorConsole() {
  const [open, setOpen] = useState(false);
  const [opened, setOpened] = useState(false);

  const onToggle = (e: React.SyntheticEvent<HTMLDetailsElement>) => {
    const isOpen = e.currentTarget.open;
    setOpen(isOpen);
    if (isOpen) setOpened(true);
  };

  return (
    <details className="group rounded-2xl border border-line bg-raised" onToggle={onToggle}>
      <summary className="flex cursor-pointer items-baseline justify-between gap-2 px-4 py-3">
        <span id="console-heading" className="text-sm font-semibold text-ink">
          Operator console{' '}
          <span className="text-ink-mute font-normal">· every feed as it arrives</span>
        </span>
        <span className="text-micro text-accent group-open:hidden">Open</span>
        <span className="text-micro text-accent hidden group-open:inline">Close</span>
      </summary>
      <div className="h-[85vh] min-h-[640px] border-t border-line overflow-hidden">
        {opened && (
          <Activity mode={open ? 'visible' : 'hidden'}>
            <CommandCenterGrid />
          </Activity>
        )}
      </div>
    </details>
  );
}
