'use client';

import React from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { 
  LayoutDashboard, 
  ShieldAlert, 
  ShieldCheck,
  Globe2, 
  TrendingUp, 
  Layers,
  Bitcoin, 
  Map,
  LineChart,
  Bot
} from 'lucide-react';

const NAV_ITEMS = [
  { href: '/', label: 'Command Center', icon: LayoutDashboard },
  { href: '/intelligence', label: 'Intelligence Feed', icon: ShieldAlert },
  { href: '/osint', label: 'OSINT Threat Matrix', icon: ShieldCheck },
  { href: '/flow', label: 'Dark Pool & Sweeps', icon: Layers },
  { href: '/agents', label: 'Agent Swarm HUD', icon: Bot },
  { href: '/macro', label: 'Macro Matrix', icon: Globe2 },
  { href: '/options', label: 'Options Flow', icon: TrendingUp },
  { href: '/crypto', label: 'Crypto Analytics', icon: Bitcoin },
  { href: '/charts', label: 'Market Charts', icon: LineChart },
  { href: '/map', label: 'Global Map', icon: Map },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-16 hover:w-64 transition-all duration-300 bg-[#0f1115] border-r border-cyan-500/20 flex flex-col h-full group z-50 select-none shadow-2xl">
      <div className="flex-1 py-4 flex flex-col gap-2 font-mono">
        {NAV_ITEMS.map((item) => {
          const isActive = pathname === item.href;
          const Icon = item.icon;
          
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex items-center px-4 py-3 mx-2 rounded-lg transition-all duration-200 overflow-hidden ${
                isActive 
                  ? 'bg-cyan-500/15 text-[#00f2fe] border border-[#00f2fe]/40 glow-cyan font-bold' 
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60 border border-transparent'
              }`}
              title={item.label}
            >
              <Icon className={`w-5 h-5 min-w-[20px] ${isActive ? 'text-[#00f2fe]' : 'text-slate-400'}`} />
              <span className="ml-4 text-xs tracking-wider uppercase whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                {item.label}
              </span>
            </Link>
          );
        })}
      </div>
    </aside>
  );
}
