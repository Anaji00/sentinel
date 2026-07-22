import React from 'react';
import { Header } from '../components/ui/Header';
import DashboardClient from '../components/DashboardClient';

export default function Dashboard() {
  return (
    <div className="flex flex-col h-screen w-full bg-[#08090c] text-[#f1f5f9] overflow-hidden select-none">
      {/* Server-Rendered System Header */}
      <Header />

      {/* Hydrated Client Grid */}
      <DashboardClient />
    </div>
  );
}