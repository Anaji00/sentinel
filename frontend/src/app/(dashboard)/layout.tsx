import React from 'react';
import { Header } from '../../components/ui/Header';
import { Sidebar } from '../../components/ui/Sidebar';
import { CommandPalette } from '../../components/ui/CommandPalette';
import { NavProvider } from '../../components/ui/NavContext';
import { FeedbackProvider } from '../../components/ui/Feedback';
import { TimeZoneProvider } from '../../components/ui/TimeZoneContext';
import { DataProvider } from '../../components/ui/DataProvider';
import { StockDrawer } from '../../components/StockDrawer';
import { DataHealthStrip } from '../../components/ui/DataHealthStrip';

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  return (
    <DataProvider>
      <TimeZoneProvider>
        <NavProvider>
          <FeedbackProvider>
            <div className="flex h-screen w-full bg-page text-ink overflow-hidden">
              <CommandPalette />
              {/* Any page's ?t=TICKER opens the stock drawer over it. useSearchParams
                  needs a Suspense boundary under the app router. */}
              <React.Suspense fallback={null}>
                <StockDrawer />
              </React.Suspense>
              <Sidebar />
              <div className="flex flex-col flex-1 min-w-0 h-full overflow-hidden">
                <Header />
                {/* Padding scales with the viewport. A flat p-4 spent 32 of the 39
              pixels this app used to be left with on a narrow screen. */}
                <main className="flex-1 overflow-y-auto p-2 sm:p-4 lg:p-6">
                  <div className="max-w-[1920px] mx-auto h-full flex flex-col">
                    <DataHealthStrip />
                    {children}
                  </div>
                </main>
              </div>
            </div>
          </FeedbackProvider>
        </NavProvider>
      </TimeZoneProvider>
    </DataProvider>
  );
}
