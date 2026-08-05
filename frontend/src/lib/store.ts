import { create } from 'zustand';

interface TelemetryState {
  isConnected: boolean;
  lastUpdate: number | null;
  setConnected: (status: boolean) => void;
  updateTelemetry: () => void;
}

export const useTelemetryStore = create<TelemetryState>((set) => ({
  isConnected: false,
  lastUpdate: null,
  setConnected: (status) => set({ isConnected: status }),
  updateTelemetry: () => set({ lastUpdate: Date.now() }),
}));
