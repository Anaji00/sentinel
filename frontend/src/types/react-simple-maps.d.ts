declare module 'react-simple-maps' {
  import * as React from 'react';

  export const ComposableMap: React.ComponentType<any>;
  export const Geographies: React.ComponentType<any>;
  export const Geography: React.ComponentType<any>;
  export const Marker: React.ComponentType<any>;
  export const Line: React.ComponentType<any>;
  export const ZoomableGroup: React.ComponentType<any>;

  /** The projection of the enclosing ComposableMap: [lon, lat] to SVG [x, y]. */
  export interface MapContextValue {
    width: number;
    height: number;
    projection: ((coordinates: [number, number]) => [number, number] | null) & {
      invert?: (point: [number, number]) => [number, number] | null;
    };
  }
  export function useMapContext(): MapContextValue;

  /** The current pan and zoom of the enclosing ZoomableGroup. */
  export interface ZoomPanContextValue {
    x: number;
    y: number;
    k: number;
    transformString: string;
  }
  export function useZoomPanContext(): ZoomPanContextValue;
  export function ZoomPanProvider(props: {
    value: ZoomPanContextValue;
    children?: import('react').ReactNode;
  }): import('react').ReactElement;
}
