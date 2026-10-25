declare module 'react-simple-maps' {
  import * as React from 'react';

  /** One country shape as the library hands it to `Geographies`' render prop. */
  export interface GeographyFeature {
    rsmKey: string;
    type: string;
    properties: Record<string, unknown>;
    geometry: unknown;
  }

  export interface ComposableMapProps {
    projection?: string;
    projectionConfig?: { scale?: number; center?: [number, number]; rotate?: [number, number, number] };
    width?: number;
    height?: number;
    className?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
  }

  export interface GeographiesProps {
    geography: string | object;
    children: (args: { geographies: GeographyFeature[] }) => React.ReactNode;
  }

  type GeographyStyle = Record<'default' | 'hover' | 'pressed', React.CSSProperties>;

  export interface GeographyProps extends Omit<React.SVGProps<SVGPathElement>, 'style'> {
    geography: GeographyFeature;
    style?: Partial<GeographyStyle>;
  }

  export interface MarkerProps extends Omit<React.SVGProps<SVGGElement>, 'style'> {
    coordinates: [number, number];
    style?: Partial<GeographyStyle>;
    children?: React.ReactNode;
  }

  export interface LineProps extends React.SVGProps<SVGPathElement> {
    from: [number, number];
    to: [number, number];
  }

  export interface ZoomableGroupProps {
    center?: [number, number];
    zoom?: number;
    minZoom?: number;
    maxZoom?: number;
    children?: React.ReactNode;
  }

  export const ComposableMap: React.ComponentType<ComposableMapProps>;
  export const Geographies: React.ComponentType<GeographiesProps>;
  export const Geography: React.ComponentType<GeographyProps>;
  export const Marker: React.ComponentType<MarkerProps>;
  export const Line: React.ComponentType<LineProps>;
  export const ZoomableGroup: React.ComponentType<ZoomableGroupProps>;

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
