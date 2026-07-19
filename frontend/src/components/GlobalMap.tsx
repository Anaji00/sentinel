'use client';

import React, { useEffect, useState } from "react";
import { ComposableMap, Geographies, Geography, Marker, Line } from "react-simple-maps";
import useSWR from "swr";
import { fetcher } from "../lib/api";
import { NormalizedEvent } from "../lib/types";

// Standard CDN for world atlas map topology
const geoUrl = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

interface GeoJSONFeature {
    type: string;
    properties: {
        name: string;
    };
    geometry: {
        type: "Polygon";
        coordinates: number[][][];
    };
}

interface GeoJSONData {
    type: string;
    features: GeoJSONFeature[];
}

export default function GlobalMap() {
    const [regions, setRegions] = useState<GeoJSONData | null>(null);

    // Fetch local chokepoints geofencing boundaries
    useEffect(() => {
        fetch("/regions.geojson")
            .then((res) => res.json())
            .then((data) => setRegions(data))
            .catch((err) => console.error("Failed to load regions.geojson:", err));
    }, []);

    // Fetch active Maritime and Cyber events for map plotting
    const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
        "/events/maritime?limit=30",
        fetcher,
        { refreshInterval: 10000 }
    );

    const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
        "/events/cyber?limit=20",
        fetcher,
        { refreshInterval: 10000 }
    );

    // Extract coordinate features
    const vessels = (maritimeEvents || [])
        .filter((e) => e.vessel_data && e.vessel_data.latitude && e.vessel_data.longitude)
        .map((e) => ({
            name: e.vessel_data!.name,
            lat: e.vessel_data!.latitude,
            lon: e.vessel_data!.longitude,
            anomaly: e.anomaly_score,
            speed: e.vessel_data!.speed,
        }));

    // Extract cyber BGP hijack lines
    // We mock coordinates for AS locations (approximate major nodes) if exact IP coordinates are absent
    const bgpLinks = (cyberEvents || [])
        .filter((e) => e.type === "bgp_anomaly" && e.security_data?.ip_address)
        .map((e, idx) => {
            // Generate deterministic coordinates for visualization based on ASN/IP prefix
            const hash = e.security_data?.ip_address?.split(".").reduce((acc, val) => acc + parseInt(val || "0"), 0) || idx;
            const startLon = -80 + (hash % 60); // Eastern US / Atlantic range
            const startLat = 20 + (hash % 30);
            const endLon = 30 + (hash % 80);   // Mid East / Asian range
            const endLat = 10 + (hash % 40);
            return {
                from: [startLon, startLat] as [number, number],
                to: [endLon, endLat] as [number, number],
                id: e.event_id,
                org: e.security_data?.affected_org || "Unknown AS",
                anomaly: e.anomaly_score
            };
        });

    return (
        <div className="w-full h-full bg-[#0a0c10] relative overflow-hidden font-mono text-white">
            {/* Visual HUD overlays */}
            <div className="absolute bottom-4 right-4 z-10 bg-black/60 px-3 py-2 rounded border border-[#1f2833] text-[9px] space-y-1.5 backdrop-blur-md">
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
                    <span>Active Telemetry ({vessels.length} vessels)</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-red-500 animate-pulse" />
                    <span>Cyber Routing Hijacks ({bgpLinks.length} paths)</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 bg-amber-500/20 border border-amber-500/50 rounded" />
                    <span>Geofence Risk Corridors</span>
                </div>
            </div>

            {/* Map Canvas */}
            <div className="w-full h-full flex items-center justify-center">
                <ComposableMap
                    projection="geoMercator"
                    projectionConfig={{
                        scale: 110,
                        center: [20, 15]
                    }}
                    width={800}
                    height={380}
                    style={{ width: "100%", height: "100%" }}
                >
                    {/* World Base Geography */}
                    <Geographies geography={geoUrl}>
                        {({ geographies }) =>
                            geographies.map((geo) => (
                                <Geography
                                    key={geo.rsmKey}
                                    geography={geo}
                                    fill="#12161f"
                                    stroke="#1f2833"
                                    strokeWidth={0.5}
                                    style={{
                                        default: { outline: "none" },
                                        hover: { fill: "#1c2331", outline: "none" },
                                        pressed: { outline: "none" }
                                    }}
                                />
                            ))
                        }
                    </Geographies>

                    {/* Geofence Risk Corridors (from regions.geojson) */}
                    {regions?.features.map((feature, idx) => {
                        // Project polygon coordinates to draw SVG shapes manually
                        const coordinates = feature.geometry.coordinates[0];
                        if (!coordinates) return null;

                        return (
                            <g key={`region-${idx}`}>
                                {/* Since react-simple-maps handles geographies via Projection, 
                                    we project individual polygon points to match Mercator space */}
                                <Geographies geography={feature}>
                                    {({ geographies }) =>
                                        geographies.map((geo) => (
                                            <Geography
                                                key={geo.rsmKey}
                                                geography={geo}
                                                fill="rgba(245, 158, 11, 0.08)"
                                                stroke="rgba(245, 158, 11, 0.35)"
                                                strokeWidth={1}
                                                style={{
                                                    default: { outline: "none" },
                                                    hover: { fill: "rgba(245, 158, 11, 0.15)", outline: "none" }
                                                }}
                                            />
                                        ))
                                    }
                                </Geographies>
                            </g>
                        );
                    })}

                    {/* Cyber BGP Routing Links */}
                    {bgpLinks.map((link) => (
                        <g key={link.id}>
                            <Line
                                from={link.from}
                                to={link.to}
                                stroke="rgba(239, 68, 68, 0.6)"
                                strokeWidth={1.5}
                                strokeDasharray="3 3"
                            />
                            <Marker coordinates={link.from}>
                                <circle r={3} fill="#ef4444" />
                            </Marker>
                            <Marker coordinates={link.to}>
                                <circle r={3} fill="#ef4444" className="animate-ping" />
                            </Marker>
                        </g>
                    ))}

                    {/* Maritime Vessel Markers */}
                    {vessels.map((vessel, idx) => {
                        const isHighAnomaly = vessel.anomaly >= 0.7;
                        return (
                            <Marker key={idx} coordinates={[vessel.lon, vessel.lat]}>
                                <g className="cursor-pointer">
                                    <circle
                                        r={isHighAnomaly ? 5 : 3.5}
                                        fill={isHighAnomaly ? "#ef4444" : "#10b981"}
                                        stroke="#0b0c10"
                                        strokeWidth={1}
                                    />
                                    {isHighAnomaly && (
                                        <circle
                                            r={10}
                                            fill="transparent"
                                            stroke="#ef4444"
                                            strokeWidth={0.8}
                                            className="animate-ping"
                                        />
                                    )}
                                    <title>
                                        {`Vessel: ${vessel.name}\nSpeed: ${vessel.speed} kn\nAnomaly: ${vessel.anomaly.toFixed(2)}`}
                                    </title>
                                </g>
                            </Marker>
                        );
                    })}
                </ComposableMap>
            </div>
        </div>
    );
}
