'use client';
import { useState, useEffect } from 'react';
// The 'react-flow-renderer' library is deprecated. For new projects, it's recommended to use '@xyflow/react'.
// However, we will stick to the existing dependency for this review.
import ReactFlow, { Background, Controls, Node, Edge } from 'react-flow-renderer';
import { apiClient } from '../lib/api';

/**
 * Defines the structure of a single connection coming from the API.
 * Using a specific type is better than `any` for type safety and code clarity.
 */
interface ApiConnection {
    target_name: string;
    relationship: string;
}

/**
 * GraphExplorer Component
 * 
 * Visualizes a network graph for a given entity. It fetches the entity's
 * first-degree connections from the API and renders them as an interactive
 * node-edge diagram using React Flow.
 * 
 * @param {object} props - The component props.
 * @param {string} props.entityId - The ID of the central entity to graph. Defaults to "Oligarch_A".
 */
export default function GraphExplorer({ entityId = "Oligarch_A" }) {
    // --- State Management ---
    // useState hooks hold data that, when changed, causes the component to re-render.
    // We store the 'nodes' (the circles/boxes) and 'edges' (the connecting lines) here.
    const [nodes, setNodes] = useState<Node[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);

    // These states manage our UI feedback loop (loading spinners, error messages).
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // --- Data Fetching & Processing ---
    // useEffect runs side effects (like API calls) outside the normal render cycle.
    // The dependency array `[entityId]` at the bottom tells React to re-run this function ONLY if entityId changes.
    useEffect(() => {
        const fetchGraphData = async () => {
            setIsLoading(true);
            setError(null);
            try {
                // Fetch 1st-degree connections from the backend API.
                const response = await apiClient.get(`/graph/entity/${entityId}`);
                const connections: ApiConnection[] = response.data.connections;
                
                // --- Map API data to React Flow format ---

                // 1. Initialize the central node.
                // This represents the `entityId` we are querying. We place it at base coordinates (250, 250)
                // and give it a distinct red background to stand out from the connections.
                const centralNode: Node = { 
                    id: entityId, 
                    data: { label: entityId }, 
                    position: { x: 250, y: 250 }, // Center of the canvas
                    style: { background: '#ef4444', color: 'white', border: 'none' } 
                };
                const newNodes: Node[] = [centralNode];
                const newEdges: Edge[] = [];

                // 2. Create nodes and edges for each connection.
                connections.forEach((conn, index) => {
                    const targetId = conn.target_name;
                    
                    // Trigonometry to calculate positions in a perfect circle (radial layout):
                    // - `2 * Math.PI` represents a full circle in radians (360 degrees).
                    // - We divide the circle by the total number of connections to get the angle for this specific node.
                    const angle = (index / connections.length) * 2 * Math.PI;
                    const radius = 250; // The distance from the central node (px)
                    
                    // Math.cos(angle) gives the X coordinate, Math.sin(angle) gives the Y coordinate on a unit circle.
                    // We multiply by our radius and add 250 to shift the center from (0,0) to the central node's position (250,250).
                    newNodes.push({
                        id: targetId,
                        data: { label: targetId },
                        position: { x: 250 + Math.cos(angle) * radius, y: 250 + Math.sin(angle) * radius },
                    });

                    // Create an edge (line) linking the central node to this connected node.
                    // React Flow strictly requires edges to have a unique ID, a source ID, and a target ID.
                    newEdges.push({
                        id: `e-${entityId}-${targetId}`,
                        source: entityId,
                        target: targetId,
                        label: conn.relationship, // Text displayed along the line
                        animated: true, // Adds a moving dashed effect to visually represent data flow or active ties
                    });
                });

                setNodes(newNodes);
                setEdges(newEdges);
            } catch (err) {
                console.error("Failed to fetch graph data:", err);
                setError("Could not load entity connections. The network may be down.");
            } finally {
                setIsLoading(false);
            }
        };

        fetchGraphData();
    }, [entityId]);

    // Render loading or error states before attempting to render the graph.
    if (isLoading) return <div className="flex items-center justify-center h-full w-full bg-gray-950 text-gray-400">Loading graph...</div>;
    if (error) return <div className="flex items-center justify-center h-full w-full bg-gray-950 text-red-500 p-4">{error}</div>;

    return (
        <div className="h-full w-full bg-gray-950">
            {/* The main React Flow component that renders the graph.
                - `fitView`: Automatically zooms and pans to fit all nodes in the viewport. */}
            <ReactFlow nodes={nodes} edges={edges} fitView>
                {/* Background adds a dotted/lined grid pattern to the canvas for a blueprint look. */}
                <Background color="#333" gap={16} />
                {/* Controls add the interactive zoom-in, zoom-out, and reset view buttons to the bottom corner. */}
                <Controls />
            </ReactFlow>
        </div>
    );
}