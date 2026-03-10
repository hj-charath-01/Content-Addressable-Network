package com.can.routing;

import com.can.core.NodeInfo;
import com.can.core.Point;
import com.can.core.Zone;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


// Stores the set of neighbors for a single CAN node.

public class RoutingTable {

    private final int dims;

    // using ConcurrentHashMap so the heartbeat thread can iterate without locking
    private final Map<String, NodeInfo> neighbors = new ConcurrentHashMap<>();

    public RoutingTable(int dims) {
        this.dims = dims;
    }

    public void addNeighbor(NodeInfo info) {
        if (info != null)
            neighbors.put(info.getNodeId(), info);
    }

    public void removeNeighbor(String nodeId) {
        neighbors.remove(nodeId);
    }

    public boolean isEmpty() {
        return neighbors.isEmpty();
    }
   
    public List<NodeInfo> getAllNeighbors() {
        return new ArrayList<>(neighbors.values());
    }

    
    // Greedy next-hop: pick the neighbor whose zone is closest to the destination.
    
    public NodeInfo nextHop(Point destination, Zone myZone) {
        NodeInfo best = null;
        double bestDist = myZone.minDistanceTo(destination);

        for (NodeInfo n : neighbors.values()) {
            double d = n.getZone().minDistanceTo(destination);
            if (d < bestDist) {
                bestDist = d;
                best = n;
            }
        }

        return best;
    }

    // returns only neighbors that geometrically share a face
    public List<NodeInfo> geometricNeighbors(Zone myZone) {
        return neighbors.values().stream()
                .filter(n -> myZone.isAdjacentTo(n.getZone()))
                .collect(Collectors.toList());
    }


    // Pick the adjacent neighbor with the smallest zone (merging is easiest with small zones).
     
    public Optional<NodeInfo> bestTakeoverCandidate(Zone myZone) {
        return neighbors.values().stream() .filter(n -> myZone.isAdjacentTo(n.getZone())) .min(Comparator.comparingDouble(n -> n.getZone().volume()));
    }

    public int size() { return neighbors.size(); }

    @Override
    public String toString() {
        if (neighbors.isEmpty()) return "RoutingTable[]";
        return "RoutingTable[\n" + neighbors.values().stream() .map(n -> "  " + n.getNodeId() + " -> " + n.getZone())  .collect(Collectors.joining("\n")) + "\n]";
    }
}