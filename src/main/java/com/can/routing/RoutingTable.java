package com.can.routing;

import com.can.core.NodeInfo;
import com.can.core.Point;
import com.can.core.Zone;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Stores the set of neighbors for a single CAN node.
 *
 * In theory: 2d neighbors (one on each face of the zone).
 * In practice: can be more or fewer during joins/leaves, doesn't matter much,
 * we just keep whoever we know about.
 *
 * Routing is purely greedy -- forward to whoever gets us closest to the target.
 * The paper proves this works in O(d * n^(1/d)) hops on average.
 * Could do something smarter but greedy is simple and it works.
 */
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

    public List<NodeInfo> getAll() {
        return new ArrayList<>(neighbors.values());
    }

    // getAllNeighbors() alias -- kept both because i kept forgetting which name i used
    public List<NodeInfo> getAllNeighbors() {
        return getAll();
    }

    /**
     * Greedy next-hop: pick the neighbor whose zone is closest to the destination.
     * Returns null if we're already the closest (caller should handle locally).
     *
     * "Closest" = minimum distance from zone boundary to target point.
     * If target is inside a zone, minDistance is 0 -- that node wins immediately.
     */
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

    // returns only neighbors that geometrically share a face with myZone
    // (as opposed to ones we just know about for other reasons)
    public List<NodeInfo> geometricNeighbors(Zone myZone) {
        return neighbors.values().stream()
                .filter(n -> myZone.isAdjacentTo(n.getZone()))
                .collect(Collectors.toList());
    }

    /**
     * When we leave, who should take over our zone?
     * Pick the adjacent neighbor with the smallest zone (merging is easiest with small zones).
     *
     * FIXME: doesn't work well if multiple neighbors have identical zone sizes.
     * Could use load as a tiebreaker but haven't implemented that yet.
     */
    public Optional<NodeInfo> bestTakeoverCandidate(Zone myZone) {
        return neighbors.values().stream()
                .filter(n -> myZone.isAdjacentTo(n.getZone()))
                .min(Comparator.comparingDouble(n -> n.getZone().volume()));
    }

    public int size() { return neighbors.size(); }

    @Override
    public String toString() {
        if (neighbors.isEmpty()) return "RoutingTable[]";
        return "RoutingTable[\n" + neighbors.values().stream()
                .map(n -> "  " + n.getNodeId() + " -> " + n.getZone())
                .collect(Collectors.joining("\n")) + "\n]";
    }
}