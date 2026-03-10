package com.can.core;

import com.can.util.HashUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


public class CANNetwork {

    private static final Logger log = Logger.getLogger(CANNetwork.class.getName());

    private final int dims;
    private final Map<String, CANNode> overlay = new ConcurrentHashMap<>();
    private final AtomicInteger seq = new AtomicInteger(0);

    public CANNetwork(int dims) {
        this.dims = dims;
    }

    /*
      Create the first node. It starts owning the entire keyspace.
      Must be called before addNode().
     */
    public CANNode bootstrap(String nodeId) {
        CANNode n = new CANNode(nodeId, "localhost", nextPort(), dims, Zone.fullSpace(dims), overlay);
        log.info("bootstrapped: " + nodeId);
        return n;
    }

    /*
      Add a node with an auto-generated ID.
      While running program, need to specify which existing node to use as the bootstrap contact.
     */
    public CANNode addNode(String bootstrapId) {
        String id = "n" + seq.incrementAndGet();
        return addNode(id, bootstrapId);
    }

    public CANNode addNode(String nodeId, String bootstrapId) {
        if (overlay.isEmpty())
            throw new IllegalStateException("call bootstrap() first");

        CANNode n = new CANNode(nodeId, "localhost", nextPort(), dims, Zone.fullSpace(dims), overlay);
        if (!n.join(bootstrapId)) {
            overlay.remove(nodeId);
            throw new RuntimeException("join failed for " + nodeId);
        }
        return n;
    }

    // network-level ops 

    // store via a random node (any entry point is fine -- routing handles the rest)
    public boolean store(String key, String value) {
        CANNode n = anyNode();
        if (n == null) throw new IllegalStateException("empty network");
        return n.store(key, value);
    }

    public Optional<String> lookup(String key) {
        CANNode n = anyNode();
        if (n == null) return Optional.empty();
        return n.lookupString(key);
    }

    // useful for debugging -- shows which node "should" own a given key
    public Optional<CANNode> responsibleNode(String key) {
        Point p = HashUtil.hashKey(key, dims);
        return overlay.values().stream() .filter(n -> n.getZone().contains(p)) .findFirst();
    }

    // accessors 

    public CANNode       getNode(String id) { return overlay.get(id); }
    public int           size()             { return overlay.size(); }
    public int           getDims()          { return dims; }
    public List<CANNode> getAllNodes()       { return new ArrayList<>(overlay.values()); }

    private CANNode anyNode() {
        return overlay.values().stream().findAny().orElse(null);
    }

    private int nextPort() {
        return 9000 + seq.incrementAndGet();
    }

    // diagnostics 

    public void printSummary() {
        System.out.println("\n--- network summary (" + dims + "D, " + overlay.size() + " nodes) ---");
        double totalVol = 0;
        List<CANNode> sorted = new ArrayList<>(overlay.values());
        sorted.sort(Comparator.comparing(CANNode::getNodeId));
        for (CANNode n : sorted) {
            totalVol += n.getZone().volume();
            System.out.printf("  %-12s  %s  nbrs=%d  data=%d%n", n.getNodeId(), n.getZone(), n.getRoutingTable().size(), n.getDataStore().size());
        }
        System.out.printf("  total coverage: %.6f%n", totalVol); // should be ~1.0
        System.out.println();
    }

    
    //Returns true if >= 99% of points have exactly one owner.
    public boolean verifyPartitioning() {
        int N = 2000;
        int ok = 0;
        Random rng = new Random(42);
        for (int i = 0; i < N; i++) {
            double[] c = new double[dims];
            for (int d = 0; d < dims; d++) c[d] = rng.nextDouble();
            Point p = new Point(c);
            long owners = overlay.values().stream().filter(n -> n.getZone().contains(p)).count();
            if (owners == 1) ok++;
        }
        double pct = 100.0 * ok / N;
        System.out.printf("  partitioning: %d/%d ok (%.1f%%)%n", ok, N, pct);
        return pct >= 99.0;
    }
}