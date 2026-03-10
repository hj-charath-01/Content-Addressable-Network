package com.can.core;

import com.can.network.Message;
import com.can.routing.RoutingTable;
import com.can.storage.DataStore;
import com.can.util.HashUtil;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * A node in the CAN.
 *
 * Handles the four main operations from the paper:
 *   - JOIN: find a zone, split it, take half
 *   - STORE: hash key to point, route to responsible node, store locally
 *   - LOOKUP: hash key to point, route to responsible node, return value
 *   - LEAVE: hand off data and zone, notify neighbors
 *
 * The routing/storage/messaging logic is split into separate classes but
 * this is the main orchestrator. It's getting a bit long, might refactor later.
 *
 * Threading: zone updates are synchronized. Routing table uses ConcurrentHashMap
 * internally. The heartbeat runs on a separate daemon thread.
 *
 * For the simulation, nodes communicate via a shared in-memory overlay map
 * (nodeId -> CANNode). In a real system this would be network I/O.
 */
public class CANNode {

    private static final Logger log = Logger.getLogger(CANNode.class.getName());

    // routing gives up after this many hops and stores/returns locally
    // shouldn't be hit in a healthy network but good to have a safety valve
    private static final int MAX_HOPS = 50;

    private static final long HEARTBEAT_MS = 5000;
    private static final long EVICT_MS     = 30000;

    final  String nodeId; // package-private so CANNetwork can read it
    private final String host;
    private final int    port;
    private final int    dims;

    // zone is volatile so we can read it without synchronizing in logging/toString
    // writes always go through synchronized methods
    volatile Zone zone;

    private final RoutingTable  routingTable;
    private final DataStore     store;
    private final Map<String, CANNode> overlay; // shared ref, not owned by us

    // metrics for the evaluation section of the writeup
    private final AtomicLong msgSent  = new AtomicLong();
    private final AtomicLong msgRcvd  = new AtomicLong();
    private final AtomicLong hopTotal = new AtomicLong();
    private final AtomicLong routed   = new AtomicLong();

    private final ScheduledExecutorService bg;

    public CANNode(String nodeId, String host, int port, int dims,
                   Zone zone, Map<String, CANNode> overlay) {
        this.nodeId       = nodeId;
        this.host         = host;
        this.port         = port;
        this.dims         = dims;
        this.zone         = zone;
        this.overlay      = overlay;
        this.routingTable = new RoutingTable(dims);
        this.store        = new DataStore();

        // init here so the thread name can include nodeId
        this.bg = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "can-bg-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        overlay.put(nodeId, this);

        bg.scheduleAtFixedRate(this::heartbeat, HEARTBEAT_MS, HEARTBEAT_MS, TimeUnit.MILLISECONDS);
        bg.scheduleAtFixedRate(() -> store.evictExpired(), EVICT_MS, EVICT_MS, TimeUnit.MILLISECONDS);
    }

    // =========================================================================
    // JOIN
    // =========================================================================

    /**
     * Join the network via a known bootstrap node.
     *
     * Process (from paper section 3.1):
     *  1. Pick random point p in [0,1)^d
     *  2. Send JOIN to bootstrap, which routes it to whoever owns p
     *  3. That node splits its zone, gives us one half
     *  4. Update our zone and routing table from the reply
     */
    public boolean join(String bootstrapId) {
        CANNode boot = overlay.get(bootstrapId);
        if (boot == null) {
            log.warning(nodeId + ": bootstrap not found: " + bootstrapId);
            return false;
        }

        // random join point -- determines which zone we'll split
        // using dims=2 full space as a quick way to get a random point in [0,1)^d
        // (the zone itself doesn't matter, just need a random point)
        Point p = Zone.fullSpace(dims).randomPoint(new Random());

        Message req   = Message.joinRequest(nodeId, p);
        Message reply = boot.routeJoin(req);

        if (reply == null || !reply.isSuccess()) {
            log.warning(nodeId + ": join failed");
            return false;
        }

        synchronized (this) {
            this.zone = reply.getAssignedZone();
        }

        if (reply.getBootstrapNeighbors() != null)
            reply.getBootstrapNeighbors().forEach(routingTable::addNeighbor);

        log.info(nodeId + " joined -> " + zone);
        return true;
    }

    // routes the join request toward the target point, hop by hop
    Message routeJoin(Message req) {
        msgRcvd.incrementAndGet();
        req.incrementHop();

        if (zone.contains(req.getJoinPoint()))
            return doJoin(req);

        if (req.getHopCount() > MAX_HOPS) {
            log.warning(nodeId + ": join MAX_HOPS exceeded, handling here");
            return doJoin(req);
        }

        NodeInfo next = routingTable.nextHop(req.getJoinPoint(), zone);
        if (next == null) return doJoin(req); // we're closest, just do it

        CANNode nextNode = overlay.get(next.getNodeId());
        if (nextNode == null) {
            // neighbor is dead, remove and handle locally
            // TODO: should try other neighbors first instead of immediately giving up
            routingTable.removeNeighbor(next.getNodeId());
            return doJoin(req);
        }

        msgSent.incrementAndGet();
        return nextNode.routeJoin(req);
    }

    private synchronized Message doJoin(Message req) {
        String newId = req.getSenderId();

        Zone[] halves = zone.splitLongest();
        Zone keep   = halves[0];
        Zone assign = halves[1];

        // grab current neighbors before we update zone (need to notify them)
        List<NodeInfo> oldNeighbors = new ArrayList<>(routingTable.getAllNeighbors());

        // hand off entries that now belong to the new zone
        Map<String, DataStore.Entry> handoff = store.extractEntriesForTransfer(key -> {
            Point kp = HashUtil.hashKey(key, dims);
            return assign.contains(kp);
        });

        this.zone = keep;

        NodeInfo newNodeInfo = new NodeInfo(newId, "localhost", 0, assign);
        routingTable.addNeighbor(newNodeInfo);

        // tell our neighbors about the zone change and the new node
        NodeInfo myInfo = toNodeInfo();
        for (NodeInfo nb : oldNeighbors) {
            CANNode nbNode = overlay.get(nb.getNodeId());
            if (nbNode == null) continue;
            nbNode.recv(Message.zoneUpdate(nodeId, myInfo));
            nbNode.recv(Message.zoneUpdate(nodeId, newNodeInfo));
        }

        // send the reply with the new node's zone + our current neighbors
        oldNeighbors.add(myInfo);
        Message reply = Message.joinReply(nodeId, assign, oldNeighbors, true);

        // transfer entries to the new node if it's already in the overlay
        CANNode newNode = overlay.get(newId);
        if (newNode != null && !handoff.isEmpty()) {
            newNode.store.bulkLoad(handoff);
            log.info(nodeId + ": transferred " + handoff.size() + " entries to " + newId);
        }

        log.info(nodeId + ": split -> kept=" + keep + " assigned=" + assign + " to " + newId);
        return reply;
    }

    // =========================================================================
    // STORE
    // =========================================================================

    public boolean store(String key, String value) {
        return store(key, value.getBytes(StandardCharsets.UTF_8));
    }

    public boolean store(String key, byte[] value) {
        Point target = HashUtil.hashKey(key, dims);
        Message reply = routeStore(Message.storeRequest(nodeId, key, value, target));
        return reply != null && reply.isSuccess();
    }

    Message routeStore(Message req) {
        msgRcvd.incrementAndGet();
        req.incrementHop();

        Point target = req.getTargetPoint();

        if (zone.contains(target)) {
            boolean ok = store.put(req.getKey(), req.getValue(), nodeId);
            return Message.storeReply(nodeId, req.getKey(), ok, null);
        }

        if (req.getHopCount() > MAX_HOPS) {
            // give up and store here rather than drop the data
            log.warning(nodeId + ": STORE max hops, storing locally for " + req.getKey());
            store.put(req.getKey(), req.getValue(), nodeId);
            return Message.storeReply(nodeId, req.getKey(), true, "stored at wrong node (routing loop?)");
        }

        NodeInfo next = routingTable.nextHop(target, zone);
        if (next == null) {
            store.put(req.getKey(), req.getValue(), nodeId);
            return Message.storeReply(nodeId, req.getKey(), true, null);
        }

        CANNode nextNode = overlay.get(next.getNodeId());
        if (nextNode == null) {
            routingTable.removeNeighbor(next.getNodeId());
            return routeStore(req); // retry
        }

        hopTotal.incrementAndGet();
        routed.incrementAndGet();
        msgSent.incrementAndGet();
        return nextNode.routeStore(req);
    }

    // =========================================================================
    // LOOKUP
    // =========================================================================

    public Optional<String> lookupString(String key) {
        return lookup(key).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    public Optional<byte[]> lookup(String key) {
        Point target  = HashUtil.hashKey(key, dims);
        Message reply = routeLookup(Message.lookupRequest(nodeId, key, target));
        if (reply == null || !reply.isFound()) return Optional.empty();
        return Optional.ofNullable(reply.getValue());
    }

    Message routeLookup(Message req) {
        msgRcvd.incrementAndGet();
        req.incrementHop();

        Point target = req.getTargetPoint();

        if (zone.contains(target)) {
            DataStore.Entry entry = store.get(req.getKey());
            boolean found = entry != null;
            // System.out.println(nodeId + " lookup " + req.getKey() + " -> " + (found ? "HIT" : "miss")); // debug
            return Message.lookupReply(nodeId, req.getKey(), found ? entry.getValue() : null, found);
        }

        if (req.getHopCount() > MAX_HOPS) {
            // do a local check and give up
            DataStore.Entry e = store.get(req.getKey());
            return Message.lookupReply(nodeId, req.getKey(), e != null ? e.getValue() : null, e != null);
        }

        NodeInfo next = routingTable.nextHop(target, zone);
        if (next == null) {
            DataStore.Entry e = store.get(req.getKey());
            return Message.lookupReply(nodeId, req.getKey(), e != null ? e.getValue() : null, e != null);
        }

        CANNode nextNode = overlay.get(next.getNodeId());
        if (nextNode == null) {
            routingTable.removeNeighbor(next.getNodeId());
            return routeLookup(req);
        }

        hopTotal.incrementAndGet();
        routed.incrementAndGet();
        msgSent.incrementAndGet();
        return nextNode.routeLookup(req);
    }

    // =========================================================================
    // LEAVE
    // =========================================================================

    /**
     * Graceful departure.
     *
     * Find an adjacent neighbor to take over our zone, give them our data,
     * tell everyone we're leaving. Ungraceful departure (crash) is handled
     * by heartbeat timeouts -- that part isn't fully implemented yet.
     */
    public void leave() {
        log.info(nodeId + ": leaving");

        Optional<NodeInfo> candidateOpt = routingTable.bestTakeoverCandidate(zone);

        if (candidateOpt.isPresent()) {
            NodeInfo candidate = candidateOpt.get();
            CANNode  nb        = overlay.get(candidate.getNodeId());

            if (nb != null) {
                // hand off all our data
                nb.store.bulkLoad(store.snapshot());
                log.info(nodeId + ": handed " + store.size() + " entries to " + candidate.getNodeId());

                // expand their zone to cover ours
                nb.zone = zone.merge(candidate.getZone());
                log.info(candidate.getNodeId() + ": zone is now " + nb.zone);

                // give them our routing table entries (minus themselves)
                for (NodeInfo n : routingTable.getAllNeighbors()) {
                    if (!n.getNodeId().equals(candidate.getNodeId()))
                        nb.routingTable.addNeighbor(n);
                }
            }
        } else {
            log.warning(nodeId + ": no takeover candidate found, data will be lost");
        }

        // notify all neighbors
        Message bye = Message.leaveNotify(nodeId, null);
        for (NodeInfo n : routingTable.getAllNeighbors()) {
            CANNode nb = overlay.get(n.getNodeId());
            if (nb != null) nb.recv(bye);
        }

        overlay.remove(nodeId);
        bg.shutdown();
    }

    // =========================================================================
    // MESSAGE HANDLER
    // =========================================================================

    public void recv(Message msg) {
        msgRcvd.incrementAndGet();
        switch (msg.getType()) {
            case ZONE_UPDATE:
                if (msg.getUpdatedInfo() != null)
                    routingTable.addNeighbor(msg.getUpdatedInfo());
                break;
            case LEAVE_NOTIFY:
                routingTable.removeNeighbor(msg.getSenderId());
                break;
            case HEARTBEAT:
                CANNode sender = overlay.get(msg.getSenderId());
                if (sender != null)
                    sender.recv(Message.heartbeatAck(nodeId));
                break;
            case HEARTBEAT_ACK:
                // alive -- could update a last-seen map for smarter failure detection
                // but simple removal on next heartbeat timeout is good enough for now
                break;
            default:
                log.warning(nodeId + ": unexpected message: " + msg.getType());
        }
    }

    // alias -- some places in the code still use receiveMessage
    // TODO: unify to just recv()
    public void receiveMessage(Message msg) { recv(msg); }

    // =========================================================================
    // HEARTBEAT
    // =========================================================================

    private void heartbeat() {
        for (NodeInfo n : routingTable.getAllNeighbors()) {
            if (overlay.get(n.getNodeId()) == null) {
                log.warning(nodeId + ": " + n.getNodeId() + " appears dead");
                routingTable.removeNeighbor(n.getNodeId());
                // TODO: trigger zone repair / data recovery
            } else {
                overlay.get(n.getNodeId()).recv(Message.heartbeat(nodeId));
            }
        }
    }

    // =========================================================================
    // MISC
    // =========================================================================

    public NodeInfo toNodeInfo() {
        return new NodeInfo(nodeId, host, port, zone);
    }

    public String       getNodeId()       { return nodeId; }
    public Zone         getZone()         { return zone; }
    public int          getDimensions()   { return dims; }
    public RoutingTable getRoutingTable() { return routingTable; }
    public DataStore    getDataStore()    { return store; }

    public String metricsString() {
        double avg = routed.get() == 0 ? 0 : (double) hopTotal.get() / routed.get();
        return String.format("%s: zone=%s nbrs=%d entries=%d sent=%d rcvd=%d avgHops=%.2f",
            nodeId, zone, routingTable.size(), store.size(),
            msgSent.get(), msgRcvd.get(), avg);
    }

    @Override
    public String toString() {
        return nodeId + "{zone=" + zone + ", nbrs=" + routingTable.size() + ", entries=" + store.size() + "}";
    }
}