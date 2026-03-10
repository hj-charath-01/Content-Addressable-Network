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

/*
  A node in the CAN.
 
  Handles the four main operations from the paper:
    - JOIN:   find a zone, split it, take half
    - STORE:  hash key to point, route to responsible node, store locally
    - LOOKUP: hash key to point, route to responsible node, return value
    - LEAVE:  hand off data and zone, notify neighbors
 */
public class CANNode {

    private static final Logger log = Logger.getLogger(CANNode.class.getName());

    private static final int  MAX_HOPS     = 50;
    private static final long HEARTBEAT_MS = 5_000;
    private static final long EVICT_MS     = 30_000;

    final  String nodeId;
    private final String host;
    private final int    port;
    private final int    dims;

    volatile Zone zone;

    private final RoutingTable         routingTable;
    private final DataStore            store;
    private final Map<String, CANNode> overlay;

    private final AtomicLong msgSent  = new AtomicLong();
    private final AtomicLong msgRcvd  = new AtomicLong();
    private final AtomicLong hopTotal = new AtomicLong();
    private final AtomicLong routed   = new AtomicLong();

    private final ScheduledExecutorService bg;

    public CANNode(String nodeId, String host, int port, int dims, Zone zone, Map<String, CANNode> overlay) {
        this.nodeId       = nodeId;
        this.host         = host;
        this.port         = port;
        this.dims         = dims;
        this.zone         = zone;
        this.overlay      = overlay;
        this.routingTable = new RoutingTable(dims);
        this.store        = new DataStore();

        this.bg = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "can-bg-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        overlay.put(nodeId, this);

        bg.scheduleAtFixedRate(this::heartbeat,      HEARTBEAT_MS, HEARTBEAT_MS, TimeUnit.MILLISECONDS);
        bg.scheduleAtFixedRate(store::evictExpired,  EVICT_MS,     EVICT_MS,     TimeUnit.MILLISECONDS);
    }

    // JOIN

    public boolean join(String bootstrapId) {
        CANNode boot = overlay.get(bootstrapId);
        if (boot == null) {
            log.warning(nodeId + ": bootstrap not found: " + bootstrapId);
            return false;
        }

        Point   p     = Zone.fullSpace(dims).randomPoint(new Random());
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
        if (next == null) return doJoin(req);

        CANNode nextNode = overlay.get(next.getNodeId());
        if (nextNode == null) {
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

        List<NodeInfo> oldNeighbors = new ArrayList<>(routingTable.getAllNeighbors());

        Map<String, DataStore.Entry> handoff = store.extractEntriesForTransfer(key -> {
            Point kp = HashUtil.hashKey(key, dims);
            return assign.contains(kp);
        });

        this.zone = keep;

        NodeInfo newNodeInfo = new NodeInfo(newId, "localhost", 0, assign);
        routingTable.addNeighbor(newNodeInfo);

        NodeInfo myInfo = toNodeInfo();
        for (NodeInfo nb : oldNeighbors) {
            CANNode nbNode = overlay.get(nb.getNodeId());
            if (nbNode == null) continue;
            nbNode.recv(Message.zoneUpdate(nodeId, myInfo));
            nbNode.recv(Message.zoneUpdate(nodeId, newNodeInfo));
        }

        oldNeighbors.add(myInfo);
        Message reply = Message.joinReply(nodeId, assign, oldNeighbors, true);

        CANNode newNode = overlay.get(newId);
        if (newNode != null && !handoff.isEmpty()) {
            newNode.store.bulkLoad(handoff);
            log.info(nodeId + ": transferred " + handoff.size() + " entries to " + newId);
        }

        log.info(nodeId + ": split -> kept=" + keep + " assigned=" + assign + " to " + newId);
        return reply;
    }

    // STORE

    public boolean store(String key, String value) {
        return store(key, value.getBytes(StandardCharsets.UTF_8));
    }

    public boolean store(String key, byte[] value) {
        Point   target = HashUtil.hashKey(key, dims);
        Message reply  = routeStore(Message.storeRequest(nodeId, key, value, target));
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
            return routeStore(req);
        }

        hopTotal.incrementAndGet();
        routed.incrementAndGet();
        msgSent.incrementAndGet();
        return nextNode.routeStore(req);
    }

    // LOOKUP

    public Optional<String> lookupString(String key) {
        return lookup(key).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    public Optional<byte[]> lookup(String key) {
        Point   target = HashUtil.hashKey(key, dims);
        Message reply  = routeLookup(Message.lookupRequest(nodeId, key, target));
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
            return Message.lookupReply(nodeId, req.getKey(), found ? entry.getValue() : null, found);
        }

        if (req.getHopCount() > MAX_HOPS) {
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

    // LEAVE

    public void leave() {
        log.info(nodeId + ": leaving");

        Optional<NodeInfo> candidateOpt = routingTable.bestTakeoverCandidate(zone);

        if (candidateOpt.isPresent()) {
            NodeInfo candidate = candidateOpt.get();
            CANNode  nb = overlay.get(candidate.getNodeId());

            if (nb != null) {
                // steps 2-4: data + zone + routing table handoff
                nb.store.bulkLoad(store.snapshot());
                log.info(nodeId + ": handed " + store.size() + " entries to " + candidate.getNodeId());

                nb.zone = zone.merge(candidate.getZone());
                log.info(candidate.getNodeId() + ": zone expanded to " + nb.zone);

                for (NodeInfo n : routingTable.getAllNeighbors()) {
                    if (!n.getNodeId().equals(candidate.getNodeId()))
                        nb.routingTable.addNeighbor(n);
                }

                // step 5: broadcast updated NodeInfo for the takeover node 

                NodeInfo expandedTakeover = nb.toNodeInfo();  // captures nb.zone AFTER merge
                for (NodeInfo n : routingTable.getAllNeighbors()) {
                    if (n.getNodeId().equals(candidate.getNodeId())) continue; // skip self
                    CANNode neighbor = overlay.get(n.getNodeId());
                    if (neighbor != null)
                        neighbor.recv(Message.zoneUpdate(nodeId, expandedTakeover));
                }
                log.info(nodeId + ": broadcast zone expansion of " + candidate.getNodeId() + " -> " + nb.zone + " to " + (routingTable.size() - 1) + " neighbors");
            }
        } else {
            log.warning(nodeId + ": no takeover candidate found, data will be lost");
        }

        // step 6: tell everyone to drop node
        Message bye = Message.leaveNotify(nodeId, null);
        for (NodeInfo n : routingTable.getAllNeighbors()) {
            CANNode nb = overlay.get(n.getNodeId());
            if (nb != null) nb.recv(bye);
        }

        // step 7: disappear 
        overlay.remove(nodeId);
        bg.shutdown();
    }

    // MESSAGE HANDLER

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
                break;
            default:
                log.warning(nodeId + ": unexpected message: " + msg.getType());
        }
    }

    public void receiveMessage(Message msg) { recv(msg); }

    // HEARTBEAT

    private void heartbeat() {
        for (NodeInfo n : routingTable.getAllNeighbors()) {
            if (overlay.get(n.getNodeId()) == null) {
                log.warning(nodeId + ": " + n.getNodeId() + " appears dead");
                routingTable.removeNeighbor(n.getNodeId());
            } else {
                overlay.get(n.getNodeId()).recv(Message.heartbeat(nodeId));
            }
        }
    }

    // MISC

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