package com.can.network;

import com.can.core.NodeInfo;
import com.can.core.Point;
import com.can.core.Zone;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Messages passed between nodes.
 *
 * I originally had a class hierarchy (StoreRequest extends Message, etc) but
 * serializing polymorphic types over sockets was getting annoying so i switched
 * to a single class with a type field and nullable payload fields. Not the
 * prettiest design but it works and is easy to debug.
 *
 * hopCount is incremented at each routing step -- useful for measuring
 * actual vs theoretical hop counts.
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {
        JOIN_REQUEST, JOIN_REPLY,
        STORE_REQUEST, STORE_REPLY,
        LOOKUP_REQUEST, LOOKUP_REPLY,
        LEAVE_NOTIFY, ZONE_UPDATE,
        HEARTBEAT, HEARTBEAT_ACK
    }

    // always present
    private final String id;        // for dedup, not really used yet
    private final Type   type;
    private final String senderId;
    private int hopCount = 0;

    // join
    private Point joinPoint;

    // join reply
    private Zone assignedZone;
    private List<NodeInfo> neighbors;

    // store / lookup
    private String key;
    private byte[] value;
    private Point  targetPoint;

    // results
    private boolean success;
    private boolean found;
    private String  errMsg;

    // for LEAVE_NOTIFY and ZONE_UPDATE
    private NodeInfo updatedInfo;

    private Message(Type type, String senderId) {
        this.id       = UUID.randomUUID().toString();
        this.type     = type;
        this.senderId = senderId;
    }

    // -- factories -------------------------------------------------------------

    public static Message joinRequest(String from, Point p) {
        Message m = new Message(Type.JOIN_REQUEST, from);
        m.joinPoint = p;
        return m;
    }

    public static Message joinReply(String from, Zone zone, List<NodeInfo> neighbors, boolean ok) {
        Message m = new Message(Type.JOIN_REPLY, from);
        m.assignedZone = zone;
        m.neighbors    = neighbors;
        m.success      = ok;
        return m;
    }

    public static Message storeRequest(String from, String key, byte[] value, Point target) {
        Message m = new Message(Type.STORE_REQUEST, from);
        m.key         = key;
        m.value       = value;
        m.targetPoint = target;
        return m;
    }

    public static Message storeReply(String from, String key, boolean ok, String err) {
        Message m = new Message(Type.STORE_REPLY, from);
        m.key     = key;
        m.success = ok;
        m.errMsg  = err;
        return m;
    }

    public static Message lookupRequest(String from, String key, Point target) {
        Message m = new Message(Type.LOOKUP_REQUEST, from);
        m.key         = key;
        m.targetPoint = target;
        return m;
    }

    public static Message lookupReply(String from, String key, byte[] value, boolean found) {
        Message m = new Message(Type.LOOKUP_REPLY, from);
        m.key   = key;
        m.value = value;
        m.found = found;
        return m;
    }

    public static Message leaveNotify(String from, NodeInfo info) {
        Message m = new Message(Type.LEAVE_NOTIFY, from);
        m.updatedInfo = info;
        return m;
    }

    public static Message zoneUpdate(String from, NodeInfo info) {
        Message m = new Message(Type.ZONE_UPDATE, from);
        m.updatedInfo = info;
        return m;
    }

    public static Message heartbeat(String from)    { return new Message(Type.HEARTBEAT, from); }
    public static Message heartbeatAck(String from) { return new Message(Type.HEARTBEAT_ACK, from); }

    // -- accessors -------------------------------------------------------------

    public Type           getType()               { return type; }
    public String         getSenderId()           { return senderId; }
    public int            getHopCount()           { return hopCount; }
    public Point          getJoinPoint()          { return joinPoint; }
    public Zone           getAssignedZone()       { return assignedZone; }
    public List<NodeInfo> getBootstrapNeighbors() { return neighbors; }
    public String         getKey()                { return key; }
    public byte[]         getValue()              { return value; }
    public Point          getTargetPoint()        { return targetPoint; }
    public boolean        isSuccess()             { return success; }
    public boolean        isFound()               { return found; }
    public String         getErrorMessage()       { return errMsg; }
    public NodeInfo       getUpdatedInfo()        { return updatedInfo; }

    public void incrementHop() { hopCount++; }

    @Override
    public String toString() {
        return type + " from=" + senderId + " hops=" + hopCount;
    }
}