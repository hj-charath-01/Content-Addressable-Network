package com.can.core;

import java.io.Serializable;

// lightweight struct holding what a node knows about a neighbor
public class NodeInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final String host;
    private final int    port;
    private final Zone   zone;

    public NodeInfo(String nodeId, String host, int port, Zone zone) {
        this.nodeId = nodeId;
        this.host   = host;
        this.port   = port;
        this.zone   = zone;
    }

    public String getNodeId() { return nodeId; }
    public String getHost()   { return host; }
    public int    getPort()   { return port; }
    public Zone   getZone()   { return zone; }
    public String getAddress(){ return host + ":" + port; }

    @Override
    public String toString() {
        return nodeId + "@" + getAddress() + zone;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NodeInfo)) return false;
        return nodeId.equals(((NodeInfo) o).nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }
}