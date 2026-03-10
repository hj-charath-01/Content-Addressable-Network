package com.can.storage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Key-value store for a single node.
 *
 * Each node only stores keys whose hash lands in its zone. When the zone
 * shrinks (new neighbor takes half), we extract and transfer the relevant entries.
 *
 * Added versioning so we don't accidentally overwrite newer data with older data
 * during zone transfers. Versions are per-key and monotonically increase.
 *
 * TTL support is there but honestly for the simulation i mostly use NO_TTL.
 * Lazy eviction (check on get) + periodic sweep should be fine.
 */
public class DataStore {

    public static final long NO_TTL = -1;

    // -------------------------------------------------------------------------
    // inner class for stored entries

    public static class Entry {
        public final byte[] value;
        public final long   storedAt;
        public final long   ttlMs;
        public final long   version;
        public final String storedBy; // which node stored this originally

        Entry(byte[] value, long ttlMs, long version, String storedBy) {
            this.value    = Arrays.copyOf(value, value.length); // don't want external mutation
            this.storedAt = System.currentTimeMillis();
            this.ttlMs    = ttlMs;
            this.version  = version;
            this.storedBy = storedBy;
        }

        public byte[] getValue() {
            return Arrays.copyOf(value, value.length);
        }

        public boolean isExpired() {
            if (ttlMs == NO_TTL) return false;
            return System.currentTimeMillis() - storedAt > ttlMs;
        }

        // old naming -- keeping both so i don't break things
        // TODO: pick one and stick with it
        public long getVersion()  { return version; }
        public String getOwner()  { return storedBy; }
        public long getStoredAt() { return storedAt; }
        public long getTtlMs()    { return ttlMs; }
    }

    // -------------------------------------------------------------------------

    private final ConcurrentHashMap<String, Entry> data     = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long>  versions = new ConcurrentHashMap<>();

    // stats -- useful for the writeup
    private long puts   = 0;
    private long gets   = 0;
    private long hits   = 0;
    private long misses = 0;

    // -------------------------------------------------------------------------

    // returns true if the entry was actually stored (false = rejected due to version)
    public synchronized boolean put(String key, byte[] value, String nodeId) {
        return put(key, value, NO_TTL, nodeId);
    }

    public synchronized boolean put(String key, byte[] value, long ttlMs, String nodeId) {
        puts++;

        long nextVersion = versions.getOrDefault(key, 0L) + 1;
        Entry existing = data.get(key);

        if (existing != null && !existing.isExpired() && existing.version >= nextVersion) {
            // shouldn't happen often but want to be safe
            return false;
        }

        data.put(key, new Entry(value, ttlMs, nextVersion, nodeId));
        versions.put(key, nextVersion);
        return true;
    }

    public Entry get(String key) {
        gets++;
        Entry e = data.get(key);
        if (e == null || e.isExpired()) {
            if (e != null) data.remove(key); // lazy eviction
            misses++;
            return null;
        }
        hits++;
        return e;
    }

    // alias for the old API -- remove eventually
    public Entry getEntry(String key) { return get(key); }

    public boolean remove(String key) {
        return data.remove(key) != null;
    }

    public boolean containsKey(String key) {
        return get(key) != null;
    }

    // snapshot of all live entries -- used when we leave and need to hand off data
    public synchronized Map<String, Entry> snapshot() {
        Map<String, Entry> out = new HashMap<>();
        for (Map.Entry<String, Entry> e : data.entrySet()) {
            if (!e.getValue().isExpired())
                out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    /**
     * Pull out all entries matching predicate (and remove them from this store).
     * Called during zone splits -- new node calls this to grab its entries.
     */
    public synchronized Map<String, Entry> extractIf(Predicate<String> keyPredicate) {
        Map<String, Entry> transferred = new HashMap<>();
        Iterator<Map.Entry<String, Entry>> it = data.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Entry> e = it.next();
            if (!e.getValue().isExpired() && keyPredicate.test(e.getKey())) {
                transferred.put(e.getKey(), e.getValue());
                it.remove();
            }
        }
        return transferred;
    }

    // alias -- CANNode uses extractEntriesForTransfer in some places, fixing later
    public Map<String, Entry> extractEntriesForTransfer(Predicate<String> p) {
        return extractIf(p);
    }

    // load entries received from another node
    public void bulkLoad(Map<String, Entry> incoming) {
        for (Map.Entry<String, Entry> e : incoming.entrySet()) {
            if (!e.getValue().isExpired()) {
                data.put(e.getKey(), e.getValue());
                versions.put(e.getKey(), e.getValue().version);
            }
        }
    }

    // returns number of evicted entries (run periodically)
    public int evictExpired() {
        int count = 0;
        Iterator<Map.Entry<String, Entry>> it = data.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().isExpired()) { it.remove(); count++; }
        }
        if (count > 0) System.out.println("[DataStore] evicted " + count + " expired entries");
        return count;
    }

    public int    size()     { return data.size(); }
    public long   getPuts()  { return puts; }
    public long   getGets()  { return gets; }
    public long   getHits()  { return hits; }
    public long   getMisses(){ return misses; }

    public double hitRate() {
        return gets == 0 ? 0.0 : (double) hits / gets;
    }

    @Override
    public String toString() {
        return String.format("DataStore[n=%d, puts=%d, gets=%d, hit=%.1f%%]",
            size(), puts, gets, hitRate() * 100);
    }
}