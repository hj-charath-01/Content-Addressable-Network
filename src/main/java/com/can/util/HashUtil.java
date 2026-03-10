package com.can.util;

import com.can.core.Point;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Maps keys to points in the d-dimensional keyspace using SHA-256.
 *
 * Basic idea: hash the key, slice the digest into d equal chunks,
 * normalize each chunk to [0,1). Works fine up to d=8 since SHA-256
 * gives 32 bytes and we use 4 bytes per dimension.
 *
 * I briefly tried MD5 (faster, 16 bytes) but SHA-256 gives better
 * distribution in my experiments. Probably doesn't matter much but
 * figured I'd use the "real" hash.
 *
 * ref: Ratnasamy et al. section 3 -- they don't specify the hash function,
 * just say "a standard hash function"
 */
public class HashUtil {

    // no instances
    private HashUtil() {}

    public static Point hashKey(String key, int dims) {
        byte[] raw = key.getBytes(StandardCharsets.UTF_8);
        return hashBytes(raw, dims);
    }

    public static Point hashBytes(byte[] data, int dims) {
        byte[] digest = sha256(data);
        return toPoint(digest, dims);
    }

    private static Point toPoint(byte[] digest, int dims) {
        double[] coords = new double[dims];
        // 4 bytes per dimension = 32-bit resolution, good enough
        int bytesPerDim = Math.max(1, digest.length / dims);

        for (int d = 0; d < dims; d++) {
            long val = 0;
            int start = (d * bytesPerDim) % digest.length;
            for (int b = 0; b < bytesPerDim; b++) {
                val = (val << 8) | (digest[(start + b) % digest.length] & 0xFF);
            }
            coords[d] = (val & 0xFFFFFFFFL) / 4294967296.0;
        }

        return new Point(coords);
    }

    public static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // never happens
        }
    }

    // returns hex string -- useful for debugging which zone a key ends up in
    public static String sha256Hex(String s) {
        byte[] d = sha256(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : d) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}