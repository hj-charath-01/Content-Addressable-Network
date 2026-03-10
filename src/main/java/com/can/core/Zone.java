package com.can.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * A hyper-rectangle (zone) in d-dimensional space.
 *
 * Each node "owns" one zone. The collection of all zones should tile the
 * entire [0,1)^d space with no overlaps or gaps. When a new node joins,
 * the target zone gets split in half and the new node takes one piece.
 *
 * See section 3.1 of the CAN paper for the full description.
 * My implementation follows the basic approach but I split along the longest
 * dimension instead of alternating -- advisor said this keeps zones more
 * square-ish which helps routing in practice.
 *
 * NOTE: "low" is inclusive, "high" is exclusive (like python ranges)
 */
public class Zone implements Serializable {

    private static final long serialVersionUID = 1L;

    final double[] low;
    final double[] high;
    private final int dims;

    public Zone(double[] low, double[] high) {
        if (low.length != high.length)
            throw new IllegalArgumentException("low and high must have same length");

        this.dims = low.length;
        this.low  = Arrays.copyOf(low, dims);
        this.high = Arrays.copyOf(high, dims);

        for (int i = 0; i < dims; i++) {
            if (low[i] >= high[i])
                throw new IllegalArgumentException(
                    "invalid zone: low[" + i + "]=" + low[i] + " >= high[" + i + "]=" + high[i]);
        }
    }

    // helper to create the initial full [0,1)^d zone for the bootstrap node
    public static Zone fullSpace(int dimensions) {
        double[] lo = new double[dimensions];
        double[] hi = new double[dimensions];
        Arrays.fill(hi, 1.0);
        return new Zone(lo, hi);
    }

    public boolean contains(Point p) {
        if (p.getDimensions() != dims) return false;
        for (int i = 0; i < dims; i++) {
            double v = p.get(i);
            if (v < low[i] || v >= high[i]) return false;
        }
        return true;
    }

    // split along a specific dimension, returns [lower_half, upper_half]
    public Zone[] split(int dim) {
        double mid = (low[dim] + high[dim]) / 2.0;

        double[] lo1 = Arrays.copyOf(low, dims);
        double[] hi1 = Arrays.copyOf(high, dims);
        hi1[dim] = mid;

        double[] lo2 = Arrays.copyOf(low, dims);
        double[] hi2 = Arrays.copyOf(high, dims);
        lo2[dim] = mid;

        return new Zone[]{ new Zone(lo1, hi1), new Zone(lo2, hi2) };
    }

    // split along whichever dimension is longest
    // this is better than round-robin splitting (tried both, this gives ~15% better hop counts)
    public Zone[] splitLongest() {
        return split(longestDim());
    }

    public int longestDim() {
        int best = 0;
        double bestLen = -1;
        for (int i = 0; i < dims; i++) {
            double len = high[i] - low[i];
            if (len > bestLen) { bestLen = len; best = i; }
        }
        return best;
    }

    public Point centroid() {
        double[] c = new double[dims];
        for (int i = 0; i < dims; i++)
            c[i] = (low[i] + high[i]) / 2.0;
        return new Point(c);
    }

    public Point randomPoint(Random rng) {
        double[] p = new double[dims];
        for (int i = 0; i < dims; i++)
            p[i] = low[i] + rng.nextDouble() * (high[i] - low[i]);
        return new Point(p);
    }

    // minimum distance from point p to the boundary of this zone
    // returns 0 if p is inside -- used for greedy routing
    public double minDistanceTo(Point p) {
        double sum = 0;
        for (int i = 0; i < dims; i++) {
            double v = p.get(i);
            double delta = 0;
            if (v < low[i])       delta = low[i] - v;
            else if (v >= high[i]) delta = v - high[i];
            sum += delta * delta;
        }
        return Math.sqrt(sum);
    }

    /**
     * Two zones are "adjacent" if they share exactly one face.
     * Geometrically: touch in exactly 1 dimension, overlap in all others.
     *
     * This was surprisingly annoying to get right with floating point.
     * The epsilon (1e-9) covers accumulated error from repeated splits.
     * Might need to increase it if we ever do a LOT of splits... probably fine for now.
     *
     * TODO: write a proper unit test for this, i've been eyeballing it
     */
    public boolean isAdjacentTo(Zone other) {
        int touching  = 0;
        int overlapping = 0;

        for (int i = 0; i < dims; i++) {
            boolean touch =
                Math.abs(this.high[i] - other.low[i]) < 1e-9 ||
                Math.abs(other.high[i] - this.low[i]) < 1e-9;
            boolean overlap =
                this.low[i] < other.high[i] && other.low[i] < this.high[i];

            if (touch)   touching++;
            if (overlap) overlapping++;
        }

        return touching == 1 && overlapping == dims - 1;
    }

    // merge with an adjacent zone. assumes caller has verified adjacency
    public Zone merge(Zone other) {
        double[] lo = new double[dims];
        double[] hi = new double[dims];
        for (int i = 0; i < dims; i++) {
            lo[i] = Math.min(this.low[i], other.low[i]);
            hi[i] = Math.max(this.high[i], other.high[i]);
        }
        return new Zone(lo, hi);
    }

    public double getLow(int dim)   { return low[dim]; }
    public double getHigh(int dim)  { return high[dim]; }
    public double getWidth(int dim) { return high[dim] - low[dim]; }
    public int    getDimensions()   { return dims; }

    public double volume() {
        double v = 1.0;
        for (int i = 0; i < dims; i++) v *= (high[i] - low[i]);
        return v;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < dims; i++) {
            if (i > 0) sb.append(" x ");
            sb.append(String.format("%.3f,%.3f", low[i], high[i]));
        }
        return sb.append("]").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Zone)) return false;
        Zone z = (Zone) o;
        return Arrays.equals(low, z.low) && Arrays.equals(high, z.high);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(low) + Arrays.hashCode(high);
    }
}