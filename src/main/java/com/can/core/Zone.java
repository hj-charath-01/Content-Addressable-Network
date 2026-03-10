package com.can.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/*
  A hyper-rectangle (zone) in d-dimensional space.
 
  Each node "owns" one zone. The collection of all zones should tile the entire [0,1)^d space with no overlaps or gaps. 
  When a new node joins, the target zone gets split in half and the new node takes one piece.
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
        this.low  = Arrays.copyOf(low,  dims);
        this.high = Arrays.copyOf(high, dims);

        for (int i = 0; i < dims; i++) {
            if (low[i] >= high[i])
                throw new IllegalArgumentException( "invalid zone: low[" + i + "]=" + low[i] + " >= high[" + i + "]=" + high[i]);
        }
    }

    // The initial full [0,1)^d zone for the bootstrap node. 
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

    // Split along a specific dimension; returns [lower_half, upper_half]. 
    public Zone[] split(int dim) {
        double mid = (low[dim] + high[dim]) / 2.0;

        double[] lo1 = Arrays.copyOf(low,  dims);
        double[] hi1 = Arrays.copyOf(high, dims);
        hi1[dim] = mid;

        double[] lo2 = Arrays.copyOf(low,  dims);
        double[] hi2 = Arrays.copyOf(high, dims);
        lo2[dim] = mid;

        return new Zone[]{ new Zone(lo1, hi1), new Zone(lo2, hi2) };
    }

    // Split along the longest dimension. 
    public Zone[] splitLongest() {
        return split(longestDim());
    }

    public int longestDim() {
        int    best    = 0;
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

    /*
      Minimum distance from point p to the boundary of this zone.
      Returns 0 if p is inside — used for greedy routing.
     */
    public double minDistanceTo(Point p) {
        double sum = 0;
        for (int i = 0; i < dims; i++) {
            double v = p.get(i);
            double delta = 0;
            if (v < low[i])        delta = low[i]  - v;
            else if (v >= high[i]) delta = v - high[i];
            sum += delta * delta;
        }
        return Math.sqrt(sum);
    }

    /*
      Two zones are "adjacent" if and only if they share exactly one COMPLETE
      face — meaning they can be cleanly merged into a single rectangle with no gaps and no overlap with any third zone.
     
      Conditions:
        - Exactly 1 dimension where the boundaries touch (the split axis).
        - All other (dims-1) dimensions have IDENTICAL extents: same low AND same high (within floating-point epsilon).
     
     */
    public boolean isAdjacentTo(Zone other) {
        int touching   = 0;
        int exactMatch = 0;

        for (int i = 0; i < dims; i++) {
            boolean touch =
                Math.abs(this.high[i] - other.low[i])  < 1e-9 ||
                Math.abs(other.high[i] - this.low[i])  < 1e-9;

            boolean exact =
                Math.abs(this.low[i]  - other.low[i])  < 1e-9 &&
                Math.abs(this.high[i] - other.high[i]) < 1e-9;

            if (touch) touching++;
            if (exact) exactMatch++;
        }

        // Valid merge partner: 1 touching dimension, all others identical.
        return touching == 1 && exactMatch == dims - 1;
    }

    // Merge with an adjacent zone.
    public Zone merge(Zone other) {
        double[] lo = new double[dims];
        double[] hi = new double[dims];
        for (int i = 0; i < dims; i++) {
            lo[i] = Math.min(this.low[i],  other.low[i]);
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