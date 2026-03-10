package com.can.core;

import java.io.Serializable;
import java.util.Arrays;

// represents a coordinate in the d-dimensional keyspace
// each dimension wraps around at 1.0 back to 0.0
public class Point implements Serializable {

    private static final long serialVersionUID = 1L;

    private final double[] coords;
    private final int d; // number of dimensions

    public Point(double... coordinates) {
        this.coords = Arrays.copyOf(coordinates, coordinates.length);
        this.d = coordinates.length;
    }

    public double get(int dim) {
        return coords[dim];
    }

    public int getDimensions() {
        return d;
    }

    // euclidean distance but accounting for wrap-around
    public double distanceTo(Point other) {
        if (other.d != this.d)
            throw new IllegalArgumentException("can't compare points with different dims (" + d + " vs " + other.d + ")");

        double sum = 0.0;
        for (int i = 0; i < d; i++) {
            double diff = Math.abs(this.coords[i] - other.coords[i]);
            diff = Math.min(diff, 1.0 - diff);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < d; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.4f", coords[i]));
        }
        return sb.append(")").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Point)) return false;
        return Arrays.equals(coords, ((Point) o).coords);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(coords);
    }
}