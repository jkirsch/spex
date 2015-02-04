package edu.tuberlin.spex.algorithms.domain;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;

import java.util.Iterator;

/**
 * Date: 04.02.2015
 * Time: 23:03
 *
 */
public class VectorSlice implements Vector {

    Vector vector;
    int start;
    int end;

    public VectorSlice(Vector vector, int start, int end) {
        this.vector = vector;
        this.start = start;
        this.end = end;
    }

    public Vector getVector() {
        return vector;
    }

    @Override
    public int size() {
        return end - start;
    }

    @Override
    public void set(int index, double value) {
        vector.set(index + start, value);
    }

    @Override
    public void add(int index, double value) {
        vector.add(index + start, value);
    }

    @Override
    public double get(int index) {
        return vector.get(index + start);
    }

    @Override
    public Vector copy() {
        return null;
    }

    @Override
    public Vector zero() {
        vector.zero();
        return this;
    }

    @Override
    public Vector scale(double alpha) {
        vector.scale(alpha);
        return this;
    }

    @Override
    public Vector set(Vector y) {
        vector.set(y);
        return this;
    }

    @Override
    public Vector set(double alpha, Vector y) {
        vector.set(alpha, y);
        return this;
    }

    @Override
    public Vector add(Vector y) {
        vector.add(y);
        return this;
    }

    @Override
    public Vector add(double alpha, Vector y) {
        vector.add(alpha, y);
        return this;
    }

    @Override
    public double dot(Vector y) {
        return vector.dot(y);
    }

    @Override
    public double norm(Norm type) {
        return vector.norm(type);
    }

    @Override
    public Iterator<VectorEntry> iterator() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
