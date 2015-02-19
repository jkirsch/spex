package edu.tuberlin.spex.algorithms.domain;

import no.uib.cipr.matrix.DenseVector;

import java.io.Serializable;

/**
 * This is a vector slice of vector elements
 *
 * Date: 18.02.2015
 * Time: 22:46
 *
 */
public class VectorBlock implements Serializable {

    /** Identifier to join with the corresponding matrix block. */
    public int startRow;
    public DenseVector vector;

    public VectorBlock() {
    }

    public VectorBlock(int startRow, DenseVector vector) {
        this.startRow = startRow;
        this.vector = vector;
    }

    public int getStartRow() {
        return startRow;
    }

    public DenseVector getVector() {
        return vector;
    }

    @Override
    public String toString() {
        return "VectorBlock{" +
                "startRow=" + startRow +
                ", vector=" + vector +
                '}';
    }
}
