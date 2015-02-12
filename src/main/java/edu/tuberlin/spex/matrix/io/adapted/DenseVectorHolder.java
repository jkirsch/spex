package edu.tuberlin.spex.matrix.io.adapted;

import no.uib.cipr.matrix.DenseVector;

/**
 * Date: 12.02.2015
 * Time: 12:38
 *
 */
public class DenseVectorHolder {

    DenseVector vector;

    public DenseVectorHolder() {
    }

    public DenseVectorHolder(DenseVector vector) {
        this.vector = vector;
    }

    public DenseVector getVector() {
        return vector;
    }

    public void setVector(DenseVector vector) {
        this.vector = vector;
    }

    public DenseVectorHolder add(DenseVectorHolder t1) {

        vector.add(t1.getVector());
        return this;
    }

    @Override
    public String toString() {
        return "DenseVectorHolder{" +
                "vector=" + vector +
                '}';
    }
}
