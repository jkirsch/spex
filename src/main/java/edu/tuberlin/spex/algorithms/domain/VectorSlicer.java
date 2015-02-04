package edu.tuberlin.spex.algorithms.domain;

import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;

/**
 * Date: 04.02.2015
 * Time: 20:42
 *
 */
public class VectorSlicer  {

    /**
     * create a sliced vector
     *
     * @param vector
     * @param start
     * @param end
     * @return
     */
    public static Vector slice(Vector vector, int start, int end) {

        DenseVector slicedVector = new DenseVector(end - start);

        for (int i = start; i < end; i++) {
            slicedVector.set(i - start, vector.get(i));
        }

        return slicedVector;
    }

    /**
     * reverse operation of sliced vector - create a new vector and copy the slice to startrow
     *
     * @param vector
     * @param start
     * @param n
     * @return
     */
    public static Vector upscale(Vector vector, int n, int start) {

        DenseVector upscaled = new DenseVector(n);

        for (int i = 0; i < vector.size(); i++) {
            upscaled.set(start + i, vector.get(i));
        }

        return upscaled;
    }

}
