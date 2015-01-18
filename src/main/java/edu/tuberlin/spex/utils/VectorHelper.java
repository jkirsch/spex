package edu.tuberlin.spex.utils;

import no.uib.cipr.matrix.DenseVector;

import java.util.Arrays;

/**
 * Date: 18.01.2015
 * Time: 19:51
 *
 */
public class VectorHelper {

    public static DenseVector identical(int size, double value) {
        double[] values = new double[size];
        Arrays.fill(values, value);
        return new DenseVector(values);
    }

    public static DenseVector ones(int size) {
        return identical(size, 1);
    }


}
