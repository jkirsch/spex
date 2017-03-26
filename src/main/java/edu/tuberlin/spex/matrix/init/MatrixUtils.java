package edu.tuberlin.spex.matrix.init;

import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseMatrix;

import java.util.Random;

/**
 * Created by Johannes on 15.04.2016.
 */
public class MatrixUtils {

    /**
     * creates a random sparse matrix.
     *
     * @param fill fill parameter
     * @param size square size
     */
    public static AdaptedCompRowMatrix generateSparserandom(double fill, int size) {

        Random random = new Random(1000);

        DenseMatrix A = new DenseMatrix(size, size);

        for (int j = 0; j < size; ++j)
            for (int i = 0; i < size; ++i)
                if (random.nextFloat() < fill) {
                    A.set(i, j, random.nextFloat());
                }
        return new AdaptedCompRowMatrix(A);

    }

}
