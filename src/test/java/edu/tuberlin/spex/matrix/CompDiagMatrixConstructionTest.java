package edu.tuberlin.spex.matrix;

import edu.tuberlin.spex.matrix.adapted.AdaptedCompDiagMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.sparse.CompDiagMatrix;
import org.junit.Test;

/**
 * 25.04.2015.
 *
 */
public class CompDiagMatrixConstructionTest {

    @Test
    public void testCreation() throws Exception {

        DenseMatrix denseMatrix = new DenseMatrix(6, 6);

        // set some entries
        // simulate the matrix from http://web.eecs.utk.edu/~dongarra/etemplates/node376.html
        denseMatrix.set(0, 0, 10);
        denseMatrix.set(0, 1, -3);

        denseMatrix.set(1, 0, 3);
        denseMatrix.set(1, 1, 9);
        denseMatrix.set(1, 2, 6);

        denseMatrix.set(2, 1, 7);
        denseMatrix.set(2, 2, 8);
        denseMatrix.set(2, 3, 7);

        denseMatrix.set(3, 2, 8);
        denseMatrix.set(3, 3, 7);
        denseMatrix.set(3, 4, 5);

        denseMatrix.set(4, 3, 9);
        denseMatrix.set(4, 4, 9);
        denseMatrix.set(4, 5, 13);

        denseMatrix.set(5, 4, 2);
        denseMatrix.set(5, 5, -1);

        CompDiagMatrix compDiagMatrix = new CompDiagMatrix(denseMatrix);

        System.out.println(denseMatrix);
        System.out.println(compDiagMatrix);

        // now check the our adapted format does not break anything
        AdaptedCompDiagMatrix adaptedCompDiagMatrix = new AdaptedCompDiagMatrix(denseMatrix.numRows(), denseMatrix.numColumns());





    }
}
