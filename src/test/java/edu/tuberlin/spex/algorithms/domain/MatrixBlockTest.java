package edu.tuberlin.spex.algorithms.domain;

import edu.tuberlin.spex.MatrixTest;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import org.junit.Test;

public class MatrixBlockTest {


    @Test
    public void testMatrixTest() throws Exception {

        DenseMatrix matrix = MatrixTest.createKnownMatrix();

        MatrixBlock matrixBlock = new MatrixBlock(0, 0, matrix);

        DenseVector ones = VectorHelper.ones(matrix.numColumns());

        Vector mult = matrixBlock.multRealigned(ones);
        System.out.println(matrix.transMult(ones, ones.copy()));

        matrix.transpose();
        mult = matrixBlock.multRealigned(ones);
        System.out.println(mult);

    }
}