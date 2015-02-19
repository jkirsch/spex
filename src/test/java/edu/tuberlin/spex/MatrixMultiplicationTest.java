package edu.tuberlin.spex;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;

/**
 * Date: 04.02.2015
 * Time: 19:59
 *
 */
public class MatrixMultiplicationTest {


    @Test
    public void testMultiplyBlocks() throws Exception {

        DenseMatrix matrix = new DenseMatrix(4, 4);

        matrix.set(0, 0, 1);
        matrix.set(1, 0, 1);
        matrix.set(2, 0, 8);
        matrix.set(2, 2, 8);

        Vector ones = VectorHelper.ones(matrix.numColumns());

        Vector mult = matrix.mult(ones, new DenseVector(matrix.numRows()));

        System.out.println(matrix);
        System.out.println(mult);

        // 4 blocks
        MatrixBlock matrixBlock1 = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);
        MatrixBlock matrixBlock3 = generateBlock(0,2,2,2);
        MatrixBlock matrixBlock4 = generateBlock(2,0,2,4,0,0,8,0,2,8);

        System.out.println(matrixBlock1);
        System.out.println(matrixBlock3);
        System.out.println(matrixBlock4);

        Vector mult1 = matrixBlock1.multRealigned(ones);
        Vector mult3 = matrixBlock3.multRealigned(ones);
        Vector mult4 = matrixBlock4.multRealigned(ones);

        // result obtained my adding all vectors together
        Vector blockWise = mult1.add(mult3).add(mult4);
        System.out.println(blockWise);

        Assert.assertEquals(mult.norm(Vector.Norm.TwoRobust), blockWise.norm(Vector.Norm.TwoRobust), 0.000001);


    }

    @Test
    public void testSmallMatrix() throws Exception {
        int n = 5;
        DenseMatrix matrix = new DenseMatrix(n, n);

        MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, 2);

        for (int row = 0; row < n; row++) {
            for (int col = 0; col < n; col++) {
                matrix.set(row, col, matrixBlockPartitioner.getKey(new Tuple3<Integer, Integer, Double>(row, col, 1d)));
            }

        }

        System.out.println(matrix);

    }
}
