package edu.tuberlin.spex;

import com.google.common.base.Preconditions;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import org.junit.Assert;
import org.junit.Test;

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

        DenseVector ones = VectorHelper.ones(matrix.numColumns());

        Vector mult = matrix.mult(ones, new DenseVector(matrix.numRows()));

        System.out.println(matrix);
        System.out.println(mult);

        // 4 blocks
        MatrixBlock matrixBlock1 = generateBlock(0,0,2,2,0,0,1,1,0,1);
        MatrixBlock matrixBlock3 = generateBlock(0,2,2,2);
        MatrixBlock matrixBlock4 = generateBlock(2,0,2,4,0,0,8,0,2,8);

        System.out.println(matrixBlock1);
        System.out.println(matrixBlock3);
        System.out.println(matrixBlock4);

        Vector mult1 = matrixBlock1.mult(ones);
        Vector mult3 = matrixBlock3.mult(ones);
        Vector mult4 = matrixBlock4.mult(ones);

        // result obtained my adding all vectors together
        Vector blockWise = mult1.add(mult3).add(mult4);
        System.out.println(blockWise);

        Assert.assertEquals(mult.norm(Vector.Norm.TwoRobust), blockWise.norm(Vector.Norm.TwoRobust), 0.000001);


    }

    /**
     * Simple Matrix generator
     * @param startRow
     * @param startCol
     * @param elements elements are a list of row,col,value .. row,col,value ...
     * @return
     */
    private MatrixBlock generateBlock(int startRow, int startCol, int rows, int columns, int ... elements) {

        Preconditions.checkArgument(elements.length % 3 == 0, "Always 3 elements make a cell");

        Matrix matrix = new DenseMatrix(rows, columns);

        for (int i = 0; i < elements.length; i+=3) {
            matrix.set(elements[i], elements[i + 1], elements[i + 2]);
        }

        return new MatrixBlock(startRow, startCol, matrix);
    }
}
