package edu.tuberlin.spex.block;

import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.io.MatrixVectorReader;
import no.uib.cipr.matrix.sparse.CompRowMatrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * 18.06.2015
 */
public class BlockCompressedSparseRowFormatTest {

    @Test
    public void testParseSmall() throws Exception {

        // replicate matrix from http://ccis2k.org/iajit/PDF/vol.8,no.2/3-766.pdf

        // http://www.cs.colostate.edu/~mroberts/toolbox/c++/sparseMatrix/sparse_matrix_compression.html

        FlexCompRowMatrix matrix = new FlexCompRowMatrix(6, 6);

        matrix.set(0, 0, 11);
        matrix.set(0, 1, 12);
        matrix.set(0, 2, 13);
        matrix.set(0, 3, 14);

        matrix.set(1, 1, 22);
        matrix.set(1, 2, 23);

        matrix.set(2, 2, 33);
        matrix.set(2, 3, 34);
        matrix.set(2, 4, 35);
        matrix.set(2, 5, 36);

        matrix.set(3, 3, 44);
        matrix.set(3, 4, 45);

        matrix.set(4, 5, 56);
        matrix.set(5, 5, 66);

        System.out.println(new DenseMatrix(matrix));
;

        BlockCompressedSparseRowFormat blockCompressedSparseRowFormat = new BlockCompressedSparseRowFormat(new CompRowMatrix(matrix), 2, 2);

        System.out.println(Arrays.toString(blockCompressedSparseRowFormat.value));

        // add one
/*        int[] bcol_index = blockCompressedSparseRowFormat.bcol_index;
        int[] brow_ptr = blockCompressedSparseRowFormat.brow_ptr;
        for (int i = 0; i < bcol_index.length; i++) {
            bcol_index[i]++;
        }
        for (int i = 0; i < brow_ptr.length; i++) {
            brow_ptr[i]++;
        }
        System.out.println("Col Index " + Arrays.toString(bcol_index));
        System.out.println("Row Index " + Arrays.toString(brow_ptr));
*/
        DenseVector ones = VectorHelper.ones(matrix.numColumns());

        DenseVector multiplication = blockCompressedSparseRowFormat.multiplication(ones);

        DenseVector mult = (DenseVector) matrix.mult(ones, new DenseVector(matrix.numRows()));

        Assert.assertArrayEquals(mult.getData(), multiplication.getData(), 0.001);
    }

    @Test
    public void testParse() throws Exception {
        Path path = ExperimentDatasets.get(ExperimentDatasets.Matrix.rlfprim);
        CompRowMatrix matrix = new CompRowMatrix(new MatrixVectorReader(new FileReader(path.toFile())));

        BlockCompressedSparseRowFormat blockCompressedSparseRowFormat = new BlockCompressedSparseRowFormat(matrix, 2, 2);

        int nr = 1 + (matrix.numRows() - 1) / 2;
        assertThat(blockCompressedSparseRowFormat.brow_ptr.length, is(nr + 1));


    }

}