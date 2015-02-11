package edu.tuberlin.spex.matrix.io;


import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;
import static org.hamcrest.core.Is.is;

public class LinkedSparseMatrixSerializerTest extends AbstractIOTest {


    @Test
    public void testSerialization() throws Exception {

        final MatrixBlock matrixBlock = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);
        LinkedSparseMatrix matrix = new LinkedSparseMatrix(matrixBlock.getMatrix());

        serialize(matrix);
        LinkedSparseMatrix matrix1 = deserialize(LinkedSparseMatrix.class);

        System.out.println(matrix1);

        Assert.assertThat(
                matrix1.norm(Matrix.Norm.Frobenius),
                is(matrix.norm(Matrix.Norm.Frobenius)));


    }
}