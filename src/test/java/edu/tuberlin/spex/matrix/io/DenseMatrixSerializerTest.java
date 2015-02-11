package edu.tuberlin.spex.matrix.io;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import no.uib.cipr.matrix.Matrix;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;
import static org.hamcrest.core.Is.is;

public class DenseMatrixSerializerTest extends AbstractIOTest {


    @Test
    public void testSerialization() throws Exception {

        final MatrixBlock matrixBlock = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);

        serialize(matrixBlock);
        MatrixBlock matrixBlock1 = deserialize(MatrixBlock.class);

        System.out.println(matrixBlock1);

        Assert.assertThat(
                matrixBlock1.getMatrix().norm(Matrix.Norm.Frobenius),
                is(matrixBlock.getMatrix().norm(Matrix.Norm.Frobenius)));


    }

}