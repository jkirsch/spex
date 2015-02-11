package edu.tuberlin.spex.matrix.io;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.io.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.Matrix;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;
import static org.hamcrest.core.Is.is;

public class AdaptedCompRowMatrixSerializerTest extends AbstractIOTest {

    @Test
    public void testSerialization() throws Exception {

        final MatrixBlock matrixBlock = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);
        Matrix matrix = new AdaptedCompRowMatrix(matrixBlock.getMatrix());

        serialize(matrix);
        AdaptedCompRowMatrix matrix1 = deserialize(AdaptedCompRowMatrix.class);

        System.out.println(matrix);

        Assert.assertThat(
                matrix1.norm(Matrix.Norm.Frobenius),
                is(matrix.norm(Matrix.Norm.Frobenius)));


    }

}