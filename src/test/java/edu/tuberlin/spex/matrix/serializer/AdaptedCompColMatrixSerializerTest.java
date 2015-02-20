package edu.tuberlin.spex.matrix.serializer;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompColMatrix;
import no.uib.cipr.matrix.Matrix;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;
import static org.hamcrest.core.Is.is;

public class AdaptedCompColMatrixSerializerTest extends AbstractIOTest {

    @Test
    public void testSerialization() throws Exception {

        final MatrixBlock matrixBlock = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);
        Matrix matrix = new AdaptedCompColMatrix(matrixBlock.getMatrix());

        serialize(matrix);
        AdaptedCompColMatrix matrix1 = deserialize(AdaptedCompColMatrix.class);

        System.out.println(matrix);

        Assert.assertThat(
                matrix1.norm(Matrix.Norm.Frobenius),
                is(matrix.norm(Matrix.Norm.Frobenius)));


    }

}