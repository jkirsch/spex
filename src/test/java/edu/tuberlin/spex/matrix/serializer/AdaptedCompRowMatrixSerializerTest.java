package edu.tuberlin.spex.matrix.serializer;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.Matrix;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;
import static org.hamcrest.core.Is.is;

public class AdaptedCompRowMatrixSerializerTest extends AbstractIOTest {

    @Test
    public void testSerialization() throws Exception {

        final MatrixBlock matrixBlock = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);
        AdaptedCompRowMatrix matrix = new AdaptedCompRowMatrix(matrixBlock.getMatrix());

        TestOutputView testOutputView = new TestOutputView();

        // serialize
        matrix.write(testOutputView);

        // read back
        AdaptedCompRowMatrix matrix1 = new AdaptedCompRowMatrix();
        matrix1.read(testOutputView.getInputView());

        System.out.println(matrix);

        Assert.assertThat(
                matrix1.norm(Matrix.Norm.Frobenius),
                is(matrix.norm(Matrix.Norm.Frobenius)));


    }

}