package edu.tuberlin.spex.matrix.serializer;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.Matrix;
import org.junit.Assert;
import org.junit.Test;

import static edu.tuberlin.spex.algorithms.domain.MatrixBlock.generateBlock;
import static org.hamcrest.core.Is.is;

public class DenseMatrixSerializerTest extends AbstractIOTest {


    @Test
    public void testSerialization() throws Exception {

        final MatrixBlock matrixBlock = generateBlock(0, 0, 2, 2, 0, 0, 1, 1, 0, 1);

        TestOutputView testOutputView = new TestOutputView();

        // serialize

        ((AdaptedCompRowMatrix)matrixBlock.getMatrix()).write(testOutputView);

        AdaptedCompRowMatrix m1 = new AdaptedCompRowMatrix();
        m1.read(testOutputView.getInputView());

        MatrixBlock matrixBlock1 = new MatrixBlock(0,0, m1);

        System.out.println(matrixBlock1);

        Assert.assertThat(
                matrixBlock1.getMatrix().norm(Matrix.Norm.Frobenius),
                is(matrixBlock.getMatrix().norm(Matrix.Norm.Frobenius)));


    }

}