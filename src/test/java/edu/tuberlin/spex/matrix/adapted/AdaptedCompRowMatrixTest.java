package edu.tuberlin.spex.matrix.adapted;

import com.google.common.collect.Lists;
import no.uib.cipr.matrix.DenseMatrix;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AdaptedCompRowMatrixTest {

    @Test
    public void testBuildFromSortedIterator() throws Exception {

        List<Tuple3<Integer, Integer, Double>> elements = Lists.newArrayList(
                generate(0, 1, 2),
                generate(0, 2, 2),
                generate(1, 0, 2)
        );

        AdaptedCompRowMatrix matrix = AdaptedCompRowMatrix.buildFromSortedIterator(elements.iterator(),3,3);

        System.out.println(matrix);
        System.out.println(new DenseMatrix(matrix));

        assertThat(matrix.get(0,1), is(2d));
        assertThat(matrix.get(0,2), is(2d));
        assertThat(matrix.get(1,0), is(2d));

    }

    private Tuple3<Integer, Integer, Double> generate(int row, int col, double value) {
        return new Tuple3<>(row, col, value);
    }
}