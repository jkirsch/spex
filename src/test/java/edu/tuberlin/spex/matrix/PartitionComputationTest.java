package edu.tuberlin.spex.matrix;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * 22.04.2015.
 */
@RunWith(value = Parameterized.class)
public class PartitionComputationTest {

    @Parameterized.Parameter(0)
    public int rows;
    @Parameterized.Parameter(1)
    public int columns;
    @Parameterized.Parameter(2)
    public int blocks;

    @Parameterized.Parameters(name = "{index}: rows({0}) columns({1}) blocks({3}")
    public static Iterable<Integer[]> data1() {
        return Arrays.asList(new Integer[][]{
                {3, 2, 2},
                {4, 2, 2},
                {5, 2, 2},
                {8, 5, 3}
        });
    }

    @Test
    public void testRectangularMatrix() throws Exception {

        System.out.println(Joiner.on(" ").join(Strings.repeat("-", 20),rows,columns,"["+blocks+"]",Strings.repeat("-",20)));


        Multiset<Long> counter = HashMultiset.create();
        MatrixBlockPartitioner partitioner = new MatrixBlockPartitioner(rows, columns, blocks);

        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < columns; col++) {
                Long key = partitioner.getKey(new Tuple3<>(row, col, 2d));
                counter.add(key);
                System.out.print(key);
            }
            System.out.println();
        }

        assertThat(counter.elementSet(), hasSize(blocks*blocks));

    }
}
