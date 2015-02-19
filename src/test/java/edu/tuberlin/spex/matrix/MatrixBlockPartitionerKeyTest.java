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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Date: 14.02.2015
 * Time: 13:51
 *
 */
@RunWith(value = Parameterized.class)
public class MatrixBlockPartitionerKeyTest {

    @Parameterized.Parameters(name = "{index}: part({0},{1})")
    public static Iterable<Integer[]> data1() {
        return Arrays.asList(new Integer[][]{
                {3, 2},
                {4, 2},
                {5, 2}
        });
    }

    @Parameterized.Parameter(0)
    public int n;


    @Parameterized.Parameter(1)
    public int blocks;

    @Test
    public void testPartition() throws Exception {

        System.out.println(Joiner.on(" ").join(Strings.repeat("-",20),n,"["+blocks+"]",Strings.repeat("-",20)));

        MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blocks);

        Multiset<Long> counter = HashMultiset.create();

        for (int row = 0; row < n; row++) {
            for (int col = 0; col < n; col++) {
                Long key = matrixBlockPartitioner.getKey(new Tuple3<>(row, col, 2d));
                counter.add(key);
                System.out.print(key);
            }
            System.out.println();
        }

        assertThat(counter.count(0L), is((n / blocks) * (n / blocks)));
        assertThat(counter.count(3L), is((n / blocks) * (n / blocks) + (n%2==0?0:(2*(n / blocks)+1))));

    }


}
