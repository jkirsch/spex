package edu.tuberlin.spex.matrix;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
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

    @Parameterized.Parameters(name = "{index}: calc({0})")
    public static Iterable<Integer[]> data1() {
        return Arrays.asList(new Integer[][]{
                {3},
                {4},
                {5}
        });
    }

    @Parameterized.Parameter(0)
    public int n;

    @Test
    public void testPartition() throws Exception {

        System.out.println(Joiner.on(" ").join(Strings.repeat("-",20),n,Strings.repeat("-",20)));

        int blocks = 2;

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
        assertThat(counter.count(3L), is((n / blocks) * (n / blocks)));

    }


}
