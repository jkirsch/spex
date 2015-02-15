package edu.tuberlin.spex.matrix;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MatrixBlockPartitionerTest {

    @Test
    public void testPartition() throws Exception {

        int n = 11;
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

    @Test
    public void testGetBlockDimensions() throws Exception {

        int n = 11;
        int blocks = 2;

        MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blocks);

        Multiset<Integer> cols = HashMultiset.create();
        Multiset<Integer> rows = HashMultiset.create();

        for (int row = 0; row < n; row++) {
            for (int col = 0; col < n; col++) {
                MatrixBlockPartitioner.BlockDimensions blockDimensions = MatrixBlockPartitioner.getBlockDimensions(n, blocks, row, col);
                cols.add(blockDimensions.getCols());
                rows.add(blockDimensions.getRows());
                System.out.print(blockDimensions.getRows()+ "," + blockDimensions.getCols()+ " ");
            }
            System.out.println();
        }

        assertThat(cols.count(1), is(n));
        assertThat(rows.count(1), is(n));
    }

    @Test
    public void testDimensionsCorrect() throws Exception {

        final int n = 325729 + 1;
        final int blocks = 4;

        MatrixBlockPartitioner.BlockDimensions blockDimensions = MatrixBlockPartitioner.getBlockDimensions(n, blocks, 325728, 325729);

        assertThat(blockDimensions.getRowStart(), is(325728));
        assertThat(blockDimensions.getColStart(), is(325728));

    }

    @Test
    public void testDimensions2() throws Exception {

        // BlockDimensions{rowStart=11284, colStart=5376, rows=4, cols=4}  -Peek- (11285,5378,1.0) -Value- (11285,5378,1.0)
        final int n = 325729;
        final int blocks = 4;

        int blockSize = n / blocks;
        System.out.println(blockSize);

        MatrixBlockPartitioner.BlockDimensions blockDimensions = MatrixBlockPartitioner.getBlockDimensions(n, blockSize, 11285, 11285);

        assertThat(blockDimensions.getRowStart(), is(0));
        assertThat(blockDimensions.getColStart(), is(0));

        assertThat(blockDimensions.getRows(), is(blockSize));
        assertThat(blockDimensions.getCols(), is(blockSize));

    }
}