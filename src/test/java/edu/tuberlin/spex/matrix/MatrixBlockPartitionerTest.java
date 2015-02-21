package edu.tuberlin.spex.matrix;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.utils.VectorBlockHelper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
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

        List<MatrixBlockPartitioner.BlockDimensions> blockDimensionses = matrixBlockPartitioner.computeRowSizes();

        assertThat(counter.count(0L), is((n / blocks) * (n / blocks)));
        assertThat(counter.count(3L), is((n / blocks) * (n / blocks) + n));

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

        int n = 325729;
        final int blocks = 2;

        MatrixBlockPartitioner.BlockDimensions dims = MatrixBlockPartitioner.getBlockDimensions(n, n / blocks, 325728, 162864);

        assertThat(dims.getRowStart(), is(325728));
        assertThat(dims.getColStart(), is(162864));
        assertThat(dims.getRows(), is(1));
        assertThat(dims.getCols(), is(n - 162864 -1));


        dims = MatrixBlockPartitioner.getBlockDimensions(n, n / blocks, 325727, 325728);

        System.out.println(dims);

        for (n = 2; n < 100000; n++) {
            MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blocks);
            List<MatrixBlockPartitioner.BlockDimensions> blockDimensions = matrixBlockPartitioner.computeRowSizes();


            for (MatrixBlockPartitioner.BlockDimensions blockDimension : blockDimensions) {

                dims = MatrixBlockPartitioner.getBlockDimensions(n, n / blocks, blockDimension.getRowStart(), blockDimension.getColStart());
                assertThat(dims.getRowStart(), is(blockDimension.getRowStart()));
                assertThat(dims.getColStart(), is(blockDimension.getColStart()));

            }

        }




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

    @Test
    public void testComputeRowSizes() throws Exception {

        int blocks = 2;

        for (int n = 2; n <= 325729; n++) {
            MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blocks);

            List<MatrixBlockPartitioner.BlockDimensions> dimensions = matrixBlockPartitioner.computeRowSizes();
            assertThat(n + " " + dimensions.toString(), dimensions.size(), is(2));

            int sum = 0;
            for (MatrixBlockPartitioner.BlockDimensions dimension : dimensions) {
                sum += dimension.getRows();
                assertThat("n:" + n, dimension.getRowStart() + dimension.getRows() <= n, is(true));
            }

            assertThat(n + " " + dimensions.toString(), sum, is(n));
        }
    }

    @Test
    public void testComputeRowSizesLarge() throws Exception {
        int n = 325729;
        int blocks = 128;

        final int adjustedN = n % blocks > 0?n + (blocks - n % blocks):n;

        List<VectorBlock> vectorBlocks = VectorBlockHelper.createBlocks(adjustedN, blocks, 1 / (double) n);

        for (VectorBlock vectorBlock : vectorBlocks) {
            assertNotNull(vectorBlock);
        }

    }
}