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
import static org.hamcrest.Matchers.hasSize;
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

        assertThat(counter.elementSet(), hasSize(blocks*blocks));

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

        assertThat(cols.count(6), is(6*n));
        assertThat(rows.count(6), is(6*n));
    }

    @Test
    public void testDimensionsCorrect() throws Exception {

        int n = 325729;
        final int blocks = 2;

        MatrixBlockPartitioner.BlockDimensions dims = MatrixBlockPartitioner.getBlockDimensions(n, blocks, 325728, 162864);

        assertThat(dims.getRowStart(), is(162865));
        assertThat(dims.getColStart(), is(0));
        assertThat(dims.getRows(), is(162864));
        assertThat(dims.getCols(), is(n - 162864));


        dims = MatrixBlockPartitioner.getBlockDimensions(n, blocks, 325727, 325728);

        System.out.println(dims);

        for (n = 2; n < 100000; n++) {
            MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blocks);
            List<MatrixBlockPartitioner.BlockDimensions> blockDimensions = matrixBlockPartitioner.computeRowSizes();


            for (MatrixBlockPartitioner.BlockDimensions blockDimension : blockDimensions) {

                dims = MatrixBlockPartitioner.getBlockDimensions(n, blocks, blockDimension.getRowStart(), blockDimension.getColStart());
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

        int blockSize = (int) Math.ceil(n / (double) blocks);
        System.out.println(blockSize);

        MatrixBlockPartitioner.BlockDimensions blockDimensions = MatrixBlockPartitioner.getBlockDimensions(n, blocks, 11285, 11285);

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

        List<VectorBlock> vectorBlocks = VectorBlockHelper.createBlocks(adjustedN, adjustedN, blocks, 1 / (double) n);

        for (VectorBlock vectorBlock : vectorBlocks) {
            assertNotNull(vectorBlock);
        }

    }

    @Test
    public void testComputeRectangularMatrix() throws Exception {
        int rows = 8;
        int columns = 5;

        int blocks = 3;

        MatrixBlockPartitioner partitioner = new MatrixBlockPartitioner(rows, columns, blocks);

        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < columns; col++) {
                Long key = partitioner.getKey(new Tuple3<>(row, col, 2d));
                System.out.print(key);
            }
            System.out.println();
        }

        assertThat(partitioner.getKey(new Tuple3<>(0, 0, 2d)), is(0L));
        assertThat(partitioner.getKey(new Tuple3<>(1, 0, 2d)), is(0L));
        assertThat(partitioner.getKey(new Tuple3<>(2, 0, 2d)), is(0L));

        assertThat(partitioner.getKey(new Tuple3<>(0, 1, 2d)), is(0L));
        assertThat(partitioner.getKey(new Tuple3<>(1, 1, 2d)), is(0L));
        assertThat(partitioner.getKey(new Tuple3<>(2, 1, 2d)), is(0L));

        assertThat(partitioner.getKey(new Tuple3<>(0, 2, 2d)), is(1L));
        assertThat(partitioner.getKey(new Tuple3<>(1, 2, 2d)), is(1L));
        assertThat(partitioner.getKey(new Tuple3<>(2, 2, 2d)), is(1L));

        assertThat(partitioner.getKey(new Tuple3<>(0, 3, 2d)), is(1L));
        assertThat(partitioner.getKey(new Tuple3<>(1, 3, 2d)), is(1L));
        assertThat(partitioner.getKey(new Tuple3<>(2, 3, 2d)), is(1L));

        assertThat(partitioner.getKey(new Tuple3<>(0, 4, 2d)), is(2L));
        assertThat(partitioner.getKey(new Tuple3<>(1, 4, 2d)), is(2L));
        assertThat(partitioner.getKey(new Tuple3<>(2, 4, 2d)), is(2L));

        assertThat(partitioner.getKey(new Tuple3<>(3, 0, 2d)), is(3L));

    }
}