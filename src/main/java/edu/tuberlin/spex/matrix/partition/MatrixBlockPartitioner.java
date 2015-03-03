package edu.tuberlin.spex.matrix.partition;

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 09.02.2015
 * Time: 22:56
 *
 */
public class MatrixBlockPartitioner implements KeySelector<Tuple3<Integer, Integer, Double>, Long> {

    /** Size the of dataset. **/
    final int n;

    /** Number of blocks. */
    final int blocks;
    private final int blockSize;
    private final int m;

    public MatrixBlockPartitioner(int n, int blocks) {

       // Preconditions.checkArgument(blocks % 2 == 0, "Blocks needs to be a factor of two");
        Preconditions.checkArgument(n >= blocks, "The square matrix needs to have at least the number of block rows");

        this.n = n;
        this.blocks = blocks;
        blockSize = n / blocks;

        // how many rows per blocks
        m = n / blockSize;// + n % blockSize;
    }


    @Override
    public Long getKey(Tuple3<Integer, Integer, Double> value) throws Exception {

        long row = value.f0 / blockSize;
        long column = value.f1 / blockSize;

        long rowIndex;

        if(value.f0 >= blockSize * blocks) {
            rowIndex = blocks;
        } else {
            rowIndex = row * blocks;
        }
        if(value.f1 >= blockSize * blocks) {
          column =  blocks - 1;
        }

        return rowIndex  + column;
    }


    /**
     * Computes the sizes for the different needed vectors for the given partition
     * @return
     */
    public List<BlockDimensions> computeRowSizes() {
        // blocksize
        List<BlockDimensions> sizes = new ArrayList<>();
        for (int i = 0; i < blocks - 1; i++) {
            sizes.add(new BlockDimensions(i * blockSize, 0, blockSize, 1));
        }
        // take care of the last
        sizes.add(new BlockDimensions((blocks - 1) * blockSize, 0, n - blockSize * (blocks - 1), 1));
        return sizes;
    }

    /**
     * Compute back the block dimensions from the offsets.
     * @param inputRow
     * @param inputCol
     */
    public static BlockDimensions getBlockDimensions(Integer n, Integer blockSize, Integer inputRow, Integer inputCol) {

        Preconditions.checkArgument(n > 0);
        Preconditions.checkArgument(blockSize > 0);
        Preconditions.checkElementIndex(inputRow, n, "inputRow");
        Preconditions.checkElementIndex(inputCol, n, "inputCol");

        int row = inputRow / blockSize;
        int column = inputCol / blockSize;


        int rowSize = inputRow >= n - n % blockSize ? n % blockSize : blockSize;
        int colSize = inputCol >= n - n % blockSize  ? n % blockSize : blockSize;

        return new BlockDimensions(row * blockSize, column * blockSize, rowSize, colSize);
    }

    public static class BlockDimensions {
        int rowStart;
        int colStart;

        int rows;
        int cols;

        public BlockDimensions(int rowStart, int colStart, int rows, int cols) {
            this.rowStart = rowStart;
            this.colStart = colStart;
            this.rows = rows;
            this.cols = cols;
        }

        public int getRowStart() {
            return rowStart;
        }

        public int getColStart() {
            return colStart;
        }

        public int getRows() {
            return rows;
        }

        public int getCols() {
            return cols;
        }

        @Override
        public String toString() {
            return "BlockDimensions{" +
                    "rowStart=" + rowStart +
                    ", colStart=" + colStart +
                    ", rows=" + rows +
                    ", cols=" + cols +
                    '}';
        }
    }
}
