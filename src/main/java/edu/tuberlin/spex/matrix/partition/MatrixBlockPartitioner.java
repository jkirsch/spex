package edu.tuberlin.spex.matrix.partition;

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 09.02.2015
 * Time: 22:56
 */
public class MatrixBlockPartitioner implements KeySelector<Tuple3<Integer, Integer, Double>, Long> {

    /**
     * Number of blocks.
     */
    final int blocks;
    /**
     * Size the of dataset.
     */
    private final int rows;
    /**
     * Number of columns.
     */
    private final int columns;
    private final int m;
    private final int rowWidth;
    private final int colwidth;


    /**
     * Creates the partitioner assuming a square matrix.
     *
     * @param rows   number of rows
     * @param blocks number of blocks to split in (per row)
     */
    public MatrixBlockPartitioner(int rows, int blocks) {
        this(rows, rows, blocks);
    }

    /**
     * Creates a partitioner for a rectangular matrix.
     *
     * @param rows    number of rows int the matrix
     * @param columns number of columns in the matrix
     * @param blocks  number of block to split (per row)
     */
    public MatrixBlockPartitioner(int rows, int columns, int blocks) {

        // Preconditions.checkArgument(blocks % 2 == 0, "Blocks needs to be a factor of two");
        Preconditions.checkArgument(rows >= blocks, "The matrix needs to have at least the number of row blocks rows");
        Preconditions.checkArgument(columns >= blocks, "The  matrix needs to have at least the number of column blocks columns");

        this.rows = rows;
        this.columns = columns;

        this.blocks = blocks;
        rowWidth = (int) Math.ceil(rows / (double) blocks);

        // how many rows per blocks
        m = rows / rowWidth;// + rows % rowWidth;

        // how many columns per block
        colwidth = (int) Math.ceil(columns / (double) blocks);
    }

    /**
     * Compute back the block dimensions from the offsets of a square matrix.
     *
     * @param inputRow
     * @param inputCol
     */
    public static BlockDimensions getBlockDimensions(Integer n, Integer blocks, Integer inputRow, Integer inputCol) {
        return getBlockDimensions(n, n, blocks, inputRow, inputCol);
    }

    /**
     * Compute back the block dimensions from the offsets of a matrix.
     *
     * @param inputRow
     * @param inputCol
     */
    public static BlockDimensions getBlockDimensions(Integer rows, Integer columns, Integer blocks, Integer inputRow, Integer inputCol) {

        Preconditions.checkArgument(rows > 0);
        Preconditions.checkArgument(columns > 0);
        Preconditions.checkArgument(blocks > 0);
        Preconditions.checkElementIndex(inputRow, rows, "inputRow");
        Preconditions.checkElementIndex(inputCol, columns, "inputCol");

        int rowWidth = (int) Math.ceil(rows / (double) blocks);
        // how many columns per block

        int colwidth = (int) Math.ceil(columns / (double) blocks);

        int row = inputRow / rowWidth;
        int column = inputCol / colwidth;

        int rowSize = row == blocks-1 ? rows - rowWidth*(blocks-1) : rowWidth;
        int colSize = column == blocks-1 ? columns - colwidth*(blocks-1) : colwidth;

        return new BlockDimensions(row * rowWidth, column * colwidth, rowSize, colSize);
    }

    @Override
    public Long getKey(Tuple3<Integer, Integer, Double> value) throws Exception {

        long row = value.f0 / rowWidth;
        long column = value.f1 / colwidth;

        long rowIndex;

        if (value.f0 >= rowWidth * blocks) {
            rowIndex = blocks;
        } else {
            rowIndex = row * blocks;
        }
        if (value.f1 >= colwidth * blocks) {
            column = blocks - 1;
        }

        return rowIndex + column;
    }

    /**
     * Computes the sizes for the different needed vectors for the given partition
     *
     * @return list of row dimensions.
     */
    public List<BlockDimensions> computeRowSizes() {
        // blocksize
        List<BlockDimensions> sizes = new ArrayList<>();
        int startRow = 0;
        while(startRow + colwidth < columns) {
            sizes.add(new BlockDimensions(startRow, 0, colwidth, 1));
            startRow += colwidth;

        }
        // take care of the last
        sizes.add(new BlockDimensions(startRow, 0, columns - startRow, 1));
        return sizes;
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
