package edu.tuberlin.spex.block;

import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.CompRowMatrix;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

/**
 * 18.06.2015
 */
public class BlockCompressedSparseRowFormat {


    final int r;
    final int c;
    /**
     * Number of rows
     */
    final int numRows;
    /**
     * Number of columns
     */
    final int numColumns;
    // the values
    double[] value;
    // what is the column index in each block
    int[] bcol_index;
    // when does a new block begin
    int[] brow_ptr;


    public BlockCompressedSparseRowFormat(CompRowMatrix matrix, int r, int c) {
        this.r = 2;
        this.c = 2;

        // fix it to 2x2 blocks

        // we need to build up the individual blocks
        // do it in a slow manner, just so that we have something that works

        value = new double[0];
        brow_ptr = new int[0];

        for (int row = 0; row < matrix.numRows(); row += r) {

            boolean set = false;
            for (int col = 0; col < matrix.numColumns(); col += c) {

                // check if this block is set
                double v = 0;
                v += Math.abs(matrix.get(row, col));
                v += Math.abs(matrix.get(row, col + 1));
                v += Math.abs(matrix.get(row + 1, col));
                v += Math.abs(matrix.get(row + 1, col + 1));


                if (v > 0) {
                    // this block has at least one value set
                    value = ArrayUtils.add(value, matrix.get(row, col));
                    value = ArrayUtils.add(value, matrix.get(row, col + 1));
                    value = ArrayUtils.add(value, matrix.get(row + 1, col));
                    value = ArrayUtils.add(value, matrix.get(row + 1, col + 1));

                    // also store the column index for this block
                    bcol_index = ArrayUtils.add(bcol_index, col);

                    // also set the pointer for the beginning of a row

                    if (!set) {
                        brow_ptr = ArrayUtils.add(brow_ptr, ((value.length - 4) / c) / r);
                        set = true;
                    }
                }

            }

            if (!set) {
                // empty row
                // we need to add an empty block
                value = ArrayUtils.add(value, 0);
                value = ArrayUtils.add(value, 0);
                value = ArrayUtils.add(value, 0);
                value = ArrayUtils.add(value, 0);
                bcol_index = ArrayUtils.add(bcol_index, 0);
                brow_ptr = ArrayUtils.add(brow_ptr, ((value.length - 4) / c) / r);
            }


        }



        numColumns = matrix.numColumns();
        numRows = matrix.numRows();

        // close the row pointer array
        brow_ptr = ArrayUtils.add(brow_ptr, numRows - 1);


    }

    public DenseVector multiplication(DenseVector x) throws IOException {

        double[] xd = ((DenseVector) x).getData();
        DenseVector y = new DenseVector(numRows);
        double[] yd = y.getData();

        int yPos = 0;
        int val = 0;
        for (int i = 0; i < numRows / r; i++, yPos += 2) {
            double y0 = 0;
            double y1 = 0;

            for (int j = brow_ptr[i]; j < brow_ptr[i + 1];
                 j++, val += 4) {
                int k = bcol_index[j];
                double x0 = xd[k], x1 = xd[k + 1];

                // two dot multiplies ...
               // y0 += Core.DotProduct_V64fV64f_S64f(value, val, xd, k, 2);
               // y1 += Core.DotProduct_V64fV64f_S64f(value, val + 2, xd, k, 2);

                y0 += value[0 + val] * x0;
                y1 += value[2 + val] * x0;
                y0 += value[1 + val] * x1;
                y1 += value[3 + val] * x1;
            }

            yd[0 + yPos] = y0;
            yd[1 + yPos] = y1;

        }


        //for b = 0 to brows-1

        return y;

    }

}
