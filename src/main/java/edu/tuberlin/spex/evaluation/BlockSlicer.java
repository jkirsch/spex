package edu.tuberlin.spex.evaluation;

import com.google.common.base.Preconditions;
import edu.tuberlin.spex.evaluation.matrixtypes.BaseMatrix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 24.03.2015.
 *
 */
public class BlockSlicer {


    public static List<double[][]> createBlocks(double[][] matrix, int blocks) {

//        Preconditions.checkArgument(matrix.length % blocks == 0, " If the blocks are off it might be hard");

        int n = matrix.length;
        final int adjustedN = n % blocks > 0 ? n + (blocks - n % blocks) : n;

        int blockSize = n / blocks;
        List<double[][]> blockMatrix = new ArrayList<>();


        // just step through the blocks
        for (int rowBlock = 0; rowBlock < blocks; rowBlock++) {
            int rowstart = rowBlock * blockSize;

            int rowSize;
            if (rowstart >= blockSize * (blocks - 1)) {
                rowSize = n - blockSize * (blocks - 1);
            } else {
                rowSize = blockSize;
            }


            for (int columnBlock = 0; columnBlock < blocks; columnBlock++) {
                double[][] theBlock = new double[rowSize][];
                blockMatrix.add(theBlock);

                int colStart = columnBlock * blockSize;

                // create the block matrix
                int colSize;

                if (colStart >= blockSize * (blocks - 1)) {
                    colSize = n - blockSize * (blocks - 1);
                } else {
                    colSize = blockSize;
                }

                for (int i = 0; i < theBlock.length; i++) {
                    double[] row = theBlock[i] = new double[colSize];
                    for (int j = 0; j < row.length; j++) {

                        row[j] = matrix[rowstart + i][colStart + j];

                    }
                }


            }
            // start with the first block
            // everything from
        }


        int c = 0;
        for (double[][] doubles : blockMatrix) {
            c += BaseMatrix.getNNZ(doubles);
        }

        if(c != BaseMatrix.getNNZ(matrix)) {
            System.out.println(Arrays.deepToString(matrix));
            for (double[][] doubles : blockMatrix) {
                System.out.println(Arrays.deepToString(doubles));
            }
            System.out.println();
            Preconditions.checkArgument(c == BaseMatrix.getNNZ(matrix), "For blocksize " + blocks);

        }


        return blockMatrix;

    }

    private static int getKey(int intputRow, int inputColumn, int blockSize, int blocks) {

        int row = intputRow / blockSize;
        int column = inputColumn / blockSize;

        int rowIndex;

        if (intputRow >= blockSize * blocks) {
            rowIndex = blocks;
        } else {
            rowIndex = row * blocks;
        }
        if (inputColumn >= blockSize * blocks) {
            column = blocks - 1;
        }

        return rowIndex + column;
    }
}
