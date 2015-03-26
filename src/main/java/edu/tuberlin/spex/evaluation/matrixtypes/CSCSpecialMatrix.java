package edu.tuberlin.spex.evaluation.matrixtypes;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.commons.lang.ArrayUtils;

import java.util.*;

/**
 * 24.03.2015.
 *
 */
public class CSCSpecialMatrix extends BaseMatrix {

    /**
     * Matrix data
     */
    double[] data;

    /**
     * Column indices. These are kept sorted within each row.
     */
    int[] columnPointer;

    /**
     * Indices to the start of each row
     */
    int[] rowIndex;

    public CSCSpecialMatrix(double[][] matrix) {
        super(matrix);
    }

    protected CSCSpecialMatrix loadDataInternal(double[][] matrix) {

        // Find the number of entries on each column
        List<Set<Integer>> cnz = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; ++i)
            cnz.add(new HashSet<Integer>());

        List<Double> values = Lists.newArrayList();

        // add data -- in column order

        for (int column = 0; column < numColumns; column++) {
            for (int row = 0; row < numRows; row++) {
                double val = matrix[row][column];
                if(Doubles.compare(val, 0) != 0) {
                    // we need this entry
                    cnz.get(column).add(row);
                    values.add(val);
                }
            }
        }

        int[][] nz = new int[numColumns][];
        for (int i = 0; i < numColumns; ++i) {
            nz[i] = new int[cnz.get(i).size()];
            int j = 0;
            for (Integer rowind : cnz.get(i))
                nz[i][j++] = rowind;
        }

        construct(nz);

        data = ArrayUtils.toPrimitive(values.toArray(new Double[values.size()]));

        return this;
    }

    private void construct(int[][] nz) {
        int nnz = 0;
        for (int i = 0; i < nz.length; ++i)
            nnz += nz[i].length;

        columnPointer = new int[numColumns + 1];
        rowIndex = new int[nnz];
        data = new double[nnz];

        if (nz.length != numColumns)
            throw new IllegalArgumentException("nz.length != numColumns");

        for (int i = 1; i <= numColumns; ++i) {
            columnPointer[i] = columnPointer[i - 1] + nz[i - 1].length;

            for (int j = columnPointer[i - 1], k = 0; j < columnPointer[i]; ++j, ++k) {
                rowIndex[j] = nz[i - 1][k];
                if (nz[i - 1][k] < 0 || nz[i - 1][k] >= numRows)
                    throw new IllegalArgumentException("nz[" + (i - 1) + "]["
                            + k + "]=" + nz[i - 1][k]
                            + ", which is not a valid row index");
            }

            Arrays.sort(rowIndex, columnPointer[i - 1], columnPointer[i]);
        }
    }

    @Override
    public int getStorageSize() {
        return columnPointer.length + rowIndex.length + data.length;
    }

    @Override
    public double get(int row, int column) {
        check(row, column);

        int index = Arrays.binarySearch(rowIndex,
                columnPointer[column], columnPointer[column + 1], row);

        if (index >= 0)
            return data[index];
        else
            return 0;
    }

    @Override
    public OperationsCounter getMultComplexity() {

        int memoryAccess = 0;
        int multiplications = 0;

        for (int i = 0; i < numColumns; ++i)
            for (int j = columnPointer[i]; j < columnPointer[i + 1]; ++j) {

                // access the data[j]
                memoryAccess++;
                // access the vector
                memoryAccess++;

                // multiplications
                multiplications++;

                // get the rowIndex[j]
                memoryAccess++;

                // set the vector
                memoryAccess++;


            }
                //yd[rowIndex[j]] += data[j] * xd[i];

        return new OperationsCounter(memoryAccess, multiplications);
    }
}
