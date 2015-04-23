package edu.tuberlin.spex.evaluation.matrixtypes;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.commons.lang.ArrayUtils;

import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.*;

/**
 * 24.03.2015.
 */
public class CSRSpecialMatrix extends BaseMatrix {


    private DoubleBuffer data;
    private IntBuffer rowPointer;
    private IntBuffer columnIndex;

    public CSRSpecialMatrix(double[][] matrix) {
        super(matrix);
    }

    protected CSRSpecialMatrix loadDataInternal(double[][] matrix) {

        // build up the arrays

        // row indices
        List<Set<Integer>> rnz = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; ++i)
            rnz.add(new HashSet<Integer>());

        List<Double> values = Lists.newArrayList();

        for (int row = 0; row < matrix.length; row++) {
            for (int column = 0; column < matrix[row].length; column++) {
                double val = matrix[row][column];
                if (Doubles.compare(val, 0) != 0) {
                    // we need this entry
                    rnz.get(row).add(column);
                    values.add(val);
                }
            }
        }

        // size of the nnz
        int[][] nz = new int[numRows][];
        for (int i = 0; i < numRows; ++i) {
            nz[i] = new int[rnz.get(i).size()];
            int j = 0;
            for (Integer colind : rnz.get(i))
                nz[i][j++] = colind;
        }

        construct(nz);

        // set the values .. assume that they are in the correct order
        data.put(ArrayUtils.toPrimitive(values.toArray(new Double[values.size()])));

        return this;
    }


    private void construct(int[][] nz) {

        // count the nnz

        int nnz = 0;
        for (int i = 0; i < nz.length; ++i)
            nnz += nz[i].length;

        int[] rowPointer = new int[numRows + 1];
        int[] columnIndex = new int[nnz];

        data = DoubleBuffer.wrap(new double[nnz]);

        if (nz.length != numRows)
            throw new IllegalArgumentException("nz.length != numRows");

        for (int i = 1; i <= numRows; ++i) {
            rowPointer[i] = rowPointer[i - 1] + nz[i - 1].length;

            for (int j = rowPointer[i - 1], k = 0; j < rowPointer[i]; ++j, ++k) {
                columnIndex[j] = nz[i - 1][k];
                if (nz[i - 1][k] < 0 || nz[i - 1][k] >= numColumns)
                    throw new IllegalArgumentException("nz[" + (i - 1) + "]["
                            + k + "]=" + nz[i - 1][k]
                            + ", which is not a valid column index");
            }

            Arrays.sort(columnIndex, rowPointer[i - 1], rowPointer[i]);
        }


        this.rowPointer = IntBuffer.wrap(rowPointer);
        this.columnIndex = IntBuffer.wrap(columnIndex);

    }


    @Override
    public double get(int row, int column) {
        check(row, column);

        int index = Arrays.binarySearch(columnIndex.array(),
                rowPointer.array()[row], rowPointer.array()[row + 1], column);

        if (index >= 0)
            return data.get(index);
        else
            return 0;
    }

    @Override
    public int getStorageSize() {
        return rowPointer.capacity() + columnIndex.capacity() + data.capacity();
    }

    @Override
    public OperationsCounter getMultComplexity() {
        // Algorithm ..
        // DenseVector optimisations

        int memoryAccess = 0;
        int multiplications = 0;

        for (int i = 0; i < numRows; ++i) {
            // row-wise multiplication
            for (int j = rowPointer.array()[i]; j < rowPointer.array()[i + 1]; j++) {
                // access the the data array
                memoryAccess++;

                // access the columnindex position J
                memoryAccess++;

                // access the vector itself at position columnindex[j]

                memoryAccess++;

                multiplications++;

                // write back the vector
                memoryAccess++;
                // access the column array
                //dot += data.get(j) * xd[columnIndex.array()[j]];
            }

        }
        return new OperationsCounter(memoryAccess, multiplications);
    }
}
