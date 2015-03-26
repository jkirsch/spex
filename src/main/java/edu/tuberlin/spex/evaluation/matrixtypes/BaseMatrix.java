package edu.tuberlin.spex.evaluation.matrixtypes;

import com.google.common.primitives.Doubles;

/**
 * 19.03.2015.
 */
public abstract class BaseMatrix {

    // simple stupid container
    int numRows;
    int numColumns;

    public BaseMatrix(double[][] matrix) {

        // here we need to load the data

        // assume rectangular
        numRows = matrix.length;
        numColumns = matrix[0].length;
        loadDataInternal(matrix);
    }

    public static int getNNZ(double[][] matrix) {
        int numEntries = 0;
        for (double[] aMatrix : matrix) {
            for (double val : aMatrix) {
                if (Doubles.compare(val, 0) != 0) {
                    // we need this entry
                    numEntries++;
                }
            }
        }

        return numEntries;
    }

    protected abstract BaseMatrix loadDataInternal(double[][] matrix);


    public final String getName() {
        return this.getClass().getSimpleName();
    }

    public void print() {
        for (int row = 0; row < numRows; row++) {
            for (int col = 0; col < numColumns; col++) {
                double v = get(row, col);
                System.out.print((v == 0?"   ":v) + " ");
            }
            System.out.println();
        }
    }

    public abstract int getStorageSize();

    public abstract OperationsCounter getMultComplexity();

    /**
     * Checks the passed row and column indices
     */
    protected void check(int row, int column) {
        if (row < 0)
            throw new IndexOutOfBoundsException("row index is negative (" + row
                    + ")");
        if (column < 0)
            throw new IndexOutOfBoundsException("column index is negative ("
                    + column + ")");
        if (row >= numRows)
            throw new IndexOutOfBoundsException("row index >= numRows (" + row
                    + " >= " + numRows + ")");
        if (column >= numColumns)
            throw new IndexOutOfBoundsException("column index >= numColumns ("
                    + column + " >= " + numColumns + ")");
    }

    public double get(int row, int column) {
        throw new UnsupportedOperationException();
    }

    public static class OperationsCounter {
        final int memoryAccess;
        final int operationsCounter;

        public OperationsCounter(int memoryAccess, int operationsCounter) {
            this.memoryAccess = memoryAccess;
            this.operationsCounter = operationsCounter;
        }

        @Override
        public String toString() {
            return "Operationscounter{" +
                    "memoryAccess=" + memoryAccess +
                    ", operationsCounter=" + operationsCounter +
                    '}';
        }

        public int getMemoryAccess() {
            return memoryAccess;
        }

        public int getOperationsCounter() {
            return operationsCounter;
        }
    }

}
