package edu.tuberlin.spex.evaluation.matrixtypes;

/**
 * 25.03.2015.
 *
 */
public class DenseSpecialMatrix extends BaseMatrix {

    private double[][] data;

    public DenseSpecialMatrix(double[][] matrix) {
        super(matrix);

    }

    @Override
    protected BaseMatrix loadDataInternal(double[][] matrix) {
        this.data = matrix;
        return this;
    }


    @Override
    public int getStorageSize() {
        return numColumns * numRows;
    }

    @Override
    public OperationsCounter getMultComplexity() {

        int memoryAccess = 0;
        int multiplications = 0;

        for (double[] doubles : data) {
            for (double aDouble : doubles) {
                // get the data from the matrix
                memoryAccess++;
                // get the vector
                memoryAccess++;
                // write back
                memoryAccess++;

                multiplications++;
            }
        }

        return new OperationsCounter(memoryAccess, multiplications);
    }

    @Override
    public double get(int row, int column) {
        check(row, column);
        return data[row][column];
    }
}
