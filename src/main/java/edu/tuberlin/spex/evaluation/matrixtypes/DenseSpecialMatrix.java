package edu.tuberlin.spex.evaluation.matrixtypes;

/**
 * 25.03.2015.
 *
 */
public class DenseSpecialMatrix extends BaseMatrix {

    private double[][] data;

    public static DenseSpecialMatrix loadData(double[][] matrix) {
        DenseSpecialMatrix denseSpecialMatrix = new DenseSpecialMatrix();

        denseSpecialMatrix.load(matrix);

        return denseSpecialMatrix;
    }

    private void load(double[][] matrix) {
        numRows = matrix.length;
        numColumns = matrix[0].length;
        this.data = matrix;
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
