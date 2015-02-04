package edu.tuberlin.spex.algorithms.domain;

import com.google.common.base.Preconditions;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;

/**
 * Date: 04.02.2015
 * Time: 20:21
 *
 */
public class MatrixBlock {

    int startRow;
    int startCol;

    Matrix matrix;

    public MatrixBlock(int startRow, int startCol, Matrix matrix) {
        this.startRow = startRow;
        this.startCol = startCol;
        this.matrix = matrix;
    }

    public long getStartRow() {
        return startRow;
    }

    public long getStartCol() {
        return startCol;
    }

    public Matrix getMatrix() {
        return matrix;
    }

    /**
     * <code>y = A*x</code>
     *
     * @param x
     *            Vector of size <code>A.numColumns()</code>
     * @return y
     */
    public Vector mult(Vector x) {

        Preconditions.checkArgument(startCol + matrix.numColumns()  <= x.size(),
                "Vector dimension needs to be at least " + (startCol + matrix.numColumns() + " not " + x.size()));

        // Thin wrapper
        // Slice the Vector
        VectorSlice slice = new VectorSlice(x, startCol, startCol + matrix.numColumns());

        // result vector
        VectorSlice res = new VectorSlice(x.copy(), startRow, startRow + matrix.numRows());

        // multiply
        Vector result = matrix.mult(slice, res);
        //Vector result = matrix.mult(slice, new DenseVector(matrix.numRows()));

        return res.getVector(); //VectorSlicer.upscale(result, x.size(), startRow);
    }

    @Override
    public String toString() {
        return "MatrixBlock{" +
                "startRow=" + startRow +
                ", startCol=" + startCol +
                ", matrix=\n" + matrix +
                '}';
    }
}
