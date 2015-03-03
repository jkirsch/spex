package edu.tuberlin.spex.algorithms.domain;

import com.google.common.base.Preconditions;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;

import java.io.Serializable;

/**
 * Date: 04.02.2015
 * Time: 20:21
 *
 */
public class MatrixBlock implements Serializable {

    public int startRow;
    public int startCol;

    public AdaptedCompRowMatrix matrix;

    public MatrixBlock() {
    }

    /**
     * The matrix should be normalized and start at 0.
     *
     * @param startRow
     * @param startCol
     * @param matrix
     */
    public MatrixBlock(int startRow, int startCol, AdaptedCompRowMatrix matrix) {
        this.startRow = startRow;
        this.startCol = startCol;
        this.matrix = matrix;
    }

    public Matrix getMatrix() {
        return matrix;
    }

    public Vector multRealigned(Vector x) {
        return multRealigned(1, x);
    }

    public Vector mult(double alpha, Vector x) {

        return matrix.mult(alpha, x, x.copy());

    }

    public Vector mult(VectorBlock x) {

        return matrix.mult(x, new VectorBlock(x.getStartRow(),new DenseVector(x.size())));

    }


    /**
     * <code>y = A*x</code>
     *
     * @param x
     *            Vector of size <code>A.numColumns()</code>
     * @return y
     */
    public Vector multRealigned(double alpha, Vector x) {

        Preconditions.checkArgument(startCol + matrix.numColumns()  <= x.size(),
                "Vector dimension needs to be at least " + (startCol + matrix.numColumns() + " not " + x.size()));

        // Thin wrapper
        // Slice the Vector
        VectorSlice slice = new VectorSlice(x, startCol, startCol + matrix.numColumns());

        // result vector
        VectorSlice res = new VectorSlice(x.copy(), startRow, startRow + matrix.numRows());

        // multiply
        Vector result = matrix.mult(alpha, slice, res);
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

    /**
     * Simple Matrix generator
     * @param startRow
     * @param startCol
     * @param elements elements are a list of row,col,value .. row,col,value ...
     * @return
     */
    public static MatrixBlock generateBlock(int startRow, int startCol, int rows, int columns, int ... elements) {

        Preconditions.checkArgument(elements.length % 3 == 0, "Always 3 elements make a cell");
        Preconditions.checkArgument(rows < 10,
                "This is using the DenseFormat which is memory bound and does not support row > 10");
        Preconditions.checkArgument(columns < 10,
                "This is using the DenseFormat which is memory bound and does not support columns > 10");

        Matrix matrix = new DenseMatrix(rows, columns);

        for (int i = 0; i < elements.length; i+=3) {
            matrix.set(elements[i], elements[i + 1], elements[i + 2]);
        }

        return new MatrixBlock(startRow, startCol, new AdaptedCompRowMatrix(matrix));
    }

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public int getStartCol() {
        return startCol;
    }

    public void setStartCol(int startCol) {
        this.startCol = startCol;
    }

    public void setMatrix(AdaptedCompRowMatrix matrix) {
        this.matrix = matrix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatrixBlock that = (MatrixBlock) o;

        if (startCol != that.startCol) return false;
        if (startRow != that.startRow) return false;
        if (!matrix.equals(that.matrix)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = startRow;
        result = 31 * result + startCol;
        result = 31 * result + matrix.hashCode();
        return result;
    }
}
