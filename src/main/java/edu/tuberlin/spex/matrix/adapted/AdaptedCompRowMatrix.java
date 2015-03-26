/*
 * Copyright (C) 2003-2006 Bjørn-Ove Heimsund
 * 
 * This file is part of MTJ.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or (at your
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package edu.tuberlin.spex.matrix.adapted;

import com.google.common.base.Preconditions;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.io.MatrixInfo;
import no.uib.cipr.matrix.io.MatrixSize;
import no.uib.cipr.matrix.io.MatrixVectorReader;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.*;

/**
 * Compressed row storage (CRS) matrix
 */
public class AdaptedCompRowMatrix extends AbstractMatrix implements Value {

    ByteBuffer byteBuffer;

    /**
     * Matrix data
     */
    DoubleBuffer data;

    /**
     * Column indices. These are kept sorted within each row.
     */
    IntBuffer columnIndex;

    /**
     * Indices to the start of each row
     */
    IntBuffer rowPointer;

    public AdaptedCompRowMatrix() {
        super(0,0);
    }

    /**
     * Constructor for CompRowMatrix
     * 
     * @param r
     *            Reader to get sparse matrix from
     */
    public AdaptedCompRowMatrix(MatrixVectorReader r) throws IOException {
        // Start with a zero-sized matrix
        super(0, 0);

        // Get matrix information. Use the header if present, else just assume
        // that the matrix stores real numbers without any symmetry
        MatrixInfo info = null;
        if (r.hasInfo())
            info = r.readMatrixInfo();
        else
            info = new MatrixInfo(true, MatrixInfo.MatrixField.Real,
                    MatrixInfo.MatrixSymmetry.General);

        // Check that the matrix is in an acceptable format
        if (info.isPattern())
            throw new UnsupportedOperationException(
                    "Pattern matrices are not supported");
        if (info.isDense())
            throw new UnsupportedOperationException(
                    "Dense matrices are not supported");
        if (info.isComplex())
            throw new UnsupportedOperationException(
                    "Complex matrices are not supported");

        // Resize the matrix to correct size
        MatrixSize size = r.readMatrixSize(info);
        numRows = size.numRows();
        numColumns = size.numColumns();

        // Start reading entries
        int numEntries = size.numEntries();
        int[] row = new int[numEntries];
        int[] column = new int[numEntries];
        double[] entry = new double[numEntries];
        r.readCoordinate(row, column, entry);

        // Shift the indices from 1 based to 0 based
        r.add(-1, row);
        r.add(-1, column);

        // Find the number of entries on each row
        List<Set<Integer>> rnz = new ArrayList<Set<Integer>>(numRows);
        for (int i = 0; i < numRows; ++i)
            rnz.add(new HashSet<Integer>());

        for (int i = 0; i < numEntries; ++i)
            rnz.get(row[i]).add(column[i]);

        // Allocate some more in case of symmetry
        if (info.isSymmetric() || info.isSkewSymmetric())
            for (int i = 0; i < numEntries; ++i)
                if (row[i] != column[i])
                    rnz.get(column[i]).add(row[i]);

        int[][] nz = new int[numRows][];
        for (int i = 0; i < numRows; ++i) {
            nz[i] = new int[rnz.get(i).size()];
            int j = 0;
            for (Integer colind : rnz.get(i))
                nz[i][j++] = colind;
        }

        // Create the sparse matrix structure
        construct(nz);

        // Insert the entries
        for (int i = 0; i < size.numEntries(); ++i)
            set(row[i], column[i], entry[i]);

        // Put in extra entries from symmetry or skew symmetry
        if (info.isSymmetric())
            for (int i = 0; i < numEntries; ++i) {
                if (row[i] != column[i])
                    set(column[i], row[i], entry[i]);
            }
        else if (info.isSkewSymmetric())
            for (int i = 0; i < numEntries; ++i) {
                if (row[i] != column[i])
                    set(column[i], row[i], -entry[i]);
            }
    }

    public AdaptedCompRowMatrix(int numRows, int numColumns, double[] data, int[] columnIndex, int[] rowPointer) {
        super(numRows, numColumns);
        this.data = DoubleBuffer.wrap(data);
        this.columnIndex = IntBuffer.wrap(columnIndex);
        this.rowPointer = IntBuffer.wrap(rowPointer);
    }

    /**
     * Constructor for CompRowMatrix
     *
     * @param numRows
     *            Number of rows
     * @param numColumns
     *            Number of columns
     * @param nz
     *            The nonzero column indices on each row
     */
    public AdaptedCompRowMatrix(int numRows, int numColumns, int[][] nz) {
        super(numRows, numColumns);
        construct(nz);
    }

    /**
     * Constructor for CompRowMatrix
     *
     * @param numRows
     *            Number of rows
     * @param numColumns
     *            Number of columns
     * @param nz
     *            The nonzero column indices on each row
     */
    public AdaptedCompRowMatrix(int numRows, int numColumns, int[][] nz, double[] data) {
        super(numRows, numColumns);
        construct(nz, data);
    }

    /**
     * Constructor for CompRowMatrix
     *
     * @param A
     *            Copies from this matrix
     * @param deep
     *            True if the copy is to be deep. If it is a shallow copy,
     *            <code>A</code> must be a <code>CompRowMatrix</code>
     */
    public AdaptedCompRowMatrix(Matrix A, boolean deep) {
        super(A);
        construct(A, deep);
    }

    /**
     * Constructor for CompRowMatrix
     *
     * @param A
     *            Copies from this matrix. The copy will be deep
     */
    public AdaptedCompRowMatrix(Matrix A) {
        this(A, true);
    }

    public static AdaptedCompRowMatrix buildFromSortedIterator(Iterator<Tuple3<Integer, Integer, Double>> values, int rows, int cols) {
        List<Double> data = new ArrayList<>();

        int lastRow = -1;

        // The nonzero column indices on each row
        int[][] nnz = new int[rows][0];
        ArrayList<Object> colIndices = new ArrayList<>();

        while (values.hasNext()) {
            Tuple3<Integer, Integer, Double> value = values.next();
            data.add(value.f2);

            if(value.f0 > lastRow && lastRow > -1) {
                Preconditions.checkArgument(lastRow < value.f0, "We need a sorted list");
                // flush last row
               nnz[lastRow] = ArrayUtils.toPrimitive(colIndices.toArray(new Integer[colIndices.size()]));
               colIndices.clear();
            }

            colIndices.add(value.f1);
            lastRow = value.f0;
        }

        nnz[lastRow] = ArrayUtils.toPrimitive(colIndices.toArray(new Integer[colIndices.size()]));

        AdaptedCompRowMatrix constructed = new AdaptedCompRowMatrix(rows, cols, nnz, ArrayUtils.toPrimitive(data.toArray(new Double[data.size()])));

        //constructed.data = ArrayUtils.toPrimitive(data.toArray(new Double[data.size()]));

        return constructed;

    }

    private void construct(int[][] nz) {
        int nnz = 0;
        for (int i = 0; i < nz.length; ++i)
            nnz += nz[i].length;

        int rowPointer[] = new int[numRows + 1];
        int columnIndex[] = new int[nnz];
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

    private void construct(int[][] nz, double[] data) {
        int nnz = 0;
        for (int i = 0; i < nz.length; ++i)
            nnz += nz[i].length;

        int rowPointer[] = new int[numRows + 1];
        int columnIndex[] = new int[nnz];
        this.data = DoubleBuffer.wrap(data);

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

    private void construct(Matrix A, boolean deep) {
        if (deep) {
            if (A instanceof AdaptedCompRowMatrix) {
                AdaptedCompRowMatrix Ac = (AdaptedCompRowMatrix) A;
                data = Ac.data.duplicate();//new double[Ac.data.capacity()];

                this.columnIndex = Ac.columnIndex.duplicate();
                this.rowPointer = Ac.rowPointer.duplicate();;
            } else {

                List<Set<Integer>> rnz = new ArrayList<>(numRows);
                for (int i = 0; i < numRows; ++i)
                    rnz.add(new HashSet<Integer>());

                for (MatrixEntry e : A)
                    rnz.get(e.row()).add(e.column());

                int[][] nz = new int[numRows][];
                for (int i = 0; i < numRows; ++i) {
                    nz[i] = new int[rnz.get(i).size()];
                    int j = 0;
                    for (Integer colind : rnz.get(i))
                        nz[i][j++] = colind;
                }

                construct(nz);
                set(A);

            }
        } else {
            AdaptedCompRowMatrix Ac = (AdaptedCompRowMatrix) A;
            columnIndex = Ac.getColumnIndices();
            rowPointer = Ac.getRowPointers();
            data = Ac.data;//getData();
        }
    }

    /**
     * Returns the column indices
     */
    public IntBuffer getColumnIndices() {
        return columnIndex;
    }

    /**
     * Returns the row pointers
     */
    public IntBuffer getRowPointers() {
        return rowPointer;
    }

    /**
     * Returns the internal data storage
     */
   /* public double[] getData() {
        return data;
    }*/

    @Override
    public Matrix mult(Matrix B, Matrix C) {
        checkMultAdd(B, C);
        C.zero();

        // optimised a little bit to avoid zeros in rows, but not to
        // exploit sparsity of matrix B
        for (int i = 0; i < numRows; ++i) {
            for (int j = 0; j < C.numColumns(); ++j) {
                double dot = 0;
                for (int k = rowPointer.array()[i]; k < rowPointer.array()[i + 1]; ++k) {
                    dot += data.get(k) * B.get(columnIndex.array()[k], j);
                }
                if (dot != 0) {
                    C.set(i, j, dot);
                }
            }
        }
        return C;
    }

    @Override
    public Vector mult(Vector x, Vector y) {
        // check dimensions
        checkMultAdd(x, y);
        // can't assume this, unfortunately
        y.zero();

        if (x instanceof DenseVector) {
            // DenseVector optimisations
            double[] xd = ((DenseVector) x).getData();
            for (int i = 0; i < numRows; ++i) {
                double dot = 0;
                for (int j = rowPointer.array()[i]; j < rowPointer.array()[i + 1]; j++) {
                    dot += data.get(j) * xd[columnIndex.array()[j]];
                }
                if (dot != 0) {
                    y.set(i, dot);
                }
            }
            return y;
        }
        // use sparsity of matrix (not vector), as get(,) is slow
        // TODO: additional optimisations for mult(ISparseVector, Vector)
        // note that this would require Sparse BLAS, e.g. BLAS_DUSDOT(,,,,)
        // @see http://www.netlib.org/blas/blast-forum/chapter3.pdf
        for (int i = 0; i < numRows; ++i) {
            double dot = 0;
            for (int j = rowPointer.get(i); j < rowPointer.get(i + 1); j++) {
                dot += data.get(j) * x.get(columnIndex.get(j));
            }
            y.set(i, dot);
        }
        return y;
    }

    @Override
    public Vector multAdd(double alpha, Vector x, Vector y) {
        if (!(x instanceof DenseVector) || !(y instanceof DenseVector))
            return super.multAdd(alpha, x, y);

        checkMultAdd(x, y);

        double[] xd = ((DenseVector) x).getData();
        double[] yd = ((DenseVector) y).getData();

        for (int i = 0; i < numRows; ++i) {
            double dot = 0;
            for (int j = rowPointer.array()[i]; j < rowPointer.array()[i + 1]; ++j)
                dot += data.get(j) * xd[columnIndex.array()[j]];
            yd[i] += alpha * dot;
        }

        return y;
    }

    @Override
    public Vector transMult(Vector x, Vector y) {
        if (!(x instanceof DenseVector) || !(y instanceof DenseVector))
            return super.transMult(x, y);

        checkTransMultAdd(x, y);

        double[] xd = ((DenseVector) x).getData();
        double[] yd = ((DenseVector) y).getData();

        y.zero();

        for (int i = 0; i < numRows; ++i)
            for (int j = rowPointer.array()[i]; j < rowPointer.array()[i + 1]; ++j)
                yd[columnIndex.array()[j]] += data.get(j) * xd[i];

        return y;
    }

    @Override
    public Vector transMultAdd(double alpha, Vector x, Vector y) {
        if (!(x instanceof DenseVector) || !(y instanceof DenseVector))
            return super.transMultAdd(alpha, x, y);

        checkTransMultAdd(x, y);

        double[] xd = ((DenseVector) x).getData();
        double[] yd = ((DenseVector) y).getData();

        // y = 1/alpha * y
        y.scale(1. / alpha);

        // y = A'x + y
        for (int i = 0; i < numRows; ++i)
            for (int j = rowPointer.array()[i]; j < rowPointer.array()[i + 1]; ++j)
                yd[columnIndex.array()[j]] += data.get(j) * xd[i];

        // y = alpha*y = alpha*A'x + y
        return y.scale(alpha);
    }

    @Override
    public void set(int row, int column, double value) {
        check(row, column);

        int index = getIndex(row, column);

        data.put(index,value);
    }

    @Override
    public void add(int row, int column, double value) {
        check(row, column);

        int index = getIndex(row, column);
        throw new UnsupportedOperationException("Sorry");

        //data[index] += value;
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

    /**
     * Finds the insertion index
     */
    private int getIndex(int row, int column) {
        int i = Arrays.binarySearch(columnIndex.array(), rowPointer.array()[row], rowPointer.array()[row + 1], column);

        if (i != -1 && columnIndex.array()[i] == column)
            return i;
        else
            throw new IndexOutOfBoundsException("Entry (" + (row + 1) + ", "
                    + (column + 1) + ") is not in the matrix structure");
    }

    @Override
    public AdaptedCompRowMatrix copy() {

        throw new UnsupportedOperationException("Sorry");

        //return new AdaptedCompRowMatrix(this);
    }

    @Override
    public Iterator<MatrixEntry> iterator() {
        return new CompRowMatrixIterator();
    }

    @Override
    public AdaptedCompRowMatrix zero() {
        data = DoubleBuffer.wrap(new double[data.limit()]);
        return this;
    }

    @Override
    public Matrix set(Matrix B) {

        if (!(B instanceof AdaptedCompRowMatrix))
            return super.set(B);

        checkSize(B);

        AdaptedCompRowMatrix Bc = (AdaptedCompRowMatrix) B;

        // Reallocate matrix structure, if necessary
        if (Bc.columnIndex.limit() != columnIndex.limit()
                || Bc.rowPointer.limit() != rowPointer.limit()) {
            data = DoubleBuffer.wrap(new double[Bc.data.capacity()]);
            columnIndex = IntBuffer.wrap(new int[Bc.columnIndex.limit()]);
            rowPointer = IntBuffer.wrap(new int[Bc.rowPointer.limit()]);
        }

        System.arraycopy(Bc.data.array(), 0, data.array(), 0, data.capacity());
        System.arraycopy(Bc.columnIndex.array(), 0, columnIndex.array(), 0, columnIndex.limit());
        System.arraycopy(Bc.rowPointer.array(), 0, rowPointer.array(), 0, rowPointer.limit());

        return this;
    }

    public IntBuffer getRowPointer() {
        return rowPointer;
    }

    @Override
    public void write(DataOutputView out) throws IOException {

        out.writeInt(numRows);
        out.writeInt(numColumns);

        // write array sizes
        out.writeInt(columnIndex.limit());
        out.writeInt(rowPointer.limit());
        out.writeInt(data.capacity());

        if(byteBuffer != null) {
            out.write(byteBuffer.array());
        } else {

            for (int i = 0; i < columnIndex.limit(); i++) {
                out.writeInt(columnIndex.get(i));
            }

            for (int i = 0; i < rowPointer.limit(); i++) {
                out.writeInt(rowPointer.get(i));
            }

            for (int i = 0; i < data.capacity(); i++) {
                out.writeDouble(data.get(i));
            }
        }

    }

    @Override
    public void read(DataInputView in) throws IOException {

        numRows = in.readInt();
        numColumns = in.readInt();

        // read array sizes
        int colSize = in.readInt();
        int rowSize = in.readInt();
        int dataSize = in.readInt();


        //columnIndex = new int[colSize];
        //rowPointer = new int[rowSize];
        //data = new double[dataSize];


        byteBuffer = ByteBuffer.allocate(colSize * 4 + rowSize * 4 + dataSize * 8);
        in.read(byteBuffer.array());

        //IntBuffer colBuf = IntBuffer.wrap(columnIndex);
        //IntBuffer rowBuf = IntBuffer.wrap(rowPointer);

        columnIndex = (IntBuffer) byteBuffer.asIntBuffer().limit(colSize);
        //colBuf.put(src);

        // advance the buffer
        byteBuffer.position(colSize * 4);

        rowPointer = (IntBuffer) byteBuffer.asIntBuffer().limit(rowSize);
        //rowBuf.put(src2);

        byteBuffer.position(colSize * 4 + rowSize * 4);

        // the remaining
        data = byteBuffer.asDoubleBuffer();
        //DoubleBuffer.wrap(data).put(byteBuffer.asDoubleBuffer());


        /*for (int i = 0; i < colSize; i++) {
            columnIndex[i] = in.readInt();
        }*/


       /* for (int i = 0; i < rowSize; i++) {
            rowPointer[i] = in.readInt();
        }*/


/*        for (int i = 0; i < dataSize; i++) {
            data[i] = in.readDouble();

        }*/

    }

    /**
     * Iterator over a compressed row matrix
     */
    private class CompRowMatrixIterator implements Iterator<MatrixEntry> {

        private int row, cursor;

        private CompRowMatrixEntry entry = new CompRowMatrixEntry();

        public CompRowMatrixIterator() {
            // Find first non-empty row
            nextNonEmptyRow();
        }

        /**
         * Locates the first non-empty row, starting at the current. After the
         * new row has been found, the cursor is also updated
         */
        private void nextNonEmptyRow() {
            while (row < numRows() && rowPointer.get(row) == rowPointer.get(row + 1))
                row++;
            cursor = rowPointer.get(row);
        }

        public boolean hasNext() {
            return cursor < data.capacity();
        }

        public MatrixEntry next() {
            entry.update(row, cursor);

            // Next position is in the same row
            if (cursor < rowPointer.get(row + 1) - 1)
                cursor++;

            // Next position is at the following (non-empty) row
            else {
                row++;
                nextNonEmptyRow();
            }

            return entry;
        }

        public void remove() {
            entry.set(0);
        }

    }

    /**
     * Entry of a compressed row matrix
     */
    private class CompRowMatrixEntry implements MatrixEntry {

        private int row, cursor;

        /**
         * Updates the entry
         */
        public void update(int row, int cursor) {
            this.row = row;
            this.cursor = cursor;
        }

        public int row() {
            return row;
        }

        public int column() {
            return columnIndex.get(cursor);
        }

        public double get() {
            return data.get(cursor);
        }

        public void set(double value) {
            throw new UnsupportedOperationException("Sorry");

//            data[cursor] = value;
        }
    }
}
