package edu.tuberlin.spex.matrix.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;

import java.io.Serializable;

/**
 * Date: 10.02.2015
 * Time: 23:32
 */
public class LinkedSparseMatrixSerializer extends Serializer<LinkedSparseMatrix> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, LinkedSparseMatrix matrix) {
        output.writeInt(matrix.numRows());
        output.writeInt(matrix.numColumns());
        int size = Iterables.size(matrix);
        output.writeInt(size);
        for (MatrixEntry matrixEntry : matrix) {
            output.writeInt(matrixEntry.row());
            output.writeInt(matrixEntry.column());
            output.writeDouble(matrixEntry.get());
        }
    }

    @Override
    public LinkedSparseMatrix read(Kryo kryo, Input input, Class<LinkedSparseMatrix> aClass) {
        int rows = input.readInt();
        int columns = input.readInt();
        int size = input.readInt();

        LinkedSparseMatrix matrix = new LinkedSparseMatrix(rows, columns);

        for (int i = 0; i < size; i++) {
            int row = input.readInt();
            int column = input.readInt();
            double v = input.readDouble();
            matrix.set(row, column, v);
        }

        return matrix;
    }
}
