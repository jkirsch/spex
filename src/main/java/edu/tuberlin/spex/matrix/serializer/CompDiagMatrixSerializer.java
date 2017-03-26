package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.CompDiagMatrix;

import java.io.Serializable;

/**
 * Created by Johannes on 08.04.2016.
 */
public class CompDiagMatrixSerializer extends Serializer<CompDiagMatrix> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, CompDiagMatrix matrix) {
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
    public CompDiagMatrix read(Kryo kryo, Input input, Class<CompDiagMatrix> type) {
        int rows = input.readInt();
        int columns = input.readInt();
        int size = input.readInt();

        CompDiagMatrix matrix = new CompDiagMatrix(rows, columns);

        for (int i = 0; i < size; i++) {
            int row = input.readInt();
            int column = input.readInt();
            double v = input.readDouble();
            matrix.set(row, column, v);
        }

        return matrix;
    }
}
