package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;

import java.io.Serializable;

/**
 * Date: 11.02.2015
 * Time: 00:38
 *
 */
public class AdaptedCompRowMatrixSerializer extends Serializer<AdaptedCompRowMatrix> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, AdaptedCompRowMatrix matrix) {
        output.writeInt(matrix.numRows());
        output.writeInt(matrix.numColumns());

        output.writeInt(matrix.getColumnIndices().length);
        output.writeInts(matrix.getColumnIndices(), true);

        output.writeInt(matrix.getRowPointer().length);
        output.writeInts(matrix.getRowPointer(), true);

        output.writeInt(matrix.getData().length);
        output.writeDoubles(matrix.getData());
    }

    @Override
    public AdaptedCompRowMatrix read(Kryo kryo, Input input, Class<AdaptedCompRowMatrix> aClass) {
        int rows = input.readInt();
        int columns = input.readInt();

        int columnIndicesSize = input.readInt();
        int[] columnIndices = input.readInts(columnIndicesSize, true);

        int rowPointerSize = input.readInt();
        int[] rowPointer = input.readInts(rowPointerSize, true);

        int dataSize = input.readInt();
        double[] data = input.readDoubles(dataSize);

        return new AdaptedCompRowMatrix(rows, columns, data, columnIndices, rowPointer);
    }




}
