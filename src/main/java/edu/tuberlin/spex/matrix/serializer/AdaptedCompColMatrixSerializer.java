package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompColMatrix;

import java.io.Serializable;

/**
 * Date: 11.02.2015
 * Time: 00:38
 *
 */
public class AdaptedCompColMatrixSerializer extends Serializer<AdaptedCompColMatrix> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, AdaptedCompColMatrix matrix) {
        output.writeInt(matrix.numRows());
        output.writeInt(matrix.numColumns());

        output.writeInt(matrix.getColumnPointers().length);
        output.writeInts(matrix.getColumnPointers(), true);

        output.writeInt(matrix.getRowIndices().length);
        output.writeInts(matrix.getRowIndices(), true);

        output.writeInt(matrix.getData().length);
        output.writeDoubles(matrix.getData());
    }

    @Override
    public AdaptedCompColMatrix read(Kryo kryo, Input input, Class<AdaptedCompColMatrix> aClass) {
        int rows = input.readInt();
        int columns = input.readInt();

        int columnPointerSizes = input.readInt();
        int[] columnPointer = input.readInts(columnPointerSizes, true);

        int rowIndexSize = input.readInt();
        int[] rowIndex = input.readInts(rowIndexSize, true);

        int dataSize = input.readInt();
        double[] data = input.readDoubles(dataSize);

        return new AdaptedCompColMatrix(rows, columns, data, columnPointer, rowIndex);
    }




}
