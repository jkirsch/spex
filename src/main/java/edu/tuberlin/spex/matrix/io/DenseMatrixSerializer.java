package edu.tuberlin.spex.matrix.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import no.uib.cipr.matrix.DenseMatrix;

import java.io.Serializable;

/**
 * Date: 10.02.2015
 * Time: 23:24
 */
public class DenseMatrixSerializer extends Serializer<DenseMatrix> implements Serializable {


    @Override
    public void write(Kryo kryo, Output output, DenseMatrix matrixEntries) {
        output.writeInt(matrixEntries.numRows());
        output.writeInt(matrixEntries.numColumns());
        output.writeDoubles(matrixEntries.getData());
    }

    @Override
    public DenseMatrix read(Kryo kryo, Input input, Class<DenseMatrix> aClass) {
        int rows = input.readInt();
        int columns = input.readInt();
        return new DenseMatrix(rows, columns, input.readDoubles(rows * columns), false);
    }

}
