package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import no.uib.cipr.matrix.AbstractMatrix;

import java.io.Serializable;

/**
 * Date: 20.02.2015
 * Time: 10:29
 *
 */
public class MatrixBlockSerializer extends Serializer<MatrixBlock> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, MatrixBlock matrixBlock) {

        output.writeInt(matrixBlock.getStartRow());
        output.writeInt(matrixBlock.getStartCol());
        kryo.writeClassAndObject(output, matrixBlock.getMatrix());
    }

    @Override
    public MatrixBlock read(Kryo kryo, Input input, Class<MatrixBlock> aClass) {
        int rows = input.readInt();
        int columns = input.readInt();
        AbstractMatrix matrix = (AbstractMatrix) kryo.readClassAndObject(input);
        return new MatrixBlock(rows, columns, matrix);
    }
}
