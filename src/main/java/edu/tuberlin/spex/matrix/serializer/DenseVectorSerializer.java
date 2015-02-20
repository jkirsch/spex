package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import no.uib.cipr.matrix.DenseVector;

import java.io.Serializable;

/**
 * Date: 12.02.2015
 * Time: 00:38
 *
 */
public class DenseVectorSerializer extends Serializer<DenseVector> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, DenseVector vector) {
        double[] data = vector.getData();
        output.writeInt(data.length);
        output.writeDoubles(data);
    }

    @Override
    public DenseVector read(Kryo kryo, Input input, Class<DenseVector> type) {
        int size = input.readInt();
        return new DenseVector(input.readDoubles(size));
    }
}
