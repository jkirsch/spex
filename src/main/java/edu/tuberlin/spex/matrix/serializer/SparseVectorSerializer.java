package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import no.uib.cipr.matrix.sparse.SparseVector;

import java.io.Serializable;

/**
 * Date: 17.02.2015
 * Time: 18:54
 *
 */
public class SparseVectorSerializer extends Serializer<SparseVector> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, SparseVector vector) {
        double[] data = vector.getData();
        int[] index = vector.getIndex();
        int used = vector.getUsed();

        output.writeInt(used);

        output.writeInt(index.length);
        output.writeInt(data.length);

        output.writeInts(index);


        output.writeDoubles(data);

    }

    @Override
    public SparseVector read(Kryo kryo, Input input, Class<SparseVector> type) {

        int used = input.readInt();
        int indexLength = input.readInt();
        int dataLength = input.readInt();

        return new SparseVector(used, input.readInts(indexLength), input.readDoubles(dataLength), false);
    }
}
