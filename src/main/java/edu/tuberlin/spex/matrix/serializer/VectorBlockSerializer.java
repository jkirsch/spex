package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;

import java.io.Serializable;

/**
 * Date: 20.02.2015
 * Time: 10:29
 */
public class VectorBlockSerializer extends Serializer<VectorBlock> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, VectorBlock vectorBlock) {
        double[] data = vectorBlock.getData();
        output.writeInt(vectorBlock.getStartRow());
        output.writeInt(data.length);
        output.writeDoubles(data);
    }

    @Override
    public VectorBlock read(Kryo kryo, Input input, Class<VectorBlock> type) {
        int startRow = input.readInt();
        int size = input.readInt();
        double[] data = input.readDoubles(size);
        VectorBlock block = new VectorBlock(size);
        block.setStartRow(startRow);
        block.setData(data);
        return block;
    }
}
