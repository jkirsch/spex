package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.BitSet;

/**
 * Date: 27.02.2015
 * Time: 10:52
 *
 */
public class ArrayBitSetSerializer extends Serializer<BitSet> {

    @Override
    public void write(Kryo kryo, Output output, BitSet bitSet) {

        long[] longs = bitSet.toLongArray();
        output.writeInt(longs.length);
        output.writeLongs(longs);

    }

    @Override
    public BitSet read(Kryo kryo, Input input, Class<BitSet> type) {
        int size = input.readInt();
        long[] longs = new long[size];
        return BitSet.valueOf(longs);
    }
}
