package edu.tuberlin.spex.matrix.memory;

import java.nio.ByteBuffer;

/**
 * Created by Johannes on 16.04.2016.
 */
public class MemoryPool {

    private static ByteBuffer buffer;

    public static ByteBuffer get(int colSize, int rowSize, int dataSize) {
        if(buffer == null) {
            buffer = ByteBuffer.allocate(colSize * 4 + rowSize * 4 + dataSize * 8);
        }

        return buffer;
    }
}
