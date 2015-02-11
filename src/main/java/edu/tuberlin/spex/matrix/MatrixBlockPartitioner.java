package edu.tuberlin.spex.matrix;

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Date: 09.02.2015
 * Time: 22:56
 *
 */
public class MatrixBlockPartitioner implements KeySelector<Tuple3<Integer, Integer, Float>, Long> {

    /** Size the of dataset. **/
    final long n;

    /** Number of blocks. */
    final int blocks;

    public MatrixBlockPartitioner(long n, int blocks) {

        Preconditions.checkArgument(blocks % 2 == 0, "Blocks needs to be a factor of two");

        this.n = n;
        this.blocks = blocks;
    }


    @Override
    public Long getKey(Tuple3<Integer, Integer, Float> value) throws Exception {

        long blockSize = n / blocks;
        long row = value.f0 / blockSize;
        long column = value.f1 / blockSize;

        return row * blocks / 2 + column;
    }
}
