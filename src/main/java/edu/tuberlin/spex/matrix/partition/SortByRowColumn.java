package edu.tuberlin.spex.matrix.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Date: 21.02.2015
 * Time: 00:31
 */
public class SortByRowColumn implements KeySelector<Tuple3<Integer, Integer, Double>, Long> {

    @Override
    public Long getKey(Tuple3<Integer, Integer, Double> integerIntegerDoubleTuple3) throws Exception {
        // build a compound sort key
        // row column .. make the rows come first followed by the columns
        // works only with integers currently
        // (x | (long) y << 32)
        return integerIntegerDoubleTuple3.f1 | (long) integerIntegerDoubleTuple3.f0 << 32;
    }
}


