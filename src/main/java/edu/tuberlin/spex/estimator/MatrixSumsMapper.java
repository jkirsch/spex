package edu.tuberlin.spex.estimator;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * Date: 12.02.2015
 * Time: 16:14
 */
public class MatrixSumsMapper extends RichMapFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> {

    public static final String ROW_SUMS = "rowSum";
    public static final String COL_SUMS = "colSum";

    private Histogram rowSums;
    private Histogram colSums;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // register the accumulator instances
        rowSums = getRuntimeContext().getHistogram(ROW_SUMS);
        colSums = getRuntimeContext().getHistogram(COL_SUMS);
    }

    @Override
    public Tuple3<Integer, Integer, Double> map(Tuple3<Integer, Integer, Double> input) throws Exception {
        rowSums.add(input.f0);
        colSums.add(input.f1);
        return input;
    }
}
