package edu.tuberlin.spex.matrix.mapper;

import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

@FunctionAnnotation.ForwardedFields("0->0; 1->1; 2->2")
public class AddMatrixElementBlockPartitionMapper extends RichMapFunction<Tuple3<Integer, Integer, Double>, Tuple4<Integer, Integer, Double, Long>> {

    final int n;
    final int m;
    final int blocks;
    MatrixBlockPartitioner matrixBlockPartitioner;

    public AddMatrixElementBlockPartitionMapper(int n, int m, int blocks) {
        this.n = n;
        this.m = m;
        this.blocks = blocks;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        matrixBlockPartitioner = new MatrixBlockPartitioner(n, m, blocks);
    }

    @Override
    public Tuple4<Integer, Integer, Double, Long> map(Tuple3<Integer, Integer, Double> input) throws Exception {
        return new Tuple4<>(input.f0, input.f1, input.f2, matrixBlockPartitioner.getKey(input));
    }
}