package edu.tuberlin.spex.matrix.io;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

public class MatrixMarketReaderTest {

    @Test
    public void testRead() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> build =
                matrixMarketReader.fromPath("datasets/webNotreDame.mtx").withOffsee(-1).transpose().build();


        build.print();

        env.execute();


    }
}