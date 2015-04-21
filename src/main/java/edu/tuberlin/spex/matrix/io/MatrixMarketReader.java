package edu.tuberlin.spex.matrix.io;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;

import java.nio.file.Path;

/**
 * Date: 24.02.2015
 * Time: 11:16
 */
public class MatrixMarketReader {

    final ExecutionEnvironment env;
    boolean transpose = false;
    private DataSource<Tuple3<Integer, Integer, Double>> dataSource;
    private int offset = 0;

    public MatrixMarketReader(ExecutionEnvironment env) {
        this.env = env;
    }


    public MatrixMarketReader fromPath(Path path) {
        return fromPath(path.toFile().getAbsolutePath());
    }

    public MatrixMarketReader fromPath(String path) {
        dataSource = env.readCsvFile(path).ignoreInvalidLines().ignoreComments("%").fieldDelimiter(" ").types(Integer.class, Integer.class, Double.class);
        // check if we need to offset the data
        return this;
    }

    public MatrixMarketReader transpose() {
        Preconditions.checkNotNull(dataSource, "Please set the Path first");
        return this;
    }

    public MatrixMarketReader withOffset(int offset) {
        Preconditions.checkNotNull(dataSource, "Please set the Path first");
        this.offset = offset;
        return this;
    }

    public MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> build() {
        Preconditions.checkNotNull(dataSource, "Please set the Path first");
        // if transpose generate the inverse
        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> map = dataSource.map(new OffsetMapper(offset));
        if (transpose) {
            return map.map(new TransposeMapper());
        }
        return map;
    }

    @FunctionAnnotation.ForwardedFields("f0 -> f1; f1 -> f0; f2 -> f2")
    private static class TransposeMapper implements MapFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> {

        @Override
        public Tuple3<Integer, Integer, Double> map(Tuple3<Integer, Integer, Double> value) throws Exception {
            int tmp = value.f0;
            value.f0 = value.f1;
            value.f1 = tmp;
            return value;
        }
    }

    @FunctionAnnotation.ForwardedFields("f2 -> f2")
    private static class OffsetMapper implements MapFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> {

        final int offset;

        private OffsetMapper(int offset) {
            this.offset = offset;
        }

        @Override
        public Tuple3<Integer, Integer, Double> map(Tuple3<Integer, Integer, Double> value) throws Exception {
            value.f0 -= offset;
            value.f1 -= offset;
            return value;
        }
    }

}
