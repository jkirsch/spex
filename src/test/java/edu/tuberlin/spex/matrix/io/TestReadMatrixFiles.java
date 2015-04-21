package edu.tuberlin.spex.matrix.io;

import com.google.common.collect.Iterables;
import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * 21.04.2015.
 *
 * @author Johannes Kirschnick
 */
@RunWith(Parameterized.class)
public class TestReadMatrixFiles {

    private static final Logger LOG = LoggerFactory.getLogger(TestReadMatrixFiles.class);
    @Parameterized.Parameter
    public ExperimentDatasets.Matrix testMatrix;

    @Parameterized.Parameters(name = "{index}: Dataset {0}")
    public static Collection<ExperimentDatasets.Matrix> data() {
        return Arrays.asList(ExperimentDatasets.Matrix.values());
    }

    @Test
    public void testRead() throws Exception {

        Path path = ExperimentDatasets.get(testMatrix);

        MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> build =
                matrixMarketReader.fromPath(path).withOffset(-1).withInfo(matrixInfo).build();

        GroupReduceOperator<Tuple3<Integer, Integer, Double>, Long> counts = build.reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Double>, Long>() {
            @Override
            public void reduce(Iterable<Tuple3<Integer, Integer, Double>> values, Collector<Long> out) throws Exception {
                long count = 0;
                for (Tuple3<Integer, Integer, Double> ignored : values) {
                    count++;
                }
                out.collect(count);
            }
        });

        Long counted = Iterables.getOnlyElement(counts.collect());

        // print is here to ensure we have some output
        counts.print();


        env.execute();

        System.out.println(matrixInfo);

        assertThat(counted, is(matrixInfo.getValues()));
    }
}
