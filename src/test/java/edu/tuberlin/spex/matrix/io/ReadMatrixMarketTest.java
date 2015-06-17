package edu.tuberlin.spex.matrix.io;

import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * 22.04.2015.
 */
@RunWith(Parameterized.class)
public class ReadMatrixMarketTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReadMatrixMarketTest.class);
    private static ExecutionEnvironment env;

    @Parameterized.Parameter
    public ExperimentDatasets.Matrix testMatrix;

    @Parameterized.Parameters(name = "{index}: Dataset {0}")
    public static ExperimentDatasets.Matrix[] data() {
        return ExperimentDatasets.Matrix.values();
    }

    @BeforeClass
    public static void setup() throws Exception {
        env = ExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void testRead() throws Exception {

        Path path = ExperimentDatasets.get(testMatrix);

        MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);


        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> matrixEntries =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();

        // partition the results so that we can create matrix blocks
        // add a field which indicates which matrix block a tuple should belong to
        // we need this number later, that's why we add that to each tuple,
        SortedGrouping<Tuple4<Integer, Integer, Double, Long>> matrixWithGroupID = matrixEntries
                .map(new RichMapFunction<Tuple3<Integer, Integer, Double>, Tuple4<Integer, Integer, Double, Long>>() {

                    MatrixBlockPartitioner matrixBlockPartitioner;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        matrixBlockPartitioner = new MatrixBlockPartitioner(1, 1);
                    }

                    @Override
                    public Tuple4<Integer, Integer, Double, Long> map(Tuple3<Integer, Integer, Double> input) throws Exception {
                        return new Tuple4<>(input.f0, input.f1, input.f2, matrixBlockPartitioner.getKey(input));
                    }
                }).withForwardedFields("0->0; 1->1; 2->2")
                .groupBy(3).sortGroup(0, Order.ASCENDING).sortGroup(1, Order.ASCENDING);//.sortGroup(1, Order.ASCENDING);

        Long counted = matrixEntries.count();



        LOG.info(matrixInfo.toString());


        assertThat(counted, is(matrixInfo.getValues()));
    }
}
