package edu.tuberlin.spex.matrix.io;

import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
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
 */
@RunWith(Parameterized.class)
public class ReadMatrixFilesTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReadMatrixFilesTest.class);
    @Parameterized.Parameter
    public ExperimentDatasets.Matrix testMatrix;

    @Parameterized.Parameters(name = "{index}: Dataset {0}")
    public static Collection<ExperimentDatasets.Matrix> data() {
        return Arrays.asList(
                ExperimentDatasets.Matrix.values()
        );
    }

    @Test
    public void testRead() throws Exception {

        Path path = ExperimentDatasets.get(testMatrix);

        MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> build =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();

        Long counted = build.count();

        LOG.info(matrixInfo.toString());


        assertThat(counted, is(matrixInfo.getValues()));
    }
}
