package edu.tuberlin.spex.matrix;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.matrix.io.MatrixMarketReader;
import edu.tuberlin.spex.matrix.kernel.TimingMatrixBlockVectorKernel;
import edu.tuberlin.spex.matrix.mapper.AddMatrixElementBlockPartitionMapper;
import edu.tuberlin.spex.matrix.partition.MatrixBlockReducer;
import edu.tuberlin.spex.utils.VectorBlockHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * 22.04.2015.
 */
@RunWith(Parameterized.class)
public class MatrixVectorComputationDatasetTest {

    private static final Logger LOG = LoggerFactory.getLogger(MatrixVectorComputationDatasetTest.class);
    // Keep Track of global stats
    static Table<ExperimentDatasets.Matrix, Integer, SummaryStatistics> stats;
    static Table<ExperimentDatasets.Matrix, Integer, Long> runtime;
    static int[] blocksizes = {1, 2, 4, 8, 16, 32};
    private static ExecutionEnvironment env;
    @Parameterized.Parameter
    public ExperimentDatasets.Matrix testMatrix;
    @Parameterized.Parameter(value = 1)
    public int blocksize;

    @Parameterized.Parameters(name = "{index}: Dataset {0} {1}")
    public static List<Object[]> data() {

        // sort the array
        Arrays.sort(blocksizes);

        // build the cross product of all test with a couple different blocksizes
        List<Object[]> parameters = Lists.newArrayList();

        for (ExperimentDatasets.Matrix matrix : ExperimentDatasets.Matrix.values()) {
            for (int blocksize : blocksizes) {
                parameters.add(new Object[]{matrix, blocksize});
            }
        }
        return parameters;
    }

    @BeforeClass
    public static void setup() throws Exception {
        env = ExecutionEnvironment.getExecutionEnvironment();
        stats = TreeBasedTable.create();
        runtime = TreeBasedTable.create();
    }

    @AfterClass
    public static void tearDown() throws IOException, ArchiveException {

        // print some stats
        System.out.println("KernelTimes");
        System.out.printf("%10s", "Dataset");
        for (int blocksize : blocksizes) {
            System.out.printf("\t%10d", blocksize);
        }
        System.out.println();


        for (ExperimentDatasets.Matrix matrix : stats.rowKeySet()) {
            System.out.printf("%10s\t", matrix);
            for (int blocksize : blocksizes) {
                if (stats.contains(matrix, blocksize)) {
                    SummaryStatistics summaryStatistics = stats.get(matrix, blocksize);
                    System.out.printf("%10.0f\t", summaryStatistics.getSum());
                } else {
                    System.out.printf("%10s\t", "");
                }
            }
            System.out.println();
        }

        System.out.println();

        System.out.println("Runtime");

        // print some stats
        System.out.printf("%10s", "Dataset");
        for (int blocksize : blocksizes) {
            System.out.printf("\t%10d", blocksize);
        }

        System.out.println();


        for (ExperimentDatasets.Matrix matrix : runtime.rowKeySet()) {
            System.out.printf("%10s\t", matrix);
            for (int blocksize : blocksizes) {
                if (runtime.contains(matrix, blocksize)) {
                    Long runLength = runtime.get(matrix, blocksize);
                    System.out.printf("%10d\t", runLength);
                } else {
                    System.out.printf("%10s\t", "");
                }
            }
            System.out.println();
        }

        System.out.println();
        System.out.println("MatrixInfo");

        for (ExperimentDatasets.Matrix matrix : ExperimentDatasets.Matrix.values()) {
            System.out.printf("%10s\t", matrix);
            Path path = ExperimentDatasets.get(matrix);
            final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

            System.out.printf("%5d\t%5d\t%10d\n", matrixInfo.getN(), matrixInfo.getM(), matrixInfo.getValues());

        }


    }

    @Test
    public void testMultiply() throws Exception {

        Path path = ExperimentDatasets.get(testMatrix);

        final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> matrixEntries =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();

        final int blocks = blocksize;

        final DataSource<VectorBlock> denseVectorDataSource = env.fromCollection(VectorBlockHelper.createBlocks(matrixInfo.getN(), matrixInfo.getM(), blocks, 1d));

        // partition the results so that we can create matrix blocks
        // add a field which indicates which matrix block a tuple should belong to
        // we need this number later, that's why we add that to each tuple,

        SortedGrouping<Tuple4<Integer, Integer, Double, Long>> matrixWithGroupID = matrixEntries
                .map(new AddMatrixElementBlockPartitionMapper(matrixInfo.getN(), matrixInfo.getM(), blocks))
                .groupBy(3).sortGroup(0, Order.ASCENDING).sortGroup(1, Order.ASCENDING);//.sortGroup(1, Order.ASCENDING);

        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = matrixWithGroupID.
                reduceGroup(new MatrixBlockReducer(matrixInfo.getN(), matrixInfo.getM(), blocks, false, false)).name("Build Matrix Blocks");

        // now multiply

        // This bit multiplies
        final ReduceOperator<VectorBlock> multiplicationResuls = matrixBlocks.joinWithTiny(denseVectorDataSource).where("startCol").equalTo("startRow")
                // here we compute the Matrix * Vector
                .map(new TimingMatrixBlockVectorKernel())
                .returns(VectorBlock.class)
                        // we need to group the values together
                .groupBy("startRow").reduce(new ReduceFunction<VectorBlock>() {

                    @Override
                    public VectorBlock reduce(VectorBlock value1, VectorBlock value2) throws Exception {
                        return (VectorBlock) value1.add(value2);
                    }
                }).returns(VectorBlock.class);

        multiplicationResuls.print();

        Stopwatch stopwatch = Stopwatch.createStarted();
        JobExecutionResult execute = env.execute();
        stopwatch.stop();


        TreeMap<Integer, Integer> histogram = execute.getAccumulatorResult(TimingMatrixBlockVectorKernel.TIMINGS_ACCUMULATOR);
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        for (Integer integer : histogram.keySet()) {
            summaryStatistics.addValue(integer);
        }

        stats.put(testMatrix, blocks, summaryStatistics);
        runtime.put(testMatrix, blocks, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }


}
