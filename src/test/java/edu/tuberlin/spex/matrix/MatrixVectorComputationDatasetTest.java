package edu.tuberlin.spex.matrix;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.matrix.io.MatrixMarketReader;
import edu.tuberlin.spex.matrix.kernel.TimingMatrixBlockVectorKernel;
import edu.tuberlin.spex.matrix.mapper.AddMatrixElementBlockPartitionMapper;
import edu.tuberlin.spex.matrix.partition.CreateMatrixBlockFromSortedEntriesReducer;
import edu.tuberlin.spex.matrix.partition.CreateMatrixBlockFromSortedEntriesReducer.MatrixType;
import edu.tuberlin.spex.matrix.serializer.LinkedSparseMatrixSerializer;
import edu.tuberlin.spex.matrix.serializer.SparseVectorSerializer;
import edu.tuberlin.spex.utils.Utils;
import edu.tuberlin.spex.utils.VectorBlockHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 22.04.2015.
 */
@RunWith(Parameterized.class)
public class MatrixVectorComputationDatasetTest {

    private static final Logger LOG = LoggerFactory.getLogger(MatrixVectorComputationDatasetTest.class);

    // Keep Track of global stats
    static Map<Experiment, Table<ExperimentDatasets.Matrix, Integer, SummaryStatistics>> stats;
    static Map<Experiment, Table<ExperimentDatasets.Matrix, Integer, Long>> runtime;
    static int[] blocksizes = {1,2,4,8,16,32};
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

        // increase the #of network buffers
        Configuration conf = new Configuration();
        conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 4096);

        env = ExecutionEnvironment.createLocalEnvironment(conf);

        env.registerTypeWithKryoSerializer(LinkedSparseMatrix.class, new LinkedSparseMatrixSerializer());
        env.registerTypeWithKryoSerializer(SparseVector.class, new SparseVectorSerializer());

        stats = Maps.newTreeMap();
        runtime = Maps.newTreeMap();

        for (Experiment experiment : Experiment.values()) {
            stats.put(experiment, TreeBasedTable.<ExperimentDatasets.Matrix, Integer, SummaryStatistics>create());
            runtime.put(experiment, TreeBasedTable.<ExperimentDatasets.Matrix, Integer, Long>create());
        }

    }

    @AfterClass
    public static void tearDown() throws IOException, ArchiveException {

        // Print Kernel Stats
        for (Experiment experiment : Experiment.values()) {
            if (stats.containsKey(experiment)) {
                printStats(experiment, stats.get(experiment));
            }
            if (runtime.containsKey(experiment)) {
                printRuntimes(experiment, runtime.get(experiment));
            }
        }

        System.out.println();

        // Print Matrix Info
        System.out.println("MatrixInfo");
        System.out.printf("%10s\t%6s\t%6s\t%10s\n", "Dataset", "rows", "cols", "entries");

        for (ExperimentDatasets.Matrix matrix : ExperimentDatasets.Matrix.values()) {
            System.out.printf("%10s\t", matrix);
            Path path = ExperimentDatasets.get(matrix);
            final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

            System.out.printf("%6d\t%6d\t%10d\n", matrixInfo.getN(), matrixInfo.getM(), matrixInfo.getValues());

        }


    }

    private static void printStats(Experiment experiment, Table<ExperimentDatasets.Matrix, Integer, SummaryStatistics> stats) {

        if (stats.rowKeySet().size() == 0) {
            return;
        }

        // print some stats
        System.out.println("KernelTimes " + experiment);
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
    }

    private static void printRuntimes(Experiment experiment, Table<ExperimentDatasets.Matrix, Integer, Long> runtime) {

        if (runtime.rowKeySet().size() == 0) {
            return;
        }

        System.out.println("Runtime " + experiment);

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
    }

    private void testEfficientMatrices(MatrixType matrixType, Experiment experiment) throws Exception {

        Path path = ExperimentDatasets.get(testMatrix);
        final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        // Generate the input data
        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = generateInputData(testMatrix, blocksize, matrixType);

        // Generate the vector
        final DataSource<VectorBlock> denseVectorDataSource = env.fromCollection(VectorBlockHelper.createBlocks(matrixInfo.getN(), matrixInfo.getM(), blocksize, 1d));

        // now multiply
        // This bit multiplies
        final ReduceOperator<VectorBlock> multiplicationResults = matrixBlocks.joinWithTiny(denseVectorDataSource).where("startCol").equalTo("startRow")
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

        // just collect the output locally
        List<VectorBlock> resultCollector = new ArrayList<>();
        multiplicationResults.output(new LocalCollectionOutputFormat<>(resultCollector));

        executeAndStoreResults(experiment, env);
    }

    @Test
    public void testMultiplyCompRowMatrix() throws Exception {

        testEfficientMatrices(MatrixType.CompRowMatrix, Experiment.BLOCK_JOIN_COMPRESSEDROW);
    }

    @Test
    @Ignore("Out of memory for too big matrices")
    public void testMultiplyCompDiagMatrix() throws Exception {

        testEfficientMatrices(MatrixType.CompDiagMatrix, Experiment.BLOCK_JOIN_COMPRESSED_DIAGONAL);
    }

    @Test
    public void testMultiplyLinkedSparseMatrix() throws Exception {

        testEfficientMatrices(MatrixType.LinkedSparseMatrix, Experiment.BLOCK_JOIN_LINKEDSPARSEMATRIX);

    }

    @Test
    @Ignore("Serialization Error")
    public void testMultiplyFlexCompRowMatrix() throws Exception {

        testEfficientMatrices(MatrixType.FlexCompRowMatrix, Experiment.BLOCK_JOIN_FLEXCOMPROWMATRIX);

    }

    @Test
    public void testMultiplyBlockKeyValue() throws Exception {


        Path path = ExperimentDatasets.get(testMatrix);
        final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        // Generate the input data
        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = generateInputData(testMatrix, blocksize, MatrixType.CompRowMatrix);

        // Generate the vector
        // Write Temp Data
        File tempVector = writeTempVector(matrixInfo.getM(), 1d);

        final DataSource<Tuple2<Integer, Double>> denseVector = env.readCsvFile(tempVector.getPath()).ignoreInvalidLines().fieldDelimiter(" ").types(Integer.class, Double.class);

        FlatMapOperator<MatrixBlock, Tuple2<Integer, Double>> multiplicationResult = matrixBlocks.flatMap(new RichFlatMapFunction<MatrixBlock, Tuple2<Integer, Double>>() {

            public Stopwatch stopwatch;
            public Histogram histogram;
            double[] index;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                stopwatch = Stopwatch.createStarted();
                histogram = getRuntimeContext().getHistogram(TimingMatrixBlockVectorKernel.TIMINGS_ACCUMULATOR);
                // build index
                List<Tuple2<Integer, Double>> denseVector = getRuntimeContext().getBroadcastVariable("denseVector");

                index = new double[denseVector.size()];

                for (Tuple2<Integer, Double> tuple2 : denseVector) {
                    index[tuple2.f0] = tuple2.f1;
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                stopwatch.stop();
                histogram.add(Utils.safeLongToInt(stopwatch.elapsed(TimeUnit.MICROSECONDS)));
            }

            @Override
            public void flatMap(MatrixBlock matrixBlock, Collector<Tuple2<Integer, Double>> out) throws Exception {
                // get qualifying vector bits

                double[] values = Arrays.copyOfRange(index, matrixBlock.getStartCol(), matrixBlock.getStartCol() + matrixBlock.getMatrix().numColumns());

                VectorBlock mult = (VectorBlock) matrixBlock.mult(new VectorBlock(matrixBlock.startRow, new DenseVector(values)));

                for (VectorEntry vectorEntry : mult) {
                    if (vectorEntry.get() != 0) {
                        out.collect(new Tuple2<>(vectorEntry.index(), vectorEntry.get()));
                    }
                }
            }
        }).withBroadcastSet(denseVector, "denseVector");

        AggregateOperator<Tuple2<Integer, Double>> result = multiplicationResult.groupBy(0).aggregate(Aggregations.SUM, 1);

        // just collect the output locally
        List<Tuple2<Integer, Double>> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        executeAndStoreResults(Experiment.BLOCK_KEY_VALUE, env);

    }

    @Test
    public void testMultiplyKeyValue() throws Exception {

        // we don't need to rerun
        //Assume.assumeThat(blocksize, is(1));

        Path path = ExperimentDatasets.get(testMatrix);
        final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        // Generate the input data
        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);
        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> keyValues =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();

        // Generate the vector
        // Write Temp Data
        File tempVector = writeTempVector(matrixInfo.getM(), 1d);

        DataSource<Tuple2<Integer, Double>> denseVector = env.readCsvFile(tempVector.getPath()).ignoreInvalidLines().fieldDelimiter(" ").types(Integer.class, Double.class);

        // This bit multiplies
        // join Matrix.colid == Vector.rowid
        ReduceOperator<Tuple2<Integer, Double>> reduce = keyValues.join(denseVector).where(1).equalTo(0).with(new RichFlatJoinFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

            public Stopwatch stopwatch;
            public Histogram histogram;

            @Override
            public void open(Configuration parameters) throws Exception {
                // register timing
                stopwatch = Stopwatch.createStarted();
                histogram = getRuntimeContext().getHistogram(TimingMatrixBlockVectorKernel.TIMINGS_ACCUMULATOR);
            }

            @Override
            public void close() throws Exception {
                super.close();
                stopwatch.stop();
                histogram.add(Utils.safeLongToInt(stopwatch.elapsed(TimeUnit.MICROSECONDS)));
            }

            @Override
            public void join(Tuple3<Integer, Integer, Double> matrixEntry, Tuple2<Integer, Double> vectorEntry, Collector<Tuple2<Integer, Double>> out) throws Exception {


                // multiply
                out.collect(new Tuple2<>(matrixEntry.f0, matrixEntry.f2 * vectorEntry.f1));


            }
        }).groupBy(0).reduce(new ReduceFunction<Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> reduce(Tuple2<Integer, Double> value1, Tuple2<Integer, Double> value2) throws Exception {
                value1.f1 += value2.f1;
                return value1;
            }
        });


        // just collect the output locally
        List<Tuple2<Integer, Double>> resultCollector = new ArrayList<>();
        reduce.output(new LocalCollectionOutputFormat<>(resultCollector));

        executeAndStoreResults(Experiment.KEYVALUE, env);


    }

    private void executeAndStoreResults(Experiment experiment, ExecutionEnvironment env) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        JobExecutionResult execute = env.execute();
        stopwatch.stop();

        TreeMap<Integer, Integer> histogram = execute.getAccumulatorResult(TimingMatrixBlockVectorKernel.TIMINGS_ACCUMULATOR);
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        for (Map.Entry<Integer, Integer> integerIntegerEntry : histogram.entrySet()) {
            for (int i = 0; i < integerIntegerEntry.getValue(); i++) {
                summaryStatistics.addValue(integerIntegerEntry.getKey());
            }
        }

        stats.get(experiment).put(testMatrix, blocksize, summaryStatistics);
        runtime.get(experiment).put(testMatrix, blocksize, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> generateInputData(ExperimentDatasets.Matrix testMatrix, int blocks, MatrixType matrixType) throws IOException, ArchiveException {
        Path path = ExperimentDatasets.get(testMatrix);

        final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> matrixEntries =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();


        // partition the results so that we can create matrix blocks
        // add a field which indicates which matrix block a tuple should belong to
        // we need this number later, that's why we add that to each tuple,

        SortedGrouping<Tuple4<Integer, Integer, Double, Long>> matrixWithGroupID = matrixEntries
                .map(new AddMatrixElementBlockPartitionMapper(matrixInfo.getN(), matrixInfo.getM(), blocks))
                .groupBy(3).sortGroup(0, Order.ASCENDING).sortGroup(1, Order.ASCENDING);//.sortGroup(1, Order.ASCENDING);

        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = matrixWithGroupID.
                reduceGroup(new CreateMatrixBlockFromSortedEntriesReducer(matrixInfo.getN(), matrixInfo.getM(), blocks, false, false, matrixType)).name("Build " + matrixType + " Matrix Blocks");

        return matrixBlocks;

    }

    private File writeTempVector(int rows, double init) throws IOException {
        File tempFile = File.createTempFile("tempVector", "csv");
        tempFile.deleteOnExit();

        // write something to file
        BufferedWriter writer = new BufferedWriter(new FileWriterWithEncoding(tempFile, Charsets.UTF_8));

        for (int i = 0; i < rows; i++) {
            writer.write(Joiner.on(" ").join(i, init));
            writer.newLine();
        }

        writer.close();

        return tempFile;
    }

    enum Experiment {
        BLOCK_JOIN_COMPRESSEDROW,
        BLOCK_JOIN_COMPRESSED_DIAGONAL,
        BLOCK_JOIN_LINKEDSPARSEMATRIX,
        BLOCK_JOIN_FLEXCOMPROWMATRIX,
        BLOCK_KEY_VALUE,
        KEYVALUE
    }
}
