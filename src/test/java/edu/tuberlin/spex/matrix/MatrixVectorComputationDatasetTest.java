package edu.tuberlin.spex.matrix;

import com.google.common.collect.Iterables;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.matrix.io.MatrixMarketReader;
import edu.tuberlin.spex.matrix.kernel.NonTimingMatrixBlockVectorKernel;
import edu.tuberlin.spex.matrix.mapper.AddMatrixElementBlockPartitionMapper;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.partition.MatrixBlockReducer;
import edu.tuberlin.spex.utils.VectorBlockHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
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
 * 22.04.2015.
 *
 */
@RunWith(Parameterized.class)
public class MatrixVectorComputationDatasetTest {

    private static final Logger LOG = LoggerFactory.getLogger(MatrixVectorComputationDatasetTest.class);
    @Parameterized.Parameter
    public ExperimentDatasets.Matrix testMatrix;

    @Parameterized.Parameters(name = "{index}: Dataset {0}")
    public static Collection<ExperimentDatasets.Matrix> data() {
        return Arrays.asList(
                //ExperimentDatasets.Matrix.lpi_box1
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

        GroupReduceOperator<Tuple3<Integer, Integer, Double>, Long> counts = matrixEntries.reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Double>, Long>() {
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

        LOG.info(matrixInfo.toString());


        assertThat(counted, is(matrixInfo.getValues()));
    }

    @Test
    public void testMultiply() throws Exception {

        Path path = ExperimentDatasets.get(testMatrix);

        final MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> matrixEntries =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();

        final int blocks = 2;

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
                .map(new NonTimingMatrixBlockVectorKernel())
                .returns(VectorBlock.class)
                        // we need to group the values together
                .groupBy("startRow").reduce(new ReduceFunction<VectorBlock>() {

                    @Override
                    public VectorBlock reduce(VectorBlock value1, VectorBlock value2) throws Exception {
                        return (VectorBlock) value1.add(value2);
                    }
                }).returns(VectorBlock.class);

        multiplicationResuls.print();

        env.execute();



    }


}
