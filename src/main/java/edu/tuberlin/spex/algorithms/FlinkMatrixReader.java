package edu.tuberlin.spex.algorithms;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.MatrixBlockReducer;
import edu.tuberlin.spex.utils.MatrixBlockVectorKernel;
import edu.tuberlin.spex.utils.VectorHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 09.02.2015
 * Time: 23:42
 */
public class FlinkMatrixReader implements Serializable {

    Logger LOG = LoggerFactory.getLogger(FlinkMatrixReader.class);


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FlinkMatrixReader flinkMatrixReader = new FlinkMatrixReader();

        Map<Integer, Stopwatch> timings = new HashMap<>();
        Map<Integer, List<Tuple2<Long, Integer>>> counts = new HashMap<>();

        for (Integer blocksize : Lists.newArrayList(1, 2, 4, 8, 16, 32, 64, 128)) {
            DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path("datasets/webNotreDame.mtx"), -1, 325729, true)).name("Edge list");
            timings.put(blocksize, flinkMatrixReader.executePageRank(env, blocksize, input, 325729));

            //counts.put(blocksize, flinkMatrixReader.getTheNumberOfSetBlocks(env, blocksize, input, 325729));
        }

        for (Map.Entry<Integer, Stopwatch> integerStopwatchEntry : timings.entrySet()) {
            System.out.printf("%2d %s\n", integerStopwatchEntry.getKey(), integerStopwatchEntry.getValue().toString());
        }

        for (Map.Entry<Integer, List<Tuple2<Long, Integer>>> integerListEntry : counts.entrySet()) {
            System.out.printf("%2d %s\n", integerListEntry.getKey(), integerListEntry.getValue().toString());
        }

    }

    public static int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                    (l + " cannot be cast to int without changing its value.");
        }
        return (int) l;
    }

    public List<Tuple2<Long, Integer>> getTheNumberOfSetBlocks(ExecutionEnvironment env, final int blocks, DataSource<Tuple3<Integer, Integer, Double>> input, final int n) throws Exception {

        LOG.info("Counting set blocks for size {} ",blocks);
        env.setDegreeOfParallelism(1);

        AggregateOperator<Tuple2<Long, Integer>> aggregate = input.map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple2<Long, Integer>>() {
            MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blocks);

            @Override
            public Tuple2<Long, Integer> map(Tuple3<Integer, Integer, Double> value) throws Exception {
                return new Tuple2<>(matrixBlockPartitioner.getKey(value), 1);
            }
        }).groupBy(0).aggregate(Aggregations.SUM, 1);

        List<Tuple2<Long, Integer>> resultCollector = new ArrayList<>();
        aggregate.output(new LocalCollectionOutputFormat<>(resultCollector));

        env.execute();

        return resultCollector;
    }

    public Stopwatch executePageRank(ExecutionEnvironment env, int blocks, DataSource<Tuple3<Integer, Integer, Double>> input, final int n) throws Exception {
        final boolean transpose = true;

        final double alpha = 0.85;

        AggregateOperator<Tuple2<Integer, Double>> colSumsDataSet = input.<Tuple2<Integer, Double>>project(1, 2).name("Select column id").groupBy(0).aggregate(Aggregations.SUM, 1).name("Calculate ColSums");

        // transform the aggregated sums into a vector which is 1 for all non entries
        GroupReduceOperator<Tuple2<Integer, Double>, SparseVector> personalizationVector = colSumsDataSet.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Double>, SparseVector>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<SparseVector> out) throws Exception {
                SparseVector personalizationVector = new SparseVector(VectorHelper.ones(n));
                /*BitSet bitSet = new BitSet(n);
                for (Tuple2<Integer, Double> entry : values) {
                    bitSet.set(entry.f0);
                }                                            */
                for (Tuple2<Integer, Double> entry : values) {
                    personalizationVector.set(entry.f0, 0);
                }
                personalizationVector.compact();
                // revert to get the ones that are dangling
                // bitSet.flip(0, n);

                //out.collect(personalizationVector);
                out.collect(personalizationVector);
            }
        }).name("Build personalization Vector");

        UnsortedGrouping<Tuple3<Integer, Integer, Double>> tuple3UnsortedGrouping = input.groupBy(new MatrixBlockPartitioner(n, blocks));

        GroupReduceOperator<Tuple3<Integer, Integer, Double>, MatrixBlock> matrixBlocks = tuple3UnsortedGrouping.
                reduceGroup(new MatrixBlockReducer(n, blocks, true, transpose)).withBroadcastSet(colSumsDataSet, "rowSums").name("Build Matrix Blocks");

        // now multiply the matrixblocks with the vector

        DataSource<DenseVector> denseVectorDataSource = env.fromElements(
                VectorHelper.identical(n, 1 / (double) n));

        final IterativeDataSet<DenseVector> iterate = denseVectorDataSource.iterate(100);

        MapOperator<DenseVector, DenseVector> reduce = matrixBlocks.map(new MatrixBlockVectorKernel(alpha)).name("MatrixBlockVectorKernel").withBroadcastSet(iterate, "vector")
                .reduce(new ReduceFunction<DenseVector>() {
                    @Override
                    public DenseVector reduce(DenseVector vector, DenseVector t1) throws Exception {
                        return (DenseVector) vector.add(t1);
                    }
                })
                .map(new RichMapFunction<DenseVector, DenseVector>() {

                    public DenseVector old;
                    public SparseVector pV;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pV = (SparseVector)
                                Iterables.getOnlyElement(getRuntimeContext().getBroadcastVariable("personalizationVector"));

                        old = (DenseVector)
                                Iterables.getOnlyElement(getRuntimeContext().getBroadcastVariable("vector"));
                    }

                    @Override
                    public DenseVector map(DenseVector value) throws Exception {

                        double[] data = value.getData();
                        double scale = (1 - alpha) / (double) n;

                        // computer personalization add - for dangling nodes
                        double sum = 0;

                       /* for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                            sum += old.get(i);
                        } */
                        for (VectorEntry vectorEntry : pV) {
                            sum += old.get(vectorEntry.index()) * vectorEntry.get();
                        }

                        double persAdd = alpha * sum / (double) n;

                        double adder = persAdd + scale;

                        for (int i = 0; i < data.length; i++) {
                            data[i] = data[i] + adder;
                        }
                        return value;
                    }
                }).withBroadcastSet(personalizationVector, "personalizationVector").withBroadcastSet(iterate, "vector").name("Calculate next vector");

        DataSet<DenseVector> result = iterate.closeWith(reduce);

        List<DenseVector> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        String executionPlan = env.getExecutionPlan();

        System.out.println(executionPlan);

        Stopwatch stopwatch = new Stopwatch().start();
        JobExecutionResult execute = env.execute("Pagerank");
        stopwatch.stop();

        System.out.println(stopwatch);

        DenseVector p_k1 = Iterables.getOnlyElement(resultCollector);
        System.out.println(p_k1.norm(Vector.Norm.One));
        //System.out.println(p_k1);

        Preconditions.checkArgument((Math.abs((p_k1.norm(Vector.Norm.One) - 1)) - 0.00001) <= 0.0);

        return stopwatch;

    }

}
