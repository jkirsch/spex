package edu.tuberlin.spex.algorithms;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.MatrixBlockReducer;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.utils.MatrixBlockVectorKernel;
import edu.tuberlin.spex.utils.VectorHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Date: 09.02.2015
 * Time: 23:42
 */
public class FlinkMatrixReader implements Serializable {

    static Logger LOG = LoggerFactory.getLogger(FlinkMatrixReader.class);


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FlinkMatrixReader flinkMatrixReader = new FlinkMatrixReader();

        int n = 325729;
        String path = "webNotreDame.mtx";

        if (args.length > 0) {
            path = args[0];
            n = Ints.tryParse(args[1]);
            Integer degree = Ints.tryParse(args[2]);
            env.setDegreeOfParallelism(degree);
            LOG.info("Analysing {} with {} nodes using parallelism {}", path, n, degree);
        }

        Map<Integer, Stopwatch> timings = new HashMap<>();
        Map<Integer, List<Tuple2<Long, Integer>>> counts = new HashMap<>();

        for (Integer blocksize : Lists.newArrayList(2)){;//1, 2, 4, 8, 16, 32, 64, 128))
            DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path("datasets/" + path), -1, n, true)).name("Edge list");
            timings.put(blocksize, flinkMatrixReader.executePageRank(env, blocksize, input, n));

           // counts.put(blocksize, flinkMatrixReader.getTheNumberOfSetBlocks(env, blocksize, input, n));
        }

        for (Map.Entry<Integer, Stopwatch> integerStopwatchEntry : timings.entrySet()) {
            LOG.info("{} {}", integerStopwatchEntry.getKey(), integerStopwatchEntry.getValue().toString());
        }

        for (Map.Entry<Integer, List<Tuple2<Long, Integer>>> integerListEntry : counts.entrySet()) {
            LOG.info("{} {}", integerListEntry.getKey(), integerListEntry.getValue().toString());
        }

    }

    public static int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                    (l + " cannot be cast to int without changing its value.");
        }
        return (int) l;
    }

    public List<Tuple2<Long, Integer>> getTheNumberOfSetBlocks(ExecutionEnvironment env, final int blocks, DataSet<Tuple3<Integer, Integer, Double>> input, final int n) throws Exception {

        LOG.info("Counting set blocks for size {} ", blocks);

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

    public Stopwatch executePageRank(ExecutionEnvironment env, int blocks, DataSet<Tuple3<Integer, Integer, Double>> input, final int n) throws Exception {
        final boolean transpose = true;

        final double alpha = 0.85;

        AggregateOperator<Tuple2<Integer, Double>> colSumsDataSet = input.<Tuple2<Integer, Double>>project(1, 2).name("Select column id").groupBy(0).aggregate(Aggregations.SUM, 1).name("Calculate ColSums");

        // transform the aggregated sums into a vector which is 1 for all non entries
        GroupReduceOperator<Tuple2<Integer, Double>, BitSet> personalizationVector = colSumsDataSet.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Double>, BitSet>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<BitSet> out) throws Exception {
                //SparseVector personalizationVector = new SparseVector(VectorHelper.ones(n));
                BitSet bitSet = new BitSet(n);
                for (Tuple2<Integer, Double> entry : values) {
                    bitSet.set(entry.f0);
                }
                //for (Tuple2<Integer, Double> entry : values) {
                //    personalizationVector.set(entry.f0, 0);
                //}
                //personalizationVector.compact();
                // revert to get the ones that are dangling
                 bitSet.flip(0, n);

                //out.collect(personalizationVector);
                out.collect(bitSet);
            }
        }).name("Build personalization Vector");

        UnsortedGrouping<Tuple3<Integer, Integer, Double>> tuple3UnsortedGrouping = input.groupBy(new MatrixBlockPartitioner(n, blocks));

        GroupReduceOperator<Tuple3<Integer, Integer, Double>, MatrixBlock> matrixBlocks = tuple3UnsortedGrouping.
                reduceGroup(new MatrixBlockReducer(n, blocks, true, transpose)).withBroadcastSet(colSumsDataSet, "rowSums").name("Build Matrix Blocks");

        // now multiply the matrixblocks with the vector

        DataSource<DenseVector> denseVectorDataSource = env.fromElements(
                VectorHelper.identical(n, 1 / (double) n));

        final IterativeDataSet<DenseVector> iterate = denseVectorDataSource.iterate(100);


        /*MapOperator<Tuple2<MatrixBlock, DenseVector>, DenseVector> matrixBlockVectorKernel = matrixBlocks.crossWithTiny(iterate).map(new MapFunction<Tuple2<MatrixBlock, DenseVector>, DenseVector>() {
            @Override
            public DenseVector map(Tuple2<MatrixBlock, DenseVector> value) throws Exception {
                return (DenseVector) value.f0.mult(alpha, value.f1);
            }
        }).name("MatrixBlockVectorKernel"); */

        MapOperator<Tuple2<DenseVector, BitSet>, Double> personalization = iterate.crossWithTiny(personalizationVector).map(new MapFunction<Tuple2<DenseVector, BitSet>, Double>() {
            @Override
            public Double map(Tuple2<DenseVector, BitSet> value) throws Exception {
                double sum = 0;
                double scale = (1 - alpha) / (double) n;
                BitSet pV = value.f1;
                DenseVector old = value.f0;
                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += old.get(i);
                }
                //for (VectorEntry vectorEntry : value.f1) {
                //    sum += value.f0.get(vectorEntry.index()) * vectorEntry.get();
                //}

                double persAdd = alpha * sum / (double) n;

                return persAdd + scale;
            }
        });

        MapOperator<Tuple2<DenseVector, Double>, DenseVector> reduce = matrixBlocks.map(new MatrixBlockVectorKernel(alpha)).name("MatrixBlockVectorKernel").withBroadcastSet(iterate, "vector").reduce(new ReduceFunction<DenseVector>() {
            //MapOperator<DenseVector, DenseVector> reduce = matrixBlockVectorKernel.reduce(new ReduceFunction<DenseVector>() {
            @Override
            public DenseVector reduce(DenseVector vector, DenseVector t1) throws Exception {
                return (DenseVector) vector.add(t1);
            }
        }).returns("no.uib.cipr.matrix.DenseVector").crossWithTiny(personalization).map(new MapFunction<Tuple2<DenseVector, Double>, DenseVector>() {
            @Override
            public DenseVector map(Tuple2<DenseVector, Double> value) throws Exception {
                double[] data = value.f0.getData();

                for (int i = 0; i < data.length; i++) {
                    data[i] = data[i] + value.f1;
                }

                return value.f0;
            }
        }).returns("java.lang.Double").name("Calculate next vector");

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
