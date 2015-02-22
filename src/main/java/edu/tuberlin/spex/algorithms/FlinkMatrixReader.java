package edu.tuberlin.spex.algorithms;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Ints;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.kernel.NonTimingMatrixBlockVectorKernel;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.partition.MatrixBlockReducer;
import edu.tuberlin.spex.utils.ParallelVectorIterator;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.Vector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.EnvironmentInformation;
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

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMatrixReader.class);


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentInformation.logEnvironmentInfo(LOG, "FlinkMatrixReader");

        FlinkMatrixReader flinkMatrixReader = new FlinkMatrixReader();

        int n = 325729;
        double alpha = 0.85;
        String path = "webNotreDame.mtx";

        int[] blockSizes = new int[]{1,2,4,8,16,32,64,128};

        if (args.length > 0) {
            path = args[0];
            n = Ints.tryParse(args[1]);
            Integer degree = Ints.tryParse(args[2]);
            String[] indices = ArrayUtils.subarray(args, 3, args.length);
            blockSizes = new int[indices.length];
            for (int i = 0; i < indices.length; i++) {
                String index = indices[i];
                blockSizes[i] = Ints.tryParse(index);
            }
            env.setDegreeOfParallelism(degree);
        }

        LOG.info("Analysing {} with {} nodes using parallelism {} for the blocksizes {} ", path, n, env.getDegreeOfParallelism(), blockSizes);

        Map<Integer, Stopwatch> timings = new TreeMap<>();
        Map<Integer, List<Tuple2<Long, Integer>>> counts = new HashMap<>();

        for (Integer blocksize : blockSizes) {
            DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path("datasets/" + path), -1, n, true)).name("Edge list");
            TimingResult timingResult = flinkMatrixReader.executePageRank(env, alpha, blocksize, input, n, 100);
            timings.put(blocksize, timingResult.stopwatch);

            //counts.put(blocksize, flinkMatrixReader.getTheNumberOfSetBlocks(env, blocksize, input, n));
        }

        for (Map.Entry<Integer, Stopwatch> integerStopwatchEntry : timings.entrySet()) {
            LOG.info("{} {}", integerStopwatchEntry.getKey(), integerStopwatchEntry.getValue().toString());
        }

        for (Map.Entry<Integer, List<Tuple2<Long, Integer>>> integerListEntry : counts.entrySet()) {
            LOG.info("{} {}", integerListEntry.getKey(), integerListEntry.getValue().toString());
        }

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

    public TimingResult executePageRank(ExecutionEnvironment env, final double alpha, final int blocks, DataSet<Tuple3<Integer, Integer, Double>> input, final int n, final int iteration) throws Exception {
        final boolean transpose = true;

        final int adjustedN = n % blocks > 0 ? n + (blocks - n % blocks) : n;

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


        SortedGrouping<Tuple4<Integer, Integer, Double, Long>> tuple3UnsortedGrouping = input
                .map(new RichMapFunction<Tuple3<Integer, Integer, Double>, Tuple4<Integer, Integer, Double, Long>>() {

                    MatrixBlockPartitioner matrixBlockPartitioner;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        matrixBlockPartitioner = new MatrixBlockPartitioner(adjustedN, blocks);
                    }

                    @Override
                    public Tuple4<Integer, Integer, Double, Long> map(Tuple3<Integer, Integer, Double> input) throws Exception {
                        return new Tuple4<Integer, Integer, Double, Long>(input.f0, input.f1, input.f2, matrixBlockPartitioner.getKey(input));
                    }
                }).withForwardedFields("0->0; 1->1; 2->2")
                .groupBy(3).sortGroup(0, Order.ASCENDING).sortGroup(1, Order.ASCENDING);//.sortGroup(1, Order.ASCENDING);

        //sortGroup(new SortByRowColumn(), Order.ASCENDING);

        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = tuple3UnsortedGrouping.
                reduceGroup(new MatrixBlockReducer(adjustedN, blocks, true, transpose)).withBroadcastSet(colSumsDataSet, "rowSums").name("Build Matrix Blocks");

        // now multiply the matrixblocks with the vector

        //DataSource<DenseVector> denseVectorDataSource = env.fromElements(
        //        VectorHelper.identical(n, 1 / (double) n));


        //final DataSource<VectorBlock> denseVectorDataSource = env.fromCollection(VectorBlockHelper.createBlocks(adjustedN, blocks, 1 / (double) n));

        final DataSource<VectorBlock> denseVectorDataSource = env.fromParallelCollection(new ParallelVectorIterator(adjustedN, blocks, 1 / (double) n), VectorBlock.class);

        final IterativeDataSet<VectorBlock> iterate = denseVectorDataSource.iterate(iteration);


        /*MapOperator<Tuple2<MatrixBlock, DenseVector>, DenseVector> matrixBlockVectorKernel = matrixBlocks.crossWithTiny(iterate).map(new MapFunction<Tuple2<MatrixBlock, DenseVector>, DenseVector>() {
            @Override
            public DenseVector map(Tuple2<MatrixBlock, DenseVector> value) throws Exception {
                return (DenseVector) value.f0.mult(alpha, value.f1);
            }
        }).name("MatrixBlockVectorKernel"); */

        MapOperator<Double, Double> personalization = iterate.crossWithTiny(personalizationVector).map(new MapFunction<Tuple2<VectorBlock, BitSet>, Double>() {
            @Override
            public Double map(Tuple2<VectorBlock, BitSet> value) throws Exception {
                double sum = 0;
                double scale = (1 - alpha) / (double) n;
                //BitSet complete = value.f1;
                BitSet complete = value.f1;
                //DenseVector old = value.f0;
                VectorBlock vector = value.f0;
/*                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += old.get(i);
                }*/
                //for (VectorEntry vectorEntry : value.f1) {
                //    sum += value.f0.get(vectorEntry.index()) * vectorEntry.get();
                //}

                // cut out the bitset of interest
                BitSet pV = complete.get(vector.getStartRow(), vector.getStartRow() + vector.size());

                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += vector.get(i);
                }


                //double persAdd = alpha * sum / (double) n;
                //return persAdd + scale;
                return sum;
            }
        }).reduce(new ReduceFunction<Double>() {
            @Override
            public Double reduce(Double value1, Double value2) throws Exception {
                return value1 + value2;
            }
        }).map(new MapFunction<Double, Double>() {
            @Override
            public Double map(Double sum) throws Exception {
                double scale = (1 - alpha) / (double) n;
                return alpha * sum / (double) n + scale;
            }
        }).name("Build Dangling Nodes");

        //MapOperator<Tuple2<DenseVector, Double>, DenseVector> reduce = matrixBlocks.crossWithTiny(iterate).map(new MatrixBlockVectorKernelCross(alpha)).name("MatrixBlockVectorKernel")

        final MapOperator<Tuple2<VectorBlock, Double>, VectorBlock> reduce = matrixBlocks.joinWithTiny(iterate).where("startCol").equalTo("startRow")
                .map(new NonTimingMatrixBlockVectorKernel())
                .groupBy("startRow").reduce(new ReduceFunction<VectorBlock>() {

                    @Override
                    public VectorBlock reduce(VectorBlock value1, VectorBlock value2) throws Exception {
                        //   if (value1.getVector() == null) {
                        //       return value2;
                        //   }
                        //   if (value2.getVector() == null) {
                        //       return value1;
                        //   }
                        //   if(value1 == null || value2 == null) {
                        //       System.out.println(value1);
                        //       System.out.println(value2);
                        //   }
                        //    if(value1.size() != value2.size()) {
                        //        System.out.println();
                        //    }

                        //System.out.println(value1.getStartRow());
                        return (VectorBlock) value1.add(value2);
                        //return new VectorBlock(value1.getStartRow(), (VectorBlock) value1.add(value2));
                    }
                }).cross(personalization).map(new MapFunction<Tuple2<VectorBlock, Double>, VectorBlock>() {
                    @Override
                    public VectorBlock map(Tuple2<VectorBlock, Double> value) throws Exception {
                        double[] data = value.f0.getData();

                        // if this is the last block which is "extended" don't change any value below the cutoff
                        int limit = data.length;
                        if (value.f0.getStartRow() + value.f0.size() > n) {
                            limit = (int) (n % Math.ceil(n / (double) blocks));
                        }
                        for (int i = 0; i < limit; i++) {
                            data[i] = alpha * data[i] + value.f1;
                        }

                        return value.f0;//new VectorBlock(value.f0.startRow, value.f0);
                    }
                }).name("Calculate next vector");

       /* MapOperator<Tuple2<DenseVector, Double>, DenseVector> reduce = matrixBlocks.map(new MatrixBlockVectorKernel(alpha)).name("MatrixBlockVectorKernel").withBroadcastSet(iterate, "vector")
                .reduce(new ReduceFunction<DenseVector>() {
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
        }).name("Calculate next vector");              */

        DataSet<VectorBlock> result = iterate.closeWith(reduce);

        List<VectorBlock> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        String executionPlan = env.getExecutionPlan();

        System.out.println(executionPlan);

        Stopwatch stopwatch = new Stopwatch().start();
        JobExecutionResult execute = env.execute("Pagerank");


        //TreeMap<Integer, Integer> histogram = execute.getAccumulatorResult(TimingMatrixBlockVectorKernel.TIMINGS_ACCUMULATOR);
        //Long reduceCounts = execute.getAccumulatorResult("reduce");

        stopwatch.stop();

        System.out.println(stopwatch);
        double sum = 0;
        for (VectorBlock vectorBlock : resultCollector) {
            //The last vector block contains entries we don't care about ..
            //FIX by subtracting from the overall sum Math.abs(e.get()); for all wrong entries - if any
            sum += vectorBlock.norm(Vector.Norm.One);
            if (vectorBlock.getStartRow() == adjustedN - adjustedN / blocks) {
                int limit = adjustedN > n ? vectorBlock.size() - (blocks - n % blocks) : vectorBlock.size();
                double adjusted = 0;
                for (int i = limit; i < vectorBlock.size(); i++) {
                    adjusted += Math.abs(vectorBlock.get(i));
                }
                LOG.info("Adjusted by {} ", adjusted);
                sum -= adjusted;
                // get the last elements in this block

            }
        }

        System.out.println(sum);
        //System.out.println(p_k1);

        Preconditions.checkArgument((Math.abs(sum - 1) - 0.001) <= 0.0, String.format("Overall sum not within bounds %1.5f n=%d b=%d", sum, n, blocks));

        return new TimingResult(resultCollector, stopwatch);

    }

    public static class TimingResult {
        List<VectorBlock> vectorBlocks;
        Stopwatch stopwatch;

        public TimingResult(List<VectorBlock> vectorBlocks, Stopwatch stopwatch) {
            this.vectorBlocks = vectorBlocks;
            this.stopwatch = stopwatch;
        }
    }

}
