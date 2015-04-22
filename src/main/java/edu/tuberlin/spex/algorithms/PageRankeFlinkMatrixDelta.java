package edu.tuberlin.spex.algorithms;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.kernel.NonTimingMatrixBlockVectorKernel;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.partition.MatrixBlockReducer;
import edu.tuberlin.spex.utils.VectorBlockHelper;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;

/**
 * Date: 22.02.2015
 * Time: 22:08
 *
 */
public class PageRankeFlinkMatrixDelta {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMatrixReader.class);

    public FlinkMatrixReader.TimingResult executePageRank(ExecutionEnvironment env, final double alpha, final int blocks, DataSet<Tuple3<Integer, Integer, Double>> input, final int n, final int iteration) throws Exception {
        final boolean transpose = true;

        final int adjustedN = n % blocks > 0 ? n + (blocks - n % blocks) : n;

        AggregateOperator<Tuple2<Integer, Double>> colSumsDataSet = input.<Tuple2<Integer, Double>>project(1, 2).name("Select column id").groupBy(0).aggregate(Aggregations.SUM, 1).name("Calculate ColSums");

        // transform the aggregated sums into a vector which is 1 for all non entries
        GroupReduceOperator<Tuple2<Integer, Double>, BitSet> personalizationVector = colSumsDataSet.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Double>, BitSet>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<BitSet> out) throws Exception {
                BitSet bitSet = new BitSet(n);
                for (Tuple2<Integer, Double> entry : values) {
                    bitSet.set(entry.f0);
                }
                bitSet.flip(0, n);
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


        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = tuple3UnsortedGrouping.
                reduceGroup(new MatrixBlockReducer(adjustedN, adjustedN, blocks, true, transpose)).withBroadcastSet(colSumsDataSet, "rowSums").name("Build Matrix Blocks");


        DataSource<Tuple2<Integer, VectorBlock>> denseVectorDataSource = env.fromCollection(VectorBlockHelper.createTupleBlocks(adjustedN, blocks, 1 / (double) n));

        DeltaIteration<Tuple2<Integer, VectorBlock>, Tuple2<Integer, VectorBlock>> iterate =
                denseVectorDataSource.iterateDelta(denseVectorDataSource, 100, 0);

        MapOperator<Double, Double> personalization = iterate.getInitialWorkset().crossWithTiny(personalizationVector).map(new MapFunction<Tuple2<Tuple2<Integer, VectorBlock>, BitSet>, Double>() {
            @Override
            public Double map(Tuple2<Tuple2<Integer, VectorBlock>, BitSet> value) throws Exception {

                double sum = 0;
                double scale = (1 - alpha) / (double) n;
                //BitSet complete = value.f1;
                BitSet complete = value.f1;
                //DenseVector old = value.f0;
                VectorBlock vector = value.f0.f1;

                // cut out the bitset of interest
                BitSet pV = complete.get(vector.getStartRow(), vector.getStartRow() + vector.size());

                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += vector.get(i);
                }

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

        //MapOperator<Tuple2<DenseVector, Double>, DenseVector> nextVector = matrixBlocks.crossWithTiny(iterate).map(new MatrixBlockVectorKernelCross(alpha)).name("MatrixBlockVectorKernel")

        final MapOperator<Tuple2<VectorBlock, Double>, Tuple2<Integer, VectorBlock>> nextVector = matrixBlocks.joinWithTiny(iterate.getInitialWorkset()).where("startCol").equalTo("startRow")
                .map(new MapFunction<Tuple2<MatrixBlock, Tuple2<Integer, VectorBlock>>, Tuple2<MatrixBlock, VectorBlock>>() {
                    @Override
                    public Tuple2<MatrixBlock, VectorBlock> map(Tuple2<MatrixBlock, Tuple2<Integer, VectorBlock>> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1.f1);
                    }
                })
                .map(new NonTimingMatrixBlockVectorKernel())
                .groupBy("startRow").reduce(new ReduceFunction<VectorBlock>() {

                    @Override
                    public VectorBlock reduce(VectorBlock value1, VectorBlock value2) throws Exception {
                        return (VectorBlock) value1.add(value2);
                    }
                }).cross(personalization).map(new MapFunction<Tuple2<VectorBlock, Double>, Tuple2<Integer, VectorBlock>>() {
                    @Override
                    public Tuple2<Integer, VectorBlock> map(Tuple2<VectorBlock, Double> value) throws Exception {
                        double[] data = value.f0.getData();

                        // if this is the last block which is "extended" don't change any value below the cutoff
                        int limit = data.length;
                        if (value.f0.getStartRow() + value.f0.size() > n) {
                            limit = (int) (n % Math.ceil(n / (double) blocks));
                        }
                        for (int i = 0; i < limit; i++) {
                            data[i] = alpha * data[i] + value.f1;
                        }

                        return new Tuple2<>(value.f0.getStartRow(), value.f0);//new VectorBlock(value.f0.startRow, value.f0);
                    }
                }).name("Calculate next vector");


        // filter out convergent vector blocks
    /*    JoinOperator.EquiJoin<Tuple2<Integer, VectorBlock>, VectorBlock, Double> deltas = iterate.getWorkset().join(nextVector).where("startRow").equalTo("startRow")
                .with(new FlatJoinFunction<Tuple2<Integer, VectorBlock>, Tuple2<Integer, VectorBlock>, Double>() {
                    @Override
                    public void join(Tuple2<Integer, VectorBlock> first, Tuple2<Integer, VectorBlock> second, Collector<Double> out) throws Exception {
                        double delta = first.f1.add(second.scale(-1f)).norm(Vector.Norm.One);
                        //if (!DoubleMath.fuzzyEquals(delta, 0, 0.00001)) {
                        out.collect(delta);
                        //}
                    }
                });

        FilterOperator<Double> stopping = deltas.reduce(new ReduceFunction<Double>() {
            @Override
            public Double reduce(Double value1, Double value2) throws Exception {
                return value1 + value2;
            }
        }).filter(new FilterFunction<Double>() {
            @Override
            public boolean filter(Double delta) throws Exception {
                return !DoubleMath.fuzzyEquals(delta, 0, 0.00001);
            }
        });

        DataSet<VectorBlock> result = iterate.closeWith(nextVector, stopping);

        List<VectorBlock> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        String executionPlan = env.getExecutionPlan();

        System.out.println(executionPlan);

        Stopwatch stopwatch = new Stopwatch().start();
        JobExecutionResult execute = env.execute("Pagerank");


        //TreeMap<Integer, Integer> histogram = execute.getAccumulatorResult(TimingMatrixBlockVectorKernel.TIMINGS_ACCUMULATOR);
        //Long reduceCounts = execute.getAccumulatorResult("nextVector");

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

        return new FlinkMatrixReader.TimingResult(resultCollector, stopwatch);*/
        return null;

    }
}
