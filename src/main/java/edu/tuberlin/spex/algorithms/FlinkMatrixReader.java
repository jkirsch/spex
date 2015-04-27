package edu.tuberlin.spex.algorithms;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.adapted.DanglingNodeInformation;
import edu.tuberlin.spex.matrix.adapted.DanglingNodeInformationBitSet;
import edu.tuberlin.spex.matrix.adapted.EWAHCompressedBitmapHolder;
import edu.tuberlin.spex.matrix.kernel.NonTimingMatrixBlockVectorKernel;
import edu.tuberlin.spex.matrix.partition.CreateMatrixBlockFromSortedEntriesReducer;
import edu.tuberlin.spex.matrix.partition.CreateMatrixBlockFromSortedEntriesReducer.MatrixType;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.serializer.SerializerRegistry;
import edu.tuberlin.spex.utils.ParallelVectorIterator;
import edu.tuberlin.spex.utils.Utils;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.Vector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;
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

        // load the configuration
        try {
            LOG.info("Loading configuration from conf/");
            GlobalConfiguration.loadConfiguration("conf");
            GlobalConfiguration.getConfiguration();
        }
        catch (Exception e) {
            throw new Exception("Could not load configuration", e);
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        EnvironmentInformation.logEnvironmentInfo(LOG, "FlinkMatrixReader");

        SerializerRegistry.register(env);

        FlinkMatrixReader flinkMatrixReader = new FlinkMatrixReader();

        double alpha = 0.85;
        String path = "datasets/webNotreDame.mtx";

        int[] blockSizes = new int[]{1,2,4,8,16,32,64,128};

        if (args.length > 0) {
            path = args[0];
            Integer degree = Ints.tryParse(args[1]);
            String[] indices = ArrayUtils.subarray(args, 2, args.length);
            blockSizes = new int[indices.length];
            for (int i = 0; i < indices.length; i++) {
                String index = indices[i];
                blockSizes[i] = Ints.tryParse(index);
            }
            env.setParallelism(degree);
        }

        // read the size information
        int n = MatrixReaderInputFormat.getMatrixInfo(path).getN();

        LOG.info("Analysing {} with {} nodes using parallelism {} for the blocksizes {} ", path, n, env.getParallelism(), blockSizes);

        Map<Integer, Stopwatch> timings = new TreeMap<>();
        Map<Integer, List<Tuple2<Long, Integer>>> counts = new HashMap<>();

        for (Integer blocksize : blockSizes) {

            DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path(path), -1, n, true)).name("Edge list");
            TimingResult timingResult = flinkMatrixReader.executePageRank(env, alpha, blocksize, input, n, 100);
            timings.put(blocksize, timingResult.stopwatch);

            // Enable the next line, if you are interested in the distribution of the blocks
            //counts.put(blocksize, flinkMatrixReader.getTheNumberOfSetBlocks(env, blocksize, input, n));
        }

        for (Map.Entry<Integer, Stopwatch> integerStopwatchEntry : timings.entrySet()) {
            LOG.info("{} {}", integerStopwatchEntry.getKey(), integerStopwatchEntry.getValue().toString());
        }

        for (Map.Entry<Integer, List<Tuple2<Long, Integer>>> integerListEntry : counts.entrySet()) {
            LOG.info("{} {}", integerListEntry.getKey(), integerListEntry.getValue().toString());
        }

    }

    private static DanglingNodeInformation createNewDanglingInformation(int adjustedN, int n, int blocks, int f0) {

        int startRow = f0 / (adjustedN / blocks) * (adjustedN / blocks);

        EWAHCompressedBitmap s = new EWAHCompressedBitmap();

        // the last entry is smaller
        int limit = adjustedN / blocks;
        if (startRow + (adjustedN / blocks) > n) {
            limit = (int) (n % Math.ceil(n / (double) blocks));
        }
        s.setSizeInBits(limit, false);

        EWAHCompressedBitmapHolder ewahCompressedBitmap = new EWAHCompressedBitmapHolder(s);

        DanglingNodeInformation danglingNodeInformation = new DanglingNodeInformation();
        danglingNodeInformation.setEwahCompressedBitmapHolder(ewahCompressedBitmap);
        danglingNodeInformation.setStartRow(startRow);

        return danglingNodeInformation;
    }

    private static DanglingNodeInformationBitSet createNewDanglingInformationBitSet(int adjustedN, int n, int blocks, int f0, BitSet bitSet) {

        int startRow = f0 / (adjustedN / blocks) * (adjustedN / blocks);

        // the last entry is smaller
        int limit = adjustedN / blocks;
        if (startRow + (adjustedN / blocks) > n) {
            limit = (int) (n % Math.ceil(n / (double) blocks));
        }

        DanglingNodeInformationBitSet danglingNodeInformationBitSet = new DanglingNodeInformationBitSet();

        BitSet slice = bitSet.get(startRow, startRow + limit);

        danglingNodeInformationBitSet.setStartRow(startRow);
        danglingNodeInformationBitSet.setBitSet(slice);

        return danglingNodeInformationBitSet;
    }

    private static void emitIfNeeded(DanglingNodeInformation danglingNodeInformation, Collector<DanglingNodeInformation> out) {

        danglingNodeInformation.getEwahCompressedBitmapHolder().not();
        if (danglingNodeInformation.getEwahCompressedBitmapHolder().getIntegers().cardinality() > 0) {
            danglingNodeInformation.getEwahCompressedBitmapHolder().trim();


            //System.out.println("HELLO " + danglingNodeInformation.startRow + " -DANGLING-> " + danglingNodeInformation.getEwahCompressedBitmapHolder().getIntegers().toString());
            //bitSet.flip(0, n);

            //out.collect(personalizationVector);
            //out.collect(bitSet);

            out.collect(danglingNodeInformation);
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

    public TimingResult executePageRank(final ExecutionEnvironment env, final double alpha, final int blocks, final DataSet<Tuple3<Integer, Integer, Double>> input, final int n, final int iteration) throws Exception {
        final boolean transpose = true;

        final int adjustedN = n % blocks > 0 ? n + (blocks - n % blocks) : n;

        AggregateOperator<Tuple2<Integer, Double>> colSumsDataSet = input.<Tuple2<Integer, Double>>project(1, 2).name("Select column id").groupBy(0).aggregate(Aggregations.SUM, 1).name("Calculate ColSums");

        final DataSource<VectorBlock> denseVectorDataSource = env.fromParallelCollection(new ParallelVectorIterator(adjustedN, blocks, 1 / (double) n), VectorBlock.class);

        DataSource<Long> blocksCounter = env.fromParallelCollection(new NumberSequenceIterator(0, blocks - 1), Long.class);

        FilterOperator<DanglingNodeInformationBitSet> personalizationVector = blocksCounter.map(new RichMapFunction<Long, DanglingNodeInformationBitSet>() {

            public BitSet bitSet;

            @Override
            public void open(Configuration parameters) throws Exception {
                List<Tuple2<Integer, Double>> aggregatedSums = getRuntimeContext().getBroadcastVariable("columnSums");

                bitSet = new BitSet(n);
                for (Tuple2<Integer, Double> aggregatedSum : aggregatedSums) {
                    bitSet.set(aggregatedSum.f0);
                }
                bitSet.flip(0, n);
            }

            @Override
            public DanglingNodeInformationBitSet map(Long blockID) throws Exception {
                // for each block create a sparse representation of the bitset
                int startRow = Utils.safeLongToInt(blockID * (adjustedN / blocks));
                DanglingNodeInformationBitSet danglingInformationBitSet = createNewDanglingInformationBitSet(adjustedN, n, blocks, startRow , bitSet);

                if (danglingInformationBitSet.getBitSet().cardinality() > 0){

                    return danglingInformationBitSet;
                }
                return null;
            }
        }).withBroadcastSet(colSumsDataSet, "columnSums").filter(new FilterFunction<DanglingNodeInformationBitSet>() {
            @Override
            public boolean filter(DanglingNodeInformationBitSet value) throws Exception {
                return value != null;
            }
        }).returns(DanglingNodeInformationBitSet.class).name("Build personalization Vector");


        // transform the aggregated sums into a vector which is 1 for all non-entries
  /*      GroupReduceOperator<Tuple2<Integer, Integer>, DanglingNodeInformation> personalizationVector = colSumsDataSet.map(new MapFunction<Tuple2<Integer,Double>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple2<Integer, Double> value) throws Exception {
                return new Tuple2<>(value.f0, value.f0 / (adjustedN / blocks));
            }
        }).sortPartition(0, Order.ASCENDING).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, DanglingNodeInformation>() {*/
    /*        @Override
            public void open(Configuration parameters) throws Exception {
                TicToc.tic("Build personalization Vector", "starting");
            }

            @Override
            public void close() throws Exception {
                TicToc.toc("Build personalization Vector", "finished");;
            }*/

        /*    @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<DanglingNodeInformation> out) throws Exception {
                //SparseVector personalizationVector = new SparseVector(VectorHelper.ones(n));
                //BitSet bitSet = new BitSet(n);

                // we need to peek to find the startrow
                PeekingIterator<Tuple2<Integer, Integer>> peekingIterator = Iterators.peekingIterator(values.iterator());

                // we assume that all elements in the group belong to the same partition in the matrix
                // so we can just pick the first one and estimate the block dimensions
                Tuple2<Integer, Integer> peek = peekingIterator.peek();

                DanglingNodeInformation danglingNodeInformation = createNewDanglingInformation(adjustedN, n, blocks, peek.f0);

                int last = Integer.MIN_VALUE;

                while (peekingIterator.hasNext()) {
                    Tuple2<Integer, Integer> entry = peekingIterator.next();
                    //bitSet.set(entry.f0);

                    if (entry.f0 < last) {
                        throw new IllegalStateException("SORT?");
                    }
                    last = entry.f0;

                    // new block
                    int blockSize = adjustedN / blocks;
                    if (entry.f0 >= danglingNodeInformation.startRow + blockSize) {
                        // new block - maybe many


                        emitIfNeeded(danglingNodeInformation, out);
                        danglingNodeInformation = createNewDanglingInformation(adjustedN, n, blocks, danglingNodeInformation.getStartRow() + blockSize);
                        while (entry.f0 >= danglingNodeInformation.startRow + blockSize) {
                            emitIfNeeded(danglingNodeInformation, out);
                            danglingNodeInformation = createNewDanglingInformation(adjustedN, n, blocks, danglingNodeInformation.getStartRow() + blockSize);
                        }

                    }

                    //System.out.println("HELLO " + danglingNodeInformation.startRow + " -- " + entry);
                    if (entry.f0 - danglingNodeInformation.getStartRow() < 0 ) {
                        System.out.println();
                    }
                    danglingNodeInformation.set(entry.f0);
                }
                //for (Tuple2<Integer, Double> entry : values) {
                //    personalizationVector.set(entry.f0, 0);
                //}
                //personalizationVector.compact();
                // revert to get the ones that are dangling
                emitIfNeeded(danglingNodeInformation, out);


            }
        }).returns(DanglingNodeInformation.class).name("Build personalization Vector");*/


        // add a field which indicates which matrix block a tuple should belong to
        // we need this number later, that's why we add that to each tuple,
        SortedGrouping<Tuple4<Integer, Integer, Double, Long>> matrixWithGroupID = input
                .map(new RichMapFunction<Tuple3<Integer, Integer, Double>, Tuple4<Integer, Integer, Double, Long>>() {

                    MatrixBlockPartitioner matrixBlockPartitioner;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        matrixBlockPartitioner = new MatrixBlockPartitioner(adjustedN, blocks);
                    }

                    @Override
                    public Tuple4<Integer, Integer, Double, Long> map(Tuple3<Integer, Integer, Double> input) throws Exception {
                        return new Tuple4<>(input.f0, input.f1, input.f2, matrixBlockPartitioner.getKey(input));
                    }
                }).withForwardedFields("0->0; 1->1; 2->2")
                .groupBy(3).sortGroup(0, Order.ASCENDING).sortGroup(1, Order.ASCENDING);//.sortGroup(1, Order.ASCENDING);


        GroupReduceOperator<Tuple4<Integer, Integer, Double, Long>, MatrixBlock> matrixBlocks = matrixWithGroupID.
                reduceGroup(new CreateMatrixBlockFromSortedEntriesReducer(adjustedN, adjustedN, blocks, true, transpose, MatrixType.CompRowMatrix)).withBroadcastSet(colSumsDataSet, "rowSums").name("Build Matrix Blocks");


        final IterativeDataSet<VectorBlock> iterate = denseVectorDataSource.iterate(iteration);

        MapOperator<Double, Double> personalization = iterate.joinWithTiny(personalizationVector).where("startRow").equalTo("startRow").map(new MapFunction<Tuple2<VectorBlock, DanglingNodeInformationBitSet>, Double>() {
            @Override
            public Double map(Tuple2<VectorBlock, DanglingNodeInformationBitSet> value) throws Exception {
                double sum = 0;
                double scale = (1 - alpha) / (double) n;
                //BitSet complete = value.f1;
                BitSet pV = value.f1.getBitSet();
                //DenseVector old = value.f0;
                final VectorBlock vector = value.f0;

                // below os an alternate implementation which can use EWAHCompressedBitmap

/*                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += old.get(i);
                }*/
                //for (VectorEntry vectorEntry : value.f1) {
                //    sum += value.f0.get(vectorEntry.index()) * vectorEntry.get();
                //}

                // cut out the bitset of interest
                //BitSet pV = complete.get(vector.getStartRow(), vector.getStartRow() + vector.size());

                //EWAHCompressedBitmap ewahCompressedBitmap = new EWAHCompressedBitmap();

                // slice the window

              /*  EWAHCompressedBitmap slice = EWAHCompressedBitmapUtil.buildWindow(vector.getStartRow(), vector.size(), n);

                EWAHCompressedBitmap view = slice.and(complete.getIntegers());*/

                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += vector.get(i);
                }

/*                IntIterator intIterator = view.igetIntegers().intIterator();
                while (intIterator.hasNext()) {
                    sum += vector.get(intIterator.next());
                }*/

/*                for (int i = vector.getStartRow(); i < vector.getStartRow() + vector.size(); i++) {
                     if (complete.get(i)) {
                         sum += vector.get(i - vector.getStartRow());
                     }
                }*/


/*                for (int i = pV.nextSetBit(0); i != -1; i = pV.nextSetBit(i + 1)) {
                    sum += vector.get(i);
                }*/


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


        // This bit multiplies
        final MapOperator<VectorBlock, VectorBlock> nextVector = matrixBlocks.joinWithTiny(iterate).where("startCol").equalTo("startRow")
                // here we compute the Matrix * Vector
                .map(new NonTimingMatrixBlockVectorKernel())
                .returns(VectorBlock.class)
                // we need to group the values together
                .groupBy("startRow").reduce(new ReduceFunction<VectorBlock>() {

                    @Override
                    public VectorBlock reduce(VectorBlock value1, VectorBlock value2) throws Exception {
                        return (VectorBlock) value1.add(value2);
                    }
                }).returns(VectorBlock.class).map(new RichMapFunction<VectorBlock, VectorBlock>() {

                    public double personalAdd;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        personalAdd = Iterables.getOnlyElement(getRuntimeContext().<Double>getBroadcastVariable("pV"));
                    }

                    // This map adds the personalization vector to the data
                    @Override
                    public VectorBlock map(VectorBlock value) throws Exception {
                        double[] data = value.getData();
                        int limit = data.length;
                        // if this is the last block which is "extended" don't change any value below the cutoff
                        if (value.getStartRow() + value.size() > n) {
                            limit = (int) (n % Math.ceil(n / (double) blocks));
                        }
                        for (int i = 0; i < limit; i++) {
                            data[i] = alpha * data[i] + personalAdd;
                        }
                        return value;
                    }
                }).withBroadcastSet(personalization, "pV").returns(VectorBlock.class).name("Calculate next vector");

        // filter out convergent vector blocks
        JoinOperator.EquiJoin<VectorBlock, VectorBlock, Double> deltas = iterate.join(nextVector).where("startRow").equalTo("startRow").with(new FlatJoinFunction<VectorBlock, VectorBlock, Double>() {
            @Override
            public void join(VectorBlock first, VectorBlock second, Collector<Double> out) throws Exception {
                double delta = first.add(second.scale(-1f)).norm(Vector.Norm.One);
                out.collect(delta);
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

        // just collect the output locally
        List<VectorBlock> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        Stopwatch stopwatch = Stopwatch.createStarted();


        String executionPlan = env.getExecutionPlan();


        JobExecutionResult execute = env.execute("Pagerank");


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
