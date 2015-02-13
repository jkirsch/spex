package edu.tuberlin.spex.algorithms;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.MatrixBlockReducer;
import edu.tuberlin.spex.utils.MatrixBlockVectorKernel;
import edu.tuberlin.spex.utils.SerializerRegistry;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;

public class FlinkMatrixReaderTest {

    @Test
    public void testCompute() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setDegreeOfParallelism(1);

        //final int n = 2;
        //final int blocks = 2;

        final double alpha = 0.80;
        final int n = 8;
        final int blocks = 1;

        boolean tranpose = true;

        DataSource<Tuple3<Integer, Integer, Double>> input = env.fromCollection(createKnownMatrix(n, tranpose));

        SerializerRegistry.register(env);


        AggregateOperator<Tuple2<Integer, Double>> colSumsDataSet = input.<Tuple2<Integer, Double>>project(1, 2).groupBy(0).aggregate(Aggregations.SUM, 1).name("Calculate ColSums");

        // transform the aggregated sums into a vector which is 1 for all non entries
        GroupReduceOperator<Tuple2<Integer, Double>, SparseVector> personalizationVector = colSumsDataSet.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Double>, SparseVector>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<SparseVector> out) throws Exception {
                SparseVector personalizationVector = new SparseVector(VectorHelper.ones(n));
                for (Tuple entry : values) {
                    personalizationVector.set((Integer) entry.getField(0), 0);
                }
                personalizationVector.compact();
                out.collect(personalizationVector);
            }
        });

        UnsortedGrouping<Tuple3<Integer, Integer, Double>> tuple3UnsortedGrouping = input.groupBy(new MatrixBlockPartitioner(n, blocks));

        GroupReduceOperator<Tuple3<Integer, Integer, Double>, MatrixBlock> matrixBlocks = tuple3UnsortedGrouping.
                reduceGroup(new MatrixBlockReducer(n, blocks, true, tranpose)).withBroadcastSet(colSumsDataSet, "rowSums");

        matrixBlocks.print();

        // now multiply the matrixblocks with the vector

        DataSource<DenseVector> denseVectorDataSource = env.fromElements(
                VectorHelper.identical(n,  1 / (double) n));

        final IterativeDataSet<DenseVector> iterate = denseVectorDataSource.iterate(100);

        MapOperator<DenseVector, DenseVector> reduce = matrixBlocks.map(new MatrixBlockVectorKernel(alpha)).withBroadcastSet(iterate, "vector")
                .reduce(new ReduceFunction<DenseVector>() {
                    @Override
                    public DenseVector reduce(DenseVector vector, DenseVector t1) throws Exception {
                        return (DenseVector) vector.add(t1);
                    }
                })
                .map(new RichMapFunction<DenseVector, DenseVector>() {
                    @Override
                    public DenseVector map(DenseVector value) throws Exception {

                        SparseVector pV = (SparseVector)
                                Iterables.getOnlyElement(getRuntimeContext().getBroadcastVariable("personalizationVector"));

                        DenseVector old = (DenseVector)
                                Iterables.getOnlyElement(getRuntimeContext().getBroadcastVariable("vector"));

                        double[] data = value.getData();
                        double scale = (1 - alpha) / (double) n;

                        // computer personalization add
                        double sum = 0;
                        for (VectorEntry vectorEntry : pV) {
                            sum += old.get(vectorEntry.index()) * vectorEntry.get();
                        }

                        double persAdd = alpha * sum / (double) n;


                        for (int i = 0; i < data.length; i++) {
                            data[i] = data[i] + persAdd + scale;
                        }
                        return value;
                    }
                }).withBroadcastSet(personalizationVector, "personalizationVector").withBroadcastSet(iterate, "vector");

        DataSet<DenseVector> result = iterate.closeWith(reduce);

        List<DenseVector> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        String executionPlan = env.getExecutionPlan();

        System.out.println(executionPlan);

        JobExecutionResult execute = env.execute();

        result.print();
        DenseVector p_k1 = Iterables.getOnlyElement(resultCollector);
        System.out.println(p_k1.norm(Vector.Norm.One));
        System.out.println(p_k1);

        System.out.println(execute.getAllAccumulatorResults());

        Assert.assertThat(p_k1.norm(Vector.Norm.One), closeTo(1, 0.00001));
        Assert.assertThat(p_k1.get(0), closeTo(0.0675, 0.0001));
        Assert.assertThat(p_k1.get(1), closeTo(0.0701, 0.0001));
        Assert.assertThat(p_k1.get(2), closeTo(0.0934, 0.0001));
        Assert.assertThat(p_k1.get(3), closeTo(0.0768, 0.0001));

    }

    private List<Tuple3<Integer, Integer, Double>> createMatrix(int n) {

        List<Tuple3<Integer, Integer, Double>> list = Lists.newArrayList();

        for (int i = 0; i < n; i++) {
            list.add(new Tuple3<>(i, i, 1d));
        }

        return list;

    }

    @Test
    public void testKnown() throws Exception {

        createKnownMatrix(8, true);

    }

    private List<Tuple3<Integer, Integer, Double>> createKnownMatrix(int n, boolean transpose) {

        // recreate the matrix from
        // http://de.wikipedia.org/wiki/Google-Matrix#cite_note-1

        List<Tuple3<Integer, Integer, Double>> list = Lists.newArrayList();

        int[][] pos = {{1, 3}, {2, 1}, {2, 6}, {3, 4}, {3, 5}, {4, 2}, {4, 7}, {7, 8}, {8, 7}};

        // Matrix is just for visual inspection
        DenseMatrix m = new DenseMatrix(n, n);

        for (int[] positions : pos) {
            int row = positions[0] - 1;
            int col = positions[1] - 1;

            m.set(row, col, 1d);
            if(!transpose) {
                list.add(new Tuple3<>(row, col, 1d));
            } else {
                list.add(new Tuple3<>(col, row, 1d));
            }
        }

        System.out.println(m);

        return list;

    }
}