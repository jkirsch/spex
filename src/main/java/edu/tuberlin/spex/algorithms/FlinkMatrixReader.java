package edu.tuberlin.spex.algorithms;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.MatrixBlockReducer;
import edu.tuberlin.spex.utils.MatrixBlockVectorKernel;
import edu.tuberlin.spex.utils.SerializerRegistry;
import edu.tuberlin.spex.utils.VectorHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.SparseVector;
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
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 09.02.2015
 * Time: 23:42
 */
public class FlinkMatrixReader {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final boolean transpose = true;

        DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path("datasets/webNotreDame.mtx"), -1, 325729, transpose));

        final int n = 325729;
        final int blocks = 4;

        final double alpha = 0.85;

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
                reduceGroup(new MatrixBlockReducer(n, blocks, true, transpose)).withBroadcastSet(colSumsDataSet, "rowSums");

        matrixBlocks.print();

        // now multiply the matrixblocks with the vector

        DataSource<DenseVector> denseVectorDataSource = env.fromElements(
                VectorHelper.identical(n, 1 / (double) n));

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


        result.print();

        String executionPlan = env.getExecutionPlan();

        System.out.println(executionPlan);

        env.execute();

        DenseVector p_k1 = Iterables.getOnlyElement(resultCollector);
        System.out.println(p_k1.norm(Vector.Norm.One));
        System.out.println(p_k1);

        Preconditions.checkArgument((Math.abs((p_k1.norm(Vector.Norm.One) - 1)) - 0.00001) <= 0.0);

    }

}
