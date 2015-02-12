package edu.tuberlin.spex.algorithms;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.MatrixBlockReducer;
import edu.tuberlin.spex.matrix.io.AdaptedCompRowMatrixSerializer;
import edu.tuberlin.spex.matrix.io.DenseMatrixSerializer;
import edu.tuberlin.spex.matrix.io.DenseVectorSerializer;
import edu.tuberlin.spex.matrix.io.LinkedSparseMatrixSerializer;
import edu.tuberlin.spex.matrix.io.adapted.AdaptedCompRowMatrix;
import edu.tuberlin.spex.utils.MatrixBlockVectorKernel;
import edu.tuberlin.spex.utils.VectorHelper;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;

/**
 * Date: 09.02.2015
 * Time: 23:42
 */
public class FlinkMatrixReader {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path("datasets/webNotreDame.mtx"), -1, 325729));

        final int n = 325729;
        final int blocks = 4;

        env.registerKryoSerializer(DenseMatrix.class, new DenseMatrixSerializer());
        env.registerKryoSerializer(LinkedSparseMatrix.class, new LinkedSparseMatrixSerializer());
        env.registerKryoSerializer(AdaptedCompRowMatrix.class, new AdaptedCompRowMatrixSerializer());
        env.registerKryoSerializer(DenseVector.class, new DenseVectorSerializer());


        UnsortedGrouping<Tuple3<Integer, Integer, Double>> tuple3UnsortedGrouping = input.groupBy(new MatrixBlockPartitioner(n, blocks));


        AggregateOperator<Tuple> rowSumsDataSet = input.project(0,2).groupBy(0).aggregate(Aggregations.SUM, 1).name("Calculate RowSums");

        final GroupReduceOperator<Tuple3<Integer, Integer, Double>, MatrixBlock> matrixBlocks = tuple3UnsortedGrouping.
                reduceGroup(new MatrixBlockReducer(n, blocks, true)).withBroadcastSet(rowSumsDataSet, "rowSums");

        // now multiply the matrixblocks with the vector

        DataSource<DenseVector> denseVectorDataSource = env.fromElements(VectorHelper.identical(n, 1 / (double) n));


        IterativeDataSet<DenseVector> loop = denseVectorDataSource.iterate(10);


        ReduceOperator<DenseVector> reduce = matrixBlocks.map(new MatrixBlockVectorKernel()).withBroadcastSet(loop, "vector")
                .reduce(new ReduceFunction<DenseVector>() {
                    @Override
                    public DenseVector reduce(DenseVector vector, DenseVector t1) throws Exception {
                        return (DenseVector) vector.add(t1);
                    }
                });


        DataSet<DenseVector> result = loop.closeWith(reduce);

        result.print();

        String executionPlan = env.getExecutionPlan();

        System.out.println(executionPlan);

        env.execute();

    }

}
