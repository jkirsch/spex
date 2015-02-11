package edu.tuberlin.spex.algorithms;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.MatrixBlockPartitioner;
import edu.tuberlin.spex.matrix.io.AdaptedCompRowMatrixSerializer;
import edu.tuberlin.spex.matrix.io.DenseMatrixSerializer;
import edu.tuberlin.spex.matrix.io.LinkedSparseMatrixSerializer;
import edu.tuberlin.spex.matrix.io.adapted.AdaptedCompRowMatrix;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

/**
 * Date: 09.02.2015
 * Time: 23:42
 */
public class FlinkMatrixReader {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple3<Integer, Integer, Float>> input = env.createInput(new MatrixReaderInputFormat(new Path("datasets/webNotreDame.mtx")));

        final int n = 325729;
        final int blocks = 4;

        env.registerKryoSerializer(DenseMatrix.class, new DenseMatrixSerializer());
        env.registerKryoSerializer(LinkedSparseMatrix.class, new LinkedSparseMatrixSerializer());
        env.registerKryoSerializer(AdaptedCompRowMatrix.class, new AdaptedCompRowMatrixSerializer());

        UnsortedGrouping<Tuple3<Integer, Integer, Float>> tuple3UnsortedGrouping = input.groupBy(new MatrixBlockPartitioner(n, blocks));

        tuple3UnsortedGrouping.reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Float>, MatrixBlock>() {
            @Override
            public void reduce(Iterable<Tuple3<Integer, Integer, Float>> values, Collector<MatrixBlock> out) throws Exception {

                LinkedSparseMatrix matrix = new LinkedSparseMatrix(n / blocks, n / blocks);
                // get the row offset
                // get the column offset

                int blockSize = n / blocks;

                int lastrow = 0;
                // get the offset
                for (Tuple3<Integer, Integer, Float> value : values) {

                    matrix.set(value.f0 % blockSize, value.f1 % blockSize, value.f2);
                    lastrow = value.f0;
                }

                // get the location of the block
                int offset = (lastrow / blockSize - 1) * blockSize;
                MatrixBlock matrixBlock = new MatrixBlock(offset,offset, new AdaptedCompRowMatrix(matrix));

                out.collect(matrixBlock);
            }
        }).first(2).print();

        env.execute();

    }


}
