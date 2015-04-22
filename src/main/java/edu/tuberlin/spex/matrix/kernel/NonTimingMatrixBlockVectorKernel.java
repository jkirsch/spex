package edu.tuberlin.spex.matrix.kernel;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Date: 20.02.2015
 * Time: 22:16
 *
 */
public class NonTimingMatrixBlockVectorKernel implements MapFunction<Tuple2<MatrixBlock, VectorBlock>, VectorBlock> {

    @Override
    public VectorBlock map(Tuple2<MatrixBlock, VectorBlock> matrixBlockVectorBlockTuple2) throws Exception {
        VectorBlock vectorBlock = matrixBlockVectorBlockTuple2.f1;
        MatrixBlock matrixBlock = matrixBlockVectorBlockTuple2.f0;
        VectorBlock mult = (VectorBlock) matrixBlock.mult(vectorBlock);
        return new VectorBlock(matrixBlock.getStartRow(), mult);
    }
}
