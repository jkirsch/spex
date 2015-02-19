package edu.tuberlin.spex.utils;

import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Date: 18.02.2015
 * Time: 21:47
 *
 */
public class MatrixBlockVectorKernelCross implements MapFunction<Tuple2<MatrixBlock, DenseVector>, DenseVector> {

    private final double alpha;

    public MatrixBlockVectorKernelCross(double alpha) {
        this.alpha = alpha;
    }

    @Override
    public DenseVector map(Tuple2<MatrixBlock, DenseVector> value) throws Exception {
        return (DenseVector) value.f0.mult(alpha, value.f1);
    }
}
