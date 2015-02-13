package edu.tuberlin.spex.utils;

import com.google.common.collect.Iterables;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Date: 12.02.2015
 * Time: 20:16
 */
public class MatrixBlockVectorKernel extends RichMapFunction<MatrixBlock, DenseVector> {

    private DenseVector vector;
    private final double alpha;

    public MatrixBlockVectorKernel(double alpha) {
        this.alpha = alpha;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.vector = (DenseVector)
                Iterables.getOnlyElement(getRuntimeContext().getBroadcastVariable("vector"));
    }

    @Override
    public DenseVector map(MatrixBlock matrixBlock) throws Exception {

        return (DenseVector) matrixBlock.mult(alpha, vector);

    }
}
