package edu.tuberlin.spex.matrix.kernel;

import com.google.common.base.Stopwatch;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.utils.Utils;
import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Date: 20.02.2015
 * Time: 22:16
 *
 */
public class TimingMatrixBlockVectorKernel extends RichMapFunction<Tuple2<MatrixBlock, VectorBlock>, VectorBlock> {

    static Logger LOG = LoggerFactory.getLogger(TimingMatrixBlockVectorKernel.class);
    public static String TIMINGS_ACCUMULATOR = "timings";

    private Histogram histogram;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // register timing
        histogram = getRuntimeContext().getHistogram(TIMINGS_ACCUMULATOR);
    }

    @Override
    public VectorBlock map(Tuple2<MatrixBlock, VectorBlock> matrixBlockVectorBlockTuple2) throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        DenseVector mult = (DenseVector) matrixBlockVectorBlockTuple2.f0.mult(matrixBlockVectorBlockTuple2.f1);
        if (mult == null) {
            LOG.error("Result is empty vector");
            LOG.error(matrixBlockVectorBlockTuple2.toString());
        }
        histogram.add(Utils.safeLongToInt(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS)));
        return new VectorBlock(matrixBlockVectorBlockTuple2.f0.getStartRow(), mult);
    }
}
