package edu.tuberlin.spex.block;

import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.CompRowMatrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import org.apache.commons.compress.archivers.ArchiveException;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 *         21.06.2015
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BlockCompressedSparseRowFormatBenchmark {

    private CompRowMatrix matrix;
    private BlockCompressedSparseRowFormat blockCompressedSparseRowFormat;
    private DenseVector ones;

    @Setup
    public void setup() throws IOException, ArchiveException {

        FlexCompRowMatrix m = new FlexCompRowMatrix(6, 6);

        m.set(0, 0, 11);
        m.set(0, 1, 12);
        m.set(0, 2, 13);
        m.set(0, 3, 14);

        m.set(1, 1, 22);
        m.set(1, 2, 23);

        m.set(2, 2, 33);
        m.set(2, 3, 34);
        m.set(2, 4, 35);
        m.set(2, 5, 36);

        m.set(3, 3, 44);
        m.set(3, 4, 45);

        m.set(4, 5, 56);
        m.set(5, 5, 66);

        Path path = ExperimentDatasets.get(ExperimentDatasets.Matrix.rlfprim);
        matrix = new CompRowMatrix(m);

        blockCompressedSparseRowFormat = new BlockCompressedSparseRowFormat(matrix, 2, 2);

        ones = VectorHelper.ones(m.numColumns());
    }


    @Benchmark
    public void CompressSparseRow() {

       matrix.mult(ones, new DenseVector(matrix.numRows()));
    }

    @Benchmark
    public void BlockCompressedSparseRow() throws IOException {

        blockCompressedSparseRowFormat.multiplication(ones);
    }


}
