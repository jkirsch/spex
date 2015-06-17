package edu.tuberlin.spex.compression;

import com.carrotsearch.sizeof.RamUsageEstimator;
import edu.tuberlin.spex.experiments.ExperimentDatasets;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.io.MatrixVectorReader;
import no.uib.cipr.matrix.sparse.CompRowMatrix;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * @author Johannes Kirschnick
 *         17.06.2015
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CompressionTest {

    private CompRowMatrix matrix;
    private SimpleCompressedMatrix simpleCompressedMatrix;
    private DenseVector ones;

    @Test
    public void testCompress() throws Exception {

        Path path = ExperimentDatasets.get(ExperimentDatasets.Matrix.sgpf5y6);
        CompRowMatrix matrix = new CompRowMatrix(new MatrixVectorReader(new FileReader(path.toFile())));

        SimpleCompressedMatrix simpleCompressedMatrix = new SimpleCompressedMatrix(matrix);

        DenseVector ones = VectorHelper.ones(simpleCompressedMatrix.numColumns);

        DenseVector multiplication = simpleCompressedMatrix.multiplication(ones);

        DenseVector mult = (DenseVector) matrix.mult(ones, new DenseVector(simpleCompressedMatrix.numRows));

        Assert.assertArrayEquals(mult.getData(), multiplication.getData(), 0.001);

        System.out.println(RamUsageEstimator.sizeOf(simpleCompressedMatrix));
        System.out.println(RamUsageEstimator.sizeOf(matrix));
    }


    @Setup
    public void setup() throws Exception {
        // create a matrix
        Path path = ExperimentDatasets.get(ExperimentDatasets.Matrix.sgpf5y6);
        matrix = new CompRowMatrix(new MatrixVectorReader(new FileReader(path.toFile())));

        simpleCompressedMatrix = new SimpleCompressedMatrix(matrix);

        ones = VectorHelper.ones(simpleCompressedMatrix.numColumns);

    }

    @Benchmark
    public void multiplySparse() throws Exception {
        DenseVector mult = (DenseVector) matrix.mult(ones, new DenseVector(simpleCompressedMatrix.numRows));

    }

    @Benchmark
    public void multiplyCompressed() throws Exception {
        DenseVector multiplication = simpleCompressedMatrix.multiplication(ones);
    }

}
