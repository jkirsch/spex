package edu.tuberlin.spex.compression;

import edu.tuberlin.spex.experiments.ExperimentDatasets;
import me.lemire.integercompression.*;
import no.uib.cipr.matrix.io.MatrixVectorReader;
import no.uib.cipr.matrix.sparse.CompColMatrix;
import org.openjdk.jmh.annotations.*;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 17.06.2015
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CompressedMultiplicationBenchmark {

    IntegerCODEC is = new Composition(new FastPFOR128(), new VariableByte());

    private CompColMatrix matrix;
    private int[] compressed;

    @Setup
    public void setup() throws Exception {
        // create a matrix
        Path path = ExperimentDatasets.get(ExperimentDatasets.Matrix.sgpf5y6);
        matrix = new CompColMatrix(new MatrixVectorReader(new FileReader(path.toFile())));

        System.out.println(matrix.numColumns());
        System.out.println(matrix.numRows());

        IntWrapper inputoffset = new IntWrapper(0);
        IntWrapper outputoffset = new IntWrapper(0);

        compressed = new int[matrix.getRowIndices().length + 10000];
        is.compress(matrix.getRowIndices(), inputoffset, matrix.getRowIndices().length, compressed, outputoffset);
        // we can repack the data: (optional)
        compressed = Arrays.copyOf(compressed, outputoffset.intValue());
    }

    @Benchmark
    public void compressionTest() throws Exception {

        // now see if we can compress the array
       /* IntegratedIntegerCODEC is =
                new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte());
*/

        IntWrapper inputoffset = new IntWrapper(0);
        IntWrapper outputoffset = new IntWrapper(0);

        int[] compressed = new int[matrix.getRowIndices().length + 10000];

        is.compress(matrix.getRowIndices(), inputoffset, matrix.getRowIndices().length, compressed, outputoffset);

    }

    @Benchmark
    public void decompressisonTest() throws Exception {

        // now see if we can compress the array
       /* IntegratedIntegerCODEC is =
                new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte());
*/

        IntWrapper inputoffset = new IntWrapper(0);
        IntWrapper outputoffset = new IntWrapper(0);

        int[] recovered = new int[matrix.getRowIndices().length];
        IntWrapper recoffset = new IntWrapper(0);

        is.uncompress(compressed, new IntWrapper(0), compressed.length, recovered, recoffset);

    }
}
