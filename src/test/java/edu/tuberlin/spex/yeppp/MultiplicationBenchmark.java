package edu.tuberlin.spex.yeppp;

import edu.tuberlin.spex.utils.VectorHelper;
import info.yeppp.Core;
import no.uib.cipr.matrix.DenseVector;
import org.apache.commons.compress.archivers.ArchiveException;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 21.06.2015
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MultiplicationBenchmark {

    private DenseVector one;
    private DenseVector two;

    @Setup
    public void setup() throws IOException, ArchiveException {
        one = VectorHelper.ones(1000);
        two = VectorHelper.identical(1000, -1);
    }

    @Benchmark
    public void mjt() {

        double res = one.dot(two);

    }

    @Benchmark
    public void yepp(){

        double res = Core.DotProduct_V64fV64f_S64f(one.getData(), 0, two.getData(), 0, two.size());

    }
}
