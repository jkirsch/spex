package edu.tuberlin.spex.matrix.partition;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

/**
 * Date: 12.02.2015
 * Time: 00:21
 */
public class MatrixBlockReducer extends RichGroupReduceFunction<Tuple3<Integer, Integer, Double>, MatrixBlock> {

    int n;
    int blocks;

    final boolean rowNormalize;
    final boolean isTransposed;

    private DenseVector rowSums;

    public MatrixBlockReducer(int n, int blocks, boolean rowNormalize, boolean isTransposed) {
        this.n = n;
        this.blocks = blocks;
        this.rowNormalize = rowNormalize;
        this.isTransposed = isTransposed;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // generate Vector
        if(rowNormalize) {
            List<Tuple2<Integer, Double>> aggregatedSums = getRuntimeContext().getBroadcastVariable("rowSums");

            rowSums = new DenseVector(n);
            for (Tuple2<Integer, Double> aggregatedSum : aggregatedSums) {
                rowSums.set(aggregatedSum.f0, aggregatedSum.f1);
            }
        }

    }

    @Override
    public void reduce(Iterable<Tuple3<Integer, Integer, Double>> values, Collector<MatrixBlock> out) throws Exception {

        PeekingIterator<Tuple3<Integer, Integer, Double>> peekingIterator = Iterators.peekingIterator(values.iterator());

        // we assume that all elements in the group belong to the same partition in the matrix
        // so we can just pick the first one and estimate the block dimensions
        Tuple3<Integer, Integer, Double> peek = peekingIterator.peek();

        // here we have the first instance
        // get the location of the block
        int blockSize = n / blocks;

        // calculate the beginning end row/col of the block
        final MatrixBlockPartitioner.BlockDimensions blockDimensions
                = MatrixBlockPartitioner.getBlockDimensions(n, blockSize, peek.f0, peek.f1);

        /*LinkedSparseMatrix matrix = new LinkedSparseMatrix(blockDimensions.getRows(),
                blockDimensions.getCols());*/

        // we also assume that the tuples are ordered
        // first by row followed by column

        // build the array information

        Iterator<Tuple3<Integer, Integer, Double>> transform = Iterators.transform(peekingIterator, new Function<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>>() {
            @Override
            public Tuple3<Integer, Integer, Double> apply(Tuple3<Integer, Integer, Double> value) {

                // if we row normalize, divide this by the row sum
                double matrixEntry = rowNormalize ?
                        value.f2 / rowSums.get(isTransposed ? value.f1 : value.f0)
                        : value.f2;

                value.f0 -= blockDimensions.getRowStart();
                value.f1 -= blockDimensions.getColStart();
                value.f2 = matrixEntry;

                return value;
            }
        });

        AdaptedCompRowMatrix matrix = AdaptedCompRowMatrix.buildFromSortedIterator(transform, blockDimensions.rows, blockDimensions.cols);
        MatrixBlock matrixBlock = new MatrixBlock(blockDimensions.getRowStart(), blockDimensions.getColStart(), matrix);



        // get the row offset
        // get the column offset
 /*       while (peekingIterator.hasNext()) {
            Tuple3<Integer, Integer, Double> value = peekingIterator.next();
            try {

                // if we row normalize, divide this by the row sum
                double matrixEntry = rowNormalize ?
                        value.f2 / rowSums.get(isTransposed ? value.f1 : value.f0)
                        : value.f2;

                if(matrix.get(value.f0 - blockDimensions.getRowStart(), value.f1 - blockDimensions.getColStart()) > 0) {
                    System.err.println("Setting existing value " + value.toString());
                }

                matrix.set(value.f0 - blockDimensions.getRowStart(), value.f1 - blockDimensions.getColStart(), matrixEntry);
            } catch (java.lang.IndexOutOfBoundsException e) {
                // catch illegal setting
                throw new IllegalStateException(blockDimensions.toString() + "  -Peek- " + peek + " -Value- " + value, e);
            }

        }

        MatrixBlock matrixBlock = new MatrixBlock(
                blockDimensions.getRowStart(),
                blockDimensions.getColStart(),
                new AdaptedCompRowMatrix(matrix));*/

        out.collect(matrixBlock);
    }
}
