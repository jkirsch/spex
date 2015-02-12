package edu.tuberlin.spex.matrix;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import edu.tuberlin.spex.algorithms.domain.MatrixBlock;
import edu.tuberlin.spex.matrix.io.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Date: 12.02.2015
 * Time: 00:21
 */
public class MatrixBlockReducer implements GroupReduceFunction<Tuple3<Integer, Integer, Float>, MatrixBlock> {

    int n;
    int blocks;

    public MatrixBlockReducer(int n, int blocks) {
        this.n = n;
        this.blocks = blocks;
    }

    @Override
    public void reduce(Iterable<Tuple3<Integer, Integer, Float>> values, Collector<MatrixBlock> out) throws Exception {

        PeekingIterator<Tuple3<Integer, Integer, Float>> peekingIterator = Iterators.peekingIterator(values.iterator());

        // we assume that all elements in the group belong to the same partition in the matrix
        // so we can just pick the first one and estimate the block dimensions
        Tuple3<Integer, Integer, Float> peek = peekingIterator.peek();

        // here we have the first instance
        // get the location of the block
        int blockSize = n / blocks;

        // calculate the beginning end row/col of the block
        MatrixBlockPartitioner.BlockDimensions blockDimensions
                = MatrixBlockPartitioner.getBlockDimensions(n, blockSize, peek.f0, peek.f1);

        LinkedSparseMatrix matrix = new LinkedSparseMatrix(blockDimensions.getRows(),
                blockDimensions.getCols());
        // get the row offset
        // get the column offset
        while (peekingIterator.hasNext()) {
            Tuple3<Integer, Integer, Float> value = peekingIterator.next();
            try {
                matrix.set(value.f0 - blockDimensions.getRowStart(), value.f1 - blockDimensions.getColStart(), value.f2);
            } catch (java.lang.IndexOutOfBoundsException e) {
                // catch illegal setting
                throw new IllegalStateException(blockDimensions.toString() + "  -Peek- " + peek + " -Value- " + value, e);
            }

        }


        MatrixBlock matrixBlock = new MatrixBlock(
                blockDimensions.getRowStart(),
                blockDimensions.getColStart(),
                new AdaptedCompRowMatrix(matrix));

        out.collect(matrixBlock);
    }
}
