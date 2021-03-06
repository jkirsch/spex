package edu.tuberlin.spex.utils;

import com.google.common.base.Function;
import com.google.common.collect.*;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 18.02.2015
 * Time: 23:05
 */
public class VectorBlockHelper {

    public static List<VectorBlock> createBlocks(final int rows, final int cols, int blocks, final double init) {
        MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(rows, cols, blocks);

        // get the row partition sizes
        List<MatrixBlockPartitioner.BlockDimensions> blockDimensions = matrixBlockPartitioner.computeRowSizes();

        return Lists.transform(blockDimensions, new Function<MatrixBlockPartitioner.BlockDimensions, VectorBlock>() {
            @Override
            public VectorBlock apply(MatrixBlockPartitioner.BlockDimensions blockDimension) {
                DenseVector identical = VectorHelper.identical(blockDimension.getRows(), init);
                // if overflow set the last elements to 0
                return new VectorBlock(
                        blockDimension.getRowStart(),
                        identical);
            }
        });

    }

    public static List<Tuple2<Integer, Double>> createSingleVector(final int rows, final double init) {


        List<Tuple2<Integer, Double>> list = Lists.newArrayListWithExpectedSize(rows);

        for (int i = 0; i < rows; i++) {
            list.add(new Tuple2<>(i, init));
        }

        return list;

    }

    public static List<Tuple2<Integer, VectorBlock>> createTupleBlocks(final int n, int blockSize, final double init) {
        MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blockSize);

        // get the row partition sizes
        List<MatrixBlockPartitioner.BlockDimensions> blockDimensions = matrixBlockPartitioner.computeRowSizes();


        return Lists.transform(blockDimensions, new Function<MatrixBlockPartitioner.BlockDimensions, Tuple2<Integer, VectorBlock>>() {
            @Override
            public Tuple2<Integer, VectorBlock> apply(MatrixBlockPartitioner.BlockDimensions blockDimension) {
                DenseVector identical = VectorHelper.identical(blockDimension.getRows(), init);
                // if overflow set the last elements to 0
                VectorBlock block = new VectorBlock(
                        blockDimension.getRowStart(),
                        identical);
                return new Tuple2<>(blockDimension.getRowStart(), block);
            }
        });

    }
}
