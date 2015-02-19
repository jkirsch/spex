package edu.tuberlin.spex.utils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.partition.MatrixBlockPartitioner;
import no.uib.cipr.matrix.DenseVector;

import java.util.List;

/**
 * Date: 18.02.2015
 * Time: 23:05
 *
 */
public class VectorBlockHelper {

    public static List<VectorBlock> createBlocks(final int n, int blockSize, final double init) {
        MatrixBlockPartitioner matrixBlockPartitioner = new MatrixBlockPartitioner(n, blockSize);

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
}
