package edu.tuberlin.spex.matrix.blocks;

import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.types.Value;

/**
 * Created by Johannes on 14.04.2016.
 */
public abstract class SpexMatrixBlock implements Value {

    /**
     * Number of rows
     */
    protected int numRows;

    /**
     * Number of columns
     */
    protected int numColumns;

    public abstract void mult(DenseVector v);
}
