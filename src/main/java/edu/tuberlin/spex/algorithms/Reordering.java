package edu.tuberlin.spex.algorithms;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.FlexCompColMatrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;

import java.util.List;

/**
 * Date: 18.01.2015
 * Time: 19:37
 */
public class Reordering {

    public static Matrix orderByRowSum(Matrix matrix) {

        DenseVector IDENTITY_VECTOR = VectorHelper.ones(matrix.numColumns());

        Vector rowSums = matrix.mult(IDENTITY_VECTOR, new DenseVector(matrix.numColumns()));
        Vector colSums = matrix.transMult(IDENTITY_VECTOR, new DenseVector(matrix.numColumns()));

        List<Index> sortedByRow = vectorEntryOrdering.sortedCopy(Iterables.transform(rowSums, function));

        FlexCompRowMatrix rowMatrix = new FlexCompRowMatrix(matrix);

        int counter = 0;
        for (Index index : sortedByRow) {
            if(counter != index.getIndex()) {
                // reorder in place by swapping
                SparseVector original = rowMatrix.getRow(counter);
                SparseVector newRow = rowMatrix.getRow(index.getIndex());
                rowMatrix.setRow(counter, newRow);
                rowMatrix.setRow(index.getIndex(), original);
            }
            counter++;
        }


        return rowMatrix;
    }

    public static Matrix orderByColumnSum(Matrix matrix) {

        DenseVector IDENTITY_VECTOR = VectorHelper.ones(matrix.numColumns());

        Vector colSums = matrix.transMult(IDENTITY_VECTOR, new DenseVector(matrix.numColumns()));

        List<Index> sortedByRow = vectorEntryOrdering.sortedCopy(Iterables.transform(colSums, function));

        FlexCompColMatrix columnMatrix = new FlexCompColMatrix(matrix);

        int counter = 0;
        for (Index index : sortedByRow) {
            if(counter != index.getIndex()) {
                // reorder in place by swapping
                SparseVector original = columnMatrix.getColumn(counter);
                SparseVector newRow = columnMatrix.getColumn(index.getIndex());
                columnMatrix.setColumn(counter, newRow);
                columnMatrix.setColumn(index.getIndex(), original);
            }
            counter++;
        }


        return columnMatrix;
    }



    public static Ordering<Index> vectorEntryOrdering = Ordering.natural().onResultOf(new Function<Index, Comparable>() {
        @Override
        public Comparable apply(Index input) {
            return input.getValue();
        }
    });

    public static Function<VectorEntry, Index> function = new Function<VectorEntry, Index>() {
        @Override
        public Index apply(VectorEntry input) {
            return new Index(input.index(), input.get());
        }
    };


    private static class Index {
        final int index;
        final double value;

        public Index(int index, double value) {
            this.index = index;
            this.value = value;
        }

        public int getIndex() {
            return index;
        }

        public double getValue() {
            return value;
        }
    }
}
