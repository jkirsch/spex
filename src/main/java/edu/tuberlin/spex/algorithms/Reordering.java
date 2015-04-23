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

    private static final Ordering<Index> vectorEntryOrdering = Ordering.natural().onResultOf(new Function<Index, Comparable>() {
        @Override
        public Comparable apply(Index input) {
            return input.getValue();
        }
    }).reverse();
    private static final Function<VectorEntry, Index> function = new Function<VectorEntry, Index>() {
        @Override
        public Index apply(VectorEntry input) {
            return new Index(input.index(), input.get());
        }
    };

    public static Matrix orderByRowSum(Matrix matrix) {

        Vector IDENTITY_VECTOR = VectorHelper.ones(matrix.numColumns());

        Vector rowSums = matrix.mult(IDENTITY_VECTOR, new DenseVector(matrix.numColumns()));

        List<Index> sortedByRow = vectorEntryOrdering.sortedCopy(Iterables.transform(rowSums, function));

        FlexCompRowMatrix rowMatrix = new FlexCompRowMatrix(matrix);

        int[] positions = new int[matrix.numColumns()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i;
        }

        int counter = 0;
        for (Index index : sortedByRow) {
            // element[counter] should now be at new Index
            int newIndex = index.getIndex();
            if (counter != positions[newIndex]) {
                // reorder in place by swapping
                SparseVector original = rowMatrix.getRow(counter);
                SparseVector newRow = rowMatrix.getRow(positions[newIndex]);
                rowMatrix.setRow(counter, newRow);
                rowMatrix.setRow(positions[newIndex], original);

                // remember that we swapped
                int oldValue = positions[newIndex];
                positions[newIndex] = positions[counter];
                positions[counter] = oldValue;
            }
            counter++;
        }


        return rowMatrix;
    }

    public static Matrix orderByColumnSum(Matrix matrix) {

        Vector IDENTITY_VECTOR = VectorHelper.ones(matrix.numColumns());

        Vector colSums = matrix.transMult(IDENTITY_VECTOR, new DenseVector(matrix.numColumns()));

        List<Index> sortedByRow = vectorEntryOrdering.sortedCopy(Iterables.transform(colSums, function));

        FlexCompColMatrix columnMatrix = new FlexCompColMatrix(matrix);

        int[] positions = new int[matrix.numColumns()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i;
        }

        int counter = 0;
        for (Index index : sortedByRow) {
            int newIndex = index.getIndex();
            if (counter != positions[newIndex]) {
                // reorder in place by swapping
                SparseVector original = columnMatrix.getColumn(counter);
                SparseVector newColumn = columnMatrix.getColumn(positions[newIndex]);
                columnMatrix.setColumn(counter, newColumn);
                columnMatrix.setColumn(positions[newIndex], original);

                // remember that we swapped
                int oldValue = positions[newIndex];
                positions[newIndex] = counter;
                positions[counter] = oldValue;
            }
            counter++;
        }


        return columnMatrix;
    }

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
