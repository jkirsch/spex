package edu.tuberlin.spex.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.AbstractMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.CompDiagMatrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;

import static edu.tuberlin.spex.matrix.partition.CreateMatrixBlockFromSortedEntriesReducer.MatrixType;

/**
 * Created by Johannes on 08.04.2016.
 */
public class PickFastestMatrix {
    
    public static AbstractMatrix generateFastestMatrix(Iterator<Tuple3<Integer, Integer, Double>> input, int rows, int cols) {

        AbstractMatrix matrix = null;

        // generate a inputVector
        DenseVector inputVector = VectorHelper.ones(cols);
        DenseVector outputVector = new DenseVector(cols);

        long fastestTime = Long.MAX_VALUE;

        // reuse iterator ?
        ArrayList<Tuple3<Integer, Integer, Double>> transform = Lists.newArrayList(input);


        for (MatrixType matrixType : EnumSet.of(MatrixType.CompRowMatrix)) {
            AbstractMatrix testMatrix = buildMatrixfromSortedIterator(matrixType, transform.iterator(), rows, cols);

            long start = System.nanoTime();
            for (int i = 0; i < 2; i++) {
                testMatrix.mult(inputVector, outputVector);
            }
            long elapsedTime = System.nanoTime() - start;


            if (elapsedTime < fastestTime) {
                matrix = testMatrix;
                fastestTime = elapsedTime;
            }
        }

        Preconditions.checkNotNull(matrix, "Could not find the best matrix");
        
        return matrix;
    }

        private static AbstractMatrix buildMatrixfromSortedIterator(MatrixType matrixType, Iterator<Tuple3<Integer, Integer, Double>> transform, int rows, int cols) {

            AbstractMatrix matrix;

            switch (matrixType) {
                case CompRowMatrix:
                    matrix = AdaptedCompRowMatrix.buildFromSortedIterator(transform, rows, cols);
                    break;
                case CompDiagMatrix:
                    matrix = new CompDiagMatrix(rows, cols);
                    while (transform.hasNext()) {
                        Tuple3<Integer, Integer, Double> element = transform.next();
                        matrix.set(element.f0, element.f1, element.f2);
                    }
                    break;
                case LinkedSparseMatrix:
                    matrix = new LinkedSparseMatrix(rows, cols);
                    while (transform.hasNext()) {
                        Tuple3<Integer, Integer, Double> element = transform.next();
                        matrix.set(element.f0, element.f1, element.f2);
                    }
                    break;
                case FlexCompRowMatrix:
                    matrix = new FlexCompRowMatrix(rows, cols);
                    while (transform.hasNext()) {
                        Tuple3<Integer, Integer, Double> element = transform.next();
                        matrix.set(element.f0, element.f1, element.f2);
                    }
                    break;
                default:
                    throw new IllegalStateException(matrixType + " not implemented yet");
            }

            return matrix;
        }
}
