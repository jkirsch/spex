package edu.tuberlin.spex;

import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.CompDiagMatrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.junit.Test;

/**
 * Date: 09.01.2015
 * Time: 14:59
 *
 */
public class MatrixTest {

    @Test
    public void testMultiply() throws Exception {

        int rows = 10;
        int col = 10;

        Matrix randomMatrix = Matrices.random(rows, col);
        DenseVector vector = new DenseVector(rows);
        DenseVector vector2 = new DenseVector(rows);

        for (int i = 0; i < rows; i++) {
            vector.set(i, 1);
        }


        randomMatrix.mult(vector, vector2);

        System.out.println(randomMatrix);
        System.out.println(vector);
        System.out.println(vector2);
    }

    @Test
    public void testPageRank() throws Exception {

        FlexCompRowMatrix adjacency = new FlexCompRowMatrix(5, 5);

        adjacency.set(0, 1, 1);
        adjacency.set(0, 2, 1);
        adjacency.set(1, 3, 1);
        adjacency.set(2, 3, 1);
        adjacency.set(2, 4, 1);
        adjacency.set(3, 4, 1);
        adjacency.set(4, 0, 1);

        Matrix k = new CompDiagMatrix(5, 5);

        // divide by column sum
        for (int i = 0; i < adjacency.numRows(); i++) {


            SparseVector row = adjacency.getRow(i);
            double sum = row.norm(Vector.Norm.One);

            if(sum > 0) {
                row.scale(1. / sum);
                k.set(i, i, 1. / sum);
            }
        }

        Vector p0 = new DenseVector(adjacency.numRows());
        for (VectorEntry vectorEntry : p0) {
            vectorEntry.set(1 / (double) adjacency.numColumns());
        }


        // out degree per node

        double c = 0.85d;

        System.out.println(p0);
        p0.scale(1 - c);

        System.out.println(p0);

        // loop

        Vector p_k;
        Vector p_k1 = new DenseVector(p0);

        int counter = 0;

        do {
            p_k = p_k1.copy();
            p_k1 = adjacency.transMultAdd(c, p_k, p0.copy());

            counter++;

        } while (p_k1.copy().add(-1, p_k).norm(Vector.Norm.Two) > 0.00001);

        //p_k1.scale(adjacency.numRows());

        System.out.println("Converged after " + counter + " : " + p_k1.norm(Vector.Norm.One));

        System.out.println(p_k1);

    }
}
