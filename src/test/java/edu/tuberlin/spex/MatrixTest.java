package edu.tuberlin.spex;

import edu.tuberlin.spex.algorithms.PageRank;
import edu.tuberlin.spex.algorithms.Reordering;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.Matchers.closeTo;

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

        FlexCompRowMatrix adjacency = new FlexCompRowMatrix(6, 6);

        // example graph
        // http://www.ccs.northeastern.edu/home/daikeshi/notes/PageRank.pdf
        // A=0, B=1, C=2, D=3, E=4, F=5
        // A -> B, A -> C, A -> F
        adjacency.set(0, 1, 1);
        adjacency.add(0, 2, 1);
        adjacency.add(0, 5, 1);
        // B -> C, B -> D, B -> E, B -> F
        adjacency.add(1, 2, 1);
        adjacency.add(1, 3, 1);
        adjacency.add(1, 4, 1);
        adjacency.add(1, 5, 1);
        // C -> D, C -> E
        adjacency.add(2, 3, 1);
        adjacency.add(2, 4, 1);
        // D -> A, D -> C, D -> E, D -> F
        adjacency.add(3, 0, 1);
        adjacency.add(3, 2, 1);
        adjacency.add(3, 4, 1);
        adjacency.add(3, 5, 1);
        // E -> A
        adjacency.add(4, 0, 1);
        // F -> A, F -> B, F -> E
        adjacency.add(5, 0, 1);
        adjacency.add(5, 1, 1);
        adjacency.add(5, 4, 1);


        // divide by column sum
        for (int i = 0; i < adjacency.numRows(); i++) {

            SparseVector row = adjacency.getRow(i);
            double sum = row.norm(Vector.Norm.One);

            if (sum > 0) {
                row.scale(1. / sum);
            }

        }

        PageRank.Normalized normalized = PageRank.normalizeRowWise(adjacency);

        PageRank pageRank = new PageRank(0.85);

        Vector p_k1 = pageRank.calc(normalized);

        System.out.println(new DenseMatrix(adjacency));
        for (VectorEntry vectorEntry : p_k1) {
            System.out.printf("\t %2d %1.3f\n", vectorEntry.index(), vectorEntry.get());
        }
        System.out.println(p_k1.norm(Vector.Norm.One));
    }

    @Test
    public void testPageRankDangling() throws Exception {

        FlexCompRowMatrix adjacency = new FlexCompRowMatrix(4, 4);

        adjacency.set(0, 0, 1);
        adjacency.set(1, 1, 2);
        adjacency.set(2, 2, 3);
        adjacency.set(3, 1, 2);

        System.out.println(new DenseMatrix(adjacency));

        PageRank pageRank = new PageRank(0.85);

        PageRank.Normalized normalizedRowWise = PageRank.normalizeRowWise(new DenseMatrix(adjacency));

        System.out.println(normalizedRowWise.getColumnNormalized());
        System.out.println(normalizedRowWise.getDanglingNodes());

        //System.out.println(new DenseMatrix(adjacency));
        //System.out.println(new DenseMatrix(normalizedRowWise.getColumnNormalized()));
        //System.out.println(new DenseMatrix(Reordering.orderByRowSum(adjacency)));
        //System.out.println(new DenseMatrix(Reordering.orderByColumnSum(adjacency)));

        Vector normal = pageRank.calc(normalizedRowWise);
        System.out.println(normal);

        Matrix orderByColumnSum = Reordering.orderByColumnSum(adjacency);

        PageRank.Normalized normalized = PageRank.normalizeRowWise(orderByColumnSum);
        System.out.println("orderByColumnSum \n" + new DenseMatrix(orderByColumnSum));
        System.out.println(normalized.getDanglingNodes());
        Vector reordered = pageRank.calc(normalized);
        System.out.println(reordered);

        Assert.assertThat(reordered.norm(Vector.Norm.One), closeTo(normal.norm(Vector.Norm.One), 0.0000001));

        //System.out.println(adjacency.norm(Matrix.Norm.Frobenius));
        //System.out.println(Reordering.orderByRowSum(adjacency).norm(Matrix.Norm.Frobenius));
        //System.out.println(Reordering.orderByRowSum(adjacency).norm(Matrix.Norm.Frobenius));
    }

    @Test
    public void testReorder() throws Exception {

        Matrix adjacency = new DenseMatrix(3, 3);

        adjacency.set(0, 0, 1);
        adjacency.set(0, 1, 2);
        adjacency.set(0, 2, 3);

        adjacency.set(1, 0, 4);
        adjacency.set(1, 1, 5);
        adjacency.set(1, 2, 6);

        adjacency.set(2, 0, 7);
        adjacency.set(2, 1, 8);
        adjacency.set(2, 2, 9);

        System.out.println(adjacency);

        PermutationMatrix permutationMatrix = new PermutationMatrix(new int[] {0, 2, 1});

        System.out.println(permutationMatrix);

        // re-order row wise
        System.out.println("Row-wise\n" + permutationMatrix.mult(adjacency, new DenseMatrix(adjacency)));

        // reorder column wise
        Matrix columnWise = adjacency.mult(permutationMatrix, new DenseMatrix(adjacency));
        System.out.println("Column Wise\n" + columnWise);

        System.out.println(PageRank.normalizeRowWise(columnWise));

        PageRank pageRank = new PageRank(0.85);
        pageRank.calc(PageRank.normalizeRowWise(adjacency));

        pageRank.calc(PageRank.normalizeRowWise(columnWise));



    }

}
