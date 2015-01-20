package edu.tuberlin.spex.algorithms;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.CompDiagMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 15.01.2015
 * Time: 23:22
 */
public class PageRank {

    static Logger LOG = LoggerFactory.getLogger(PageRank.class);

    // damping
    final double c;

    public PageRank(double c) {
        Preconditions.checkArgument(Range.open(0d, 1d).contains(c),"This implementation does only work for damping (0,1)");
        this.c = c;
    }

    public Vector calc(Matrix adjacency) {

        Stopwatch stopwatch = new Stopwatch().start();

        // init p with = 1/n
        Vector p0 = VectorHelper.identical(adjacency.numRows(), 1. / (double) adjacency.numRows());

        // scale the constant term - so we can reuse it
        if (c < 1) {
            p0.scale(1. - c);
        }


        Vector p_k;
        Vector p_k1 = new DenseVector(p0);

        int counter = 0;

        // iterate
        // P_k+1 = c * (A+ 1/n*diag(dangling)T )T * p_k + p_0
        do {
            p_k = p_k1.copy();

            p_k1 = adjacency.transMultAdd(c, p_k, p0.copy());

            counter++;

        } while (p_k1.copy().add(-1, p_k).norm(Vector.Norm.One) > 0.0000000001);

        stopwatch.stop();

        String fullClassName = adjacency.getClass().getName();
        String shortenedClassName = fullClassName.substring(fullClassName.lastIndexOf('.') + 1);
        LOG.info("{} Converged after {} : {} in {}", String.format("%-17s", shortenedClassName), counter, p_k1.norm(Vector.Norm.One), stopwatch);

        return p_k1;
    }

    public static Matrix normalizeColumnWise(Matrix input) {

        DenseVector rowSummer = new DenseVector(input.numRows());
        CompDiagMatrix diagMatrix = new CompDiagMatrix(input.numRows(), input.numColumns());

        for (VectorEntry vectorEntry : rowSummer) {
            vectorEntry.set(1);
        }

        Vector colSums = input.mult(rowSummer, rowSummer.copy());

        for (VectorEntry colSum : colSums) {
            double value = colSum.get();
            if(value > 0) {
                diagMatrix.set(colSum.index(), colSum.index(), 1. / value);
            } else {
                diagMatrix.set(colSum.index(), colSum.index(), 1);
            }
        }

        // divide by column sum
        // To do column-wise scaling, use
        // diag(b) * B
        return diagMatrix.mult(input, input.copy());
    }

}