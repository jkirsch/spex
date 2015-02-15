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
import no.uib.cipr.matrix.sparse.SparseVector;
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

    public Vector calc(Normalized normalized) {

        Stopwatch stopwatch = new Stopwatch().start();

        Matrix adjacency = normalized.columnNormalized;
        Vector dangling = normalized.danglingNodes;

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
        // P_k+1 = c * (A+ 1/n*diag(dangling)T )T * p_k + p_0 + (dangling)
        // dangling = c / n * p_k * dangling
        do {
            p_k = p_k1.copy();

            // add random coefficient
            Vector rand = new DenseVector(p0.size());
            for (VectorEntry vectorEntry : dangling) {
                int index = vectorEntry.index();
                rand.set(index, p_k.get(index) );
            }
            double added = c * rand.norm(Vector.Norm.One) / (double) p0.size();
            Vector y = VectorHelper.identical(p0.size(), added).add(p0);
            p_k1 = adjacency.transMultAdd(c, p_k, y);

            counter++;

        } while (p_k1.copy().add(-1, p_k).norm(Vector.Norm.One) > 0.00000000001);

        stopwatch.stop();

        String fullClassName = adjacency.getClass().getName();
        String shortenedClassName = fullClassName.substring(fullClassName.lastIndexOf('.') + 1);
        LOG.info("{} Converged after {} : {} in {}", String.format("%-17s", shortenedClassName), counter, p_k1.norm(Vector.Norm.One), stopwatch);

        return p_k1;
    }

    public static Normalized normalizeRowWise(Matrix input) {
        Vector rowSummer = VectorHelper.ones(input.numRows());
        CompDiagMatrix diagMatrix = new CompDiagMatrix(input.numRows(), input.numColumns());

        Vector rowSums = input.mult(rowSummer, rowSummer.copy());
        //Vector colSums = input.transMult(rowSummer, rowSummer.copy());

        Vector dangling = new SparseVector(rowSummer.size());

        for (VectorEntry rowSum : rowSums) {
            double value = rowSum.get();
            int index = rowSum.index();
            if(value > 0) {
                diagMatrix.set(index, index, 1. / value);
            } else {
                diagMatrix.set(index, index, 1);
                dangling.set(index, 1);
            }
        }


        // divide by column sum
        // To do column-wise scaling, use
        // diag(b) * B
        return new Normalized(diagMatrix.mult(input, input.copy()), dangling);

    }

    public static class Normalized {
        Matrix columnNormalized;
        Vector danglingNodes;

        public Normalized(Matrix columnNormalized, Vector danglingNodes) {
            this.columnNormalized = columnNormalized;
            this.danglingNodes = danglingNodes;
        }

        public Matrix getColumnNormalized() {
            return columnNormalized;
        }

        public Vector getDanglingNodes() {
            return danglingNodes;
        }

        @Override
        public String toString() {
            return "Normalized{" +
                    "columnNormalized=" + columnNormalized +
                    ", danglingNodes=" + danglingNodes +
                    '}';
        }
    }
}
