package edu.tuberlin.spex.algorithms;

import com.google.common.base.Stopwatch;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 15.01.2015
 * Time: 23:22
 *
 */
public class PageRank {

    static Logger LOG = LoggerFactory.getLogger(PageRank.class);

    // damping
    final double c;

    public PageRank(double c) {
        this.c = c;
    }

    public Vector calc(Matrix adjacency) {

        Stopwatch stopwatch = new Stopwatch().start();

        // init p with = 1/n
        Vector p0 = new DenseVector(adjacency.numRows());
        for (VectorEntry vectorEntry : p0) {
            vectorEntry.set(1. / (double) adjacency.numRows());
        }


        // scale the constant term - so we can reuse it
        p0.scale(1. - c);


        Vector p_k;
        Vector p_k1 = new DenseVector(p0);

        int counter = 0;

        // iterate
        // P_k+1 = c * AT * p_k + p_0

        do {
            p_k = p_k1.copy();
            p_k1 = adjacency.transMultAdd(c, p_k, p0.copy());

            counter++;

        } while (p_k1.copy().add(-1, p_k).norm(Vector.Norm.Two) > 0.0000000001);

        stopwatch.stop();

        String fullClassName = adjacency.getClass().getName();
        String shortenedClassName = fullClassName.substring(fullClassName.lastIndexOf('.') + 1);
        LOG.info("{} Converged after {} : {} in {}", String.format("%-17s", shortenedClassName), counter, p_k1.norm(Vector.Norm.One), stopwatch);

        return p_k1;
    }

}
