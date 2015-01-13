package edu.tuberlin.spex;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultiset;
import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import com.google.common.primitives.Ints;
import edu.tuberlin.spex.utils.CompressionHelper;
import edu.tuberlin.spex.utils.Datasets;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 11.01.2015
 * Time: 00:41
 */
public class PagerankTest {

    Logger LOG = LoggerFactory.getLogger(PagerankTest.class);

    Datasets datasets = new Datasets();

    @Test
    public void testPageRank() throws Exception {

        Path path = datasets.get(Datasets.GRAPHS.webStanford);
        Assume.assumeTrue("Example data does not exist at " + path, Files.exists(path));

        // This is the dimension of the data - fixed to a particular dataset
        final int MAX = getDimension(path);//685230;

        // Generate a scaled image
        final int max_scaled_size = 1000;
        final double scale = max_scaled_size / (double) MAX;

        final int[][] image = new int[max_scaled_size][max_scaled_size];

        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);

        final FlexCompRowMatrix adjacency = new FlexCompRowMatrix(MAX, MAX);

        // Iterate over the lines
        Long counts = CharStreams.readLines(new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8)), new LineProcessor<Long>() {

            long counter = 0;

            @Override
            public boolean processLine(String line) throws IOException {
                if (line.startsWith("#") || line.length() == 0) {
                    return true;
                }

                String[] splits = line.split("\t");

                int row = Integer.parseInt(splits[0]);
                int column = Integer.parseInt(splits[1]);

                int x = (int) (row * scale);
                int y = (int) (column * scale);

                if ((row < MAX) && (column < MAX)) {
                    image[x][y]++;

                    adjacency.add(row, column, 1d);
                    if (++counter % 100000 == 0) LOG.info("Read {} edges ...", counter);
                }

                return true;
            }

            @Override
            public Long getResult() {
                return counter;
            }
        });

        double sparseness = Math.exp(Math.log(counts) - 2 * Math.log(MAX)); // count / max^2
        LOG.info("Read {} edges ... sparseness = {}, sparseness estimate = {}", counts, sparseness, sparseness * max_scaled_size * max_scaled_size);


        // build picture
        createImage(max_scaled_size, image, sparseness * max_scaled_size * max_scaled_size);


        // init p with = 1/n
        Vector p0 = new DenseVector(adjacency.numRows());
        for (VectorEntry vectorEntry : p0) {
            vectorEntry.set(1. / (double) MAX);
        }

        Stopwatch stopwatch = new Stopwatch().start();

        // divide by column sum
        for (int i = 0; i < adjacency.numRows(); i++) {

            SparseVector row = adjacency.getRow(i);
            double sum = row.norm(Vector.Norm.One);

            if (sum > 0) {
                row.scale(1. / sum);
            }

        }


        // damping

        double c = 0.85d;

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

        } while (p_k1.copy().add(-1, p_k).norm(Vector.Norm.Two) > 0.0001);

        stopwatch.stop();

        LOG.info("Converged after {} : {} in {}", counter, p_k1.norm(Vector.Norm.One), stopwatch);

        Multiset<Long> ranks = TreeMultiset.create(Ordering.natural());

        p_k1.scale(MAX);

        for (VectorEntry vectorEntry : p_k1) {
            long round = Math.round(Math.log(vectorEntry.get()));
            ranks.add(round);
        }

        LOG.info("Rank Distribution : log normalized ranks");
        LOG.info(ranks.toString());

    }

    // get Dimensions
    private int getDimension(Path filename) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(filename.toFile());
        InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);

        // Iterate over the lines
        BufferedReader readable = new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8));
        int dim = CharStreams.readLines(readable, new LineProcessor<Integer>() {

            Pattern pattern = Pattern.compile("#\\s+Nodes: (\\d+) Edges: (\\d+)");

            int dimension = 0;

            @Override
            public boolean processLine(String line) throws IOException {

                Matcher matcher = pattern.matcher(line);

                if (matcher.find()) {
                    dimension = Ints.tryParse(matcher.group(1));
                    return false;
                }

                return true;

            }

            @Override
            public Integer getResult() {
                return dimension;
            }
        });

        readable.close();

        return dim;
    }

    private void createImage(int max_scaled_size, int[][] image, double v) throws IOException {

        final BufferedImage bufferedImage = new BufferedImage(max_scaled_size, max_scaled_size, BufferedImage.TYPE_BYTE_BINARY);
        Graphics2D g2 = bufferedImage.createGraphics();
        g2.setColor(Color.WHITE);
        g2.fillRect(0, 0, max_scaled_size, max_scaled_size);

        for (int x = 0; x < max_scaled_size; x++) {
            for (int y = 0; y < max_scaled_size; y++) {
                //Math.max(max_scaled_size*3, v / 4)
                if (image[x][y] >= Math.max(1, v / 4)) {
                    bufferedImage.setRGB(x, y, Color.BLACK.getRGB());
                }
            }
        }

        // bufferedImage.setRGB(x, y, Color.BLACK.getRGB());


        ImageIO.write(bufferedImage, "png", new File("adjacency.png"));
        LOG.info("Written adjacency graph file to: adjacency.png");

    }
}
