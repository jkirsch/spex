package edu.tuberlin.spex;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
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
import java.util.Map;
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

        // This is the dimension of the data
        final GraphInfo graphInfo = getDimension(path);

        // Generate a scaled image
        final int max_scaled_size = 1000;
        final double scale = max_scaled_size / (double) graphInfo.MAX;

        final int[][] image = new int[max_scaled_size][max_scaled_size];
        final Map<Integer, Integer> nodeIdMapping = Maps.newHashMapWithExpectedSize(graphInfo.MAX);

        final FlexCompRowMatrix adjacency = new FlexCompRowMatrix(graphInfo.MAX, graphInfo.MAX);

        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);

        // Iterate over the lines
        Long counts = CharStreams.readLines(new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8)), new LineProcessor<Long>() {

            long counter = 0;

            @Override
            public boolean processLine(String line) throws IOException {
                if (line.startsWith("#") || line.length() == 0) {
                    return true;
                }

                String[] splits = line.split("\t");

                // check if we have seen these nodes ?

                int fromNode = Integer.parseInt(splits[0]);
                int toNode = Integer.parseInt(splits[1]);

                // add to the mapping
                if (!nodeIdMapping.containsKey(fromNode)) {
                    nodeIdMapping.put(fromNode, nodeIdMapping.size());
                }
                if (!nodeIdMapping.containsKey(toNode)) {
                    nodeIdMapping.put(toNode, nodeIdMapping.size());
                }

                int row = nodeIdMapping.get(fromNode);
                int column = nodeIdMapping.get(toNode);

                int x = (int) (row * scale);
                int y = (int) (column * scale);

                if ((row < graphInfo.MAX) && (column < graphInfo.MAX)) {
                    image[x][y]++;
                    adjacency.add(row, column, 1d);

                    if (!graphInfo.directed) {
                        image[y][x]++;
                        adjacency.add(column, row, 1d);
                    }

                    if (++counter % 100000 == 0) LOG.info("Read {} edges ...", counter);
                }

                return true;
            }

            @Override
            public Long getResult() {
                return counter;
            }
        });

        double sparseness = Math.exp(Math.log(counts + (!graphInfo.directed ? counts : 0)) - 2 * Math.log(graphInfo.MAX)); // count / max^2
        LOG.info("Counted Nodes {}, Read {} edges ... directed={}, sparseness = {}, sparseness estimate = {}", nodeIdMapping.size(), counts, graphInfo.directed, sparseness, sparseness * max_scaled_size * max_scaled_size);


        // build picture
        createImage(max_scaled_size, image, sparseness * max_scaled_size * max_scaled_size);


        // init p with = 1/n
        Vector p0 = new DenseVector(adjacency.numRows());
        for (VectorEntry vectorEntry : p0) {
            vectorEntry.set(1. / (double) graphInfo.MAX);
        }

        Stopwatch stopwatch = new Stopwatch().start();

        MatrixStatisticsCollector matrixStatisticsCollector = new MatrixStatisticsCollector();

        // divide by column sum
        for (int i = 0; i < adjacency.numRows(); i++) {

            SparseVector row = adjacency.getRow(i);
            double sum = row.norm(Vector.Norm.One);

            if (sum > 0) {
                row.scale(1. / sum);
            }

            matrixStatisticsCollector.addValue(row.getUsed());
        }

        LOG.info(matrixStatisticsCollector.toString());


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

        } while (p_k1.copy().add(-1, p_k).norm(Vector.Norm.Two) > 0.0000001);

        stopwatch.stop();

        LOG.info("Converged after {} : {} in {}", counter, p_k1.norm(Vector.Norm.One), stopwatch);

        Multiset<Long> ranks = TreeMultiset.create(Ordering.natural());

        p_k1.scale(graphInfo.MAX);

        for (VectorEntry vectorEntry : p_k1) {
            long round = Math.round(Math.log(vectorEntry.get()));
            ranks.add(round);
        }

        LOG.info("Rank Distribution : log normalized ranks");
        LOG.info(ranks.toString());

    }

    // get Dimensions
    private GraphInfo getDimension(Path filename) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(filename.toFile());
        InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);

        // Iterate over the lines
        BufferedReader readable = new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8));
        GraphInfo graphInfo = CharStreams.readLines(readable, new LineProcessor<GraphInfo>() {

            Pattern size = Pattern.compile("#\\s+Nodes: (\\d+) Edges: (\\d+)");

            Pattern type = Pattern.compile("# (Undirected|Directed) graph");

            int dimension = 0;
            boolean directed = false;

            @Override
            public boolean processLine(String line) throws IOException {

                Matcher sizeLineMatches = size.matcher(line);
                Matcher graphType = type.matcher(line);
                LOG.info(line);

                if (graphType.find()) {
                    if (graphType.group(1).equalsIgnoreCase("Directed")) {
                        directed = true;
                    }
                }

                if (sizeLineMatches.find()) {
                    dimension = Ints.tryParse(sizeLineMatches.group(1));
                    return false;
                }

                return true;

            }

            @Override
            public GraphInfo getResult() {
                return new GraphInfo(dimension, directed);
            }
        });

        readable.close();

        return graphInfo;
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

    private static class GraphInfo {
        int MAX;
        boolean directed;

        public GraphInfo(int MAX, boolean directed) {
            this.MAX = MAX;
            this.directed = directed;
        }
    }

    private static class MatrixStatisticsCollector {

        long sum = 0;
        int count = 0;

        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        // Number of non-zeros per line
        public void addValue(int value) {
            sum+=value;
            count++;

            max = Math.max(max, value);
            min = Math.min(max, value);
        }

        public int getMax() {
            return max;
        }

        public int getMin() {
            return min;
        }

        public double getAvg() {
            return sum / (double) count;
        }

        @Override
        public String toString() {
            return "MatrixStatisticsCollector{" +
                    "max=" + max +
                    ", min=" + min +
                    ", avg=" + getAvg() +
                    '}';
        }
    }
}
