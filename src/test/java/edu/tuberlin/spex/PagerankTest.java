package edu.tuberlin.spex;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import com.google.common.primitives.Ints;
import edu.tuberlin.spex.algorithms.PageRank;
import edu.tuberlin.spex.algorithms.Reordering;
import edu.tuberlin.spex.utils.CompressionHelper;
import edu.tuberlin.spex.utils.Datasets;
import edu.tuberlin.spex.utils.MatrixMarketWriter;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.*;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.core.Is.is;

/**
 * Date: 11.01.2015
 * Time: 00:41
 */
@RunWith(Parameterized.class)
public class PagerankTest {

    Logger LOG = LoggerFactory.getLogger(PagerankTest.class);

    Datasets datasets = new Datasets();

    @Parameterized.Parameters(name = "{index}: Dataset {0}")
    public static Collection<Datasets.GRAPHS> data() {
        return Arrays.asList(
                Datasets.GRAPHS.dblp,
                Datasets.GRAPHS.webBerkStan,
                Datasets.GRAPHS.webStanford,
                Datasets.GRAPHS.webNotreDame
                //Datasets.GRAPHS.patents,
                //Datasets.GRAPHS.youtTube,
                //Datasets.GRAPHS.liveJournal
             );
    }

    @Parameterized.Parameter
    public Datasets.GRAPHS dataset;

    @Test
    public void testPageRank() throws Exception {

        Path path = datasets.get(dataset);
        Assume.assumeTrue("Example data does not exist at " + path, Files.exists(path));

        // This is the dimension of the data
        final GraphInfo graphInfo = getDimension(path);

        // Generate a scaled image
        final int max_scaled_size = 1000;

        final Map<Integer, Integer> nodeIdMapping = Maps.newHashMapWithExpectedSize(graphInfo.MAX);

        final FlexCompRowMatrix readAdjacency = new FlexCompRowMatrix(graphInfo.MAX, graphInfo.MAX);

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

                if ((row < graphInfo.MAX) && (column < graphInfo.MAX)) {
                    readAdjacency.add(row, column, 1d);

                    if (!graphInfo.directed) {
                        readAdjacency.add(column, row, 1d);
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
        LOG.info(graphInfo.toString());
        LOG.info("Counted Nodes {}, Read {} edges ... directed={}, sparseness = {}, sparseness estimate = {}", nodeIdMapping.size(), counts, graphInfo.directed, sparseness, sparseness * max_scaled_size * max_scaled_size);


        // build picture
        createImage(readAdjacency, dataset, max_scaled_size, "normal");

        Matrix orderByRowSumMatrix = Reordering.orderByRowSum(readAdjacency);
        Matrix orderByColumnSumMatrix = Reordering.orderByColumnSum(readAdjacency);

        // check that we are not distorting the matrix
        Assert.assertThat(orderByRowSumMatrix.norm(Matrix.Norm.Frobenius), is(readAdjacency.norm(Matrix.Norm.Frobenius)));
        Assert.assertThat(orderByColumnSumMatrix.norm(Matrix.Norm.Frobenius), is(readAdjacency.norm(Matrix.Norm.Frobenius)));

        // build picture
        createImage(orderByRowSumMatrix, dataset, max_scaled_size, "sortedByRow");

        createImage(orderByColumnSumMatrix, dataset, max_scaled_size, "sortedByColumn");

        File file = new File("datasets", dataset.name() + ".mtx");
        MatrixMarketWriter.write(readAdjacency, file);

       // calcPageRank(graphInfo, readAdjacency);
       // calcPageRank(graphInfo, new FlexCompRowMatrix(orderByColumnSumMatrix));

    }

    private void calcPageRank(GraphInfo graphInfo, FlexCompRowMatrix cols) {

        MatrixStatisticsCollector matrixStatisticsCollector = new MatrixStatisticsCollector();

        // divide by column sum
        for (int i = 0; i < cols.numRows(); i++) {

            SparseVector row = cols.getRow(i);
            double sum = row.norm(Vector.Norm.One);

            if (sum > 0) {
                row.scale(1. / sum);
            } else {
                // row.set(p0.copy());
            }

            matrixStatisticsCollector.addValue(row.getUsed());
        }

        LOG.info(matrixStatisticsCollector.toString());

        PageRank pageRank = new PageRank(0.85);

        Vector p_k1 = pageRank.calc(new FlexCompColMatrix(cols));
        p_k1 = pageRank.calc(cols);
        p_k1 = pageRank.calc(new CompRowMatrix(cols));
        p_k1 = pageRank.calc(new CompColMatrix(cols));


        Multiset<Long> ranks = TreeMultiset.create(Ordering.natural());

        p_k1.scale(1. / p_k1.norm(Vector.Norm.One));
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
                if (line.startsWith("#")) {
                    LOG.info(line);
                }

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

    public static BufferedImage getScaledImage(BufferedImage image, int width, int height) throws IOException {
        int imageWidth  = image.getWidth();
        int imageHeight = image.getHeight();

        double scaleX = (double) width / imageWidth;
        double scaleY = (double) height / imageHeight;
        AffineTransform scaleTransform = AffineTransform.getScaleInstance(scaleX, scaleY);
        AffineTransformOp bilinearScaleOp = new AffineTransformOp(scaleTransform, AffineTransformOp.TYPE_BICUBIC);

        return bilinearScaleOp.filter(
                image,
                new BufferedImage(width, height, image.getType()));
    }

    private void createImage(final Matrix adjacency, Datasets.GRAPHS dataset, int max_scaled_size, String name) throws IOException {

        final BufferedImage bufferedImage = new BufferedImage(max_scaled_size, max_scaled_size, BufferedImage.TYPE_3BYTE_BGR);
        Graphics2D g2 = bufferedImage.createGraphics();
        g2.setColor(Color.WHITE);
        g2.fillRect(0, 0, max_scaled_size, max_scaled_size);

        // Downsample
        final DenseMatrix downSample = new DenseMatrix(max_scaled_size, max_scaled_size);

        int counter = 0;
        final double scale = max_scaled_size / (double) adjacency.numColumns();

        for (MatrixEntry matrixEntry : adjacency) {
            int x = (int) (matrixEntry.column() * scale);
            int y = (int) (matrixEntry.row() * scale);
            downSample.set(x, y, downSample.get(x, y) + 1);
            counter++;
        }

        EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution(counter / 10);
        empiricalDistribution.load(downSample.getData());
        List<SummaryStatistics> binStats = empiricalDistribution.getBinStats();

        // sort in the order of least elements start painting in reverse order

        ArrayList<MyPoint> matrixEntries = Lists.newArrayList(Iterables.transform(downSample, new Function<MatrixEntry, MyPoint>() {
            @Override
            public MyPoint apply(MatrixEntry input) {
                // transform
                return new MyPoint(input.column(), input.row(), input.get());
            }
        }));
        Collections.sort(matrixEntries, Ordering.natural().onResultOf(new Function<MyPoint, Comparable>() {
            @Override
            public Comparable apply(MyPoint input) {
                return input.getValue();
            }
        }));

        for (MyPoint matrixEntry : matrixEntries) {

            final double value = matrixEntry.getValue();
            // which bin
            int bin = 0;
            for (SummaryStatistics binStat : binStats) {
                if (value >= binStat.getMin() && value <= binStat.getMax()) {
                    break;
                }
                bin++;
            }

            int i = bin; //(int) ((colors.length / (double) empiricalDistribution.getBinCount()) * bin);

            Color color1 = Color.blue.brighter();
            Color color2 = Color.blue.darker();

            // filling onm the complete graph
            double filling = (counter / (double) (adjacency.numRows() * adjacency.numColumns()));

            // how large os the window

            double estimate = matrixEntry.getValue() * ( scale * scale) ;

            if (matrixEntry.getValue() >= empiricalDistribution.getNumericalMean()) {

            //if (i >= 1) {
                int size = 10;// + i;

                double ratio = bin / (double) empiricalDistribution.getBinCount();

                int red = (int) (color2.getRed() * ratio + color1.getRed() * (1 - ratio));
                int green = (int) (color2.getGreen() * ratio + color1.getGreen() * (1 - ratio));
                int blue = (int) (color2.getBlue() * ratio + color1.getBlue() * (1 - ratio));
                Color stepColor = new Color(red, green, blue);

                //255 - ((int) ((10 / maxValue) * value) + 155);
                //Color color = new Color(colorValue, colorValue, colorValue);
                g2.setColor(stepColor);
                int x = matrixEntry.getX();
                int y = matrixEntry.getY();
                //g2.fillOval(x - 5, y - 5, size, size);
                bufferedImage.setRGB(x, y, stepColor.getRGB());
                //}
            }
        }


        // bufferedImage.setRGB(x, y, Color.BLACK.getRGB());

        File images = new File("images");
        images.mkdir();

        String prefix = dataset.name() + "-" + name;
        ImageIO.write(bufferedImage, "png", new File(images, prefix + "-adjacency.png"));
       // ImageIO.write(getScaledImage(bufferedImage, 100, 100), "png", new File(images, prefix + "-adjacency-small.png"));
        LOG.info("Written adjacency graph file to: adjacency.png");
    }

    private static class GraphInfo {
        int MAX;
        boolean directed;

        public GraphInfo(int MAX, boolean directed) {
            this.MAX = MAX;
            this.directed = directed;
        }

        @Override
        public String toString() {
            return "GraphInfo{" +
                    "MAX=" + MAX +
                    ", directed=" + directed +
                    '}';
        }
    }

    private static class MyPoint {
        int x;
        int y;
        double value;

        public MyPoint(int x, int y, double value) {
            this.x = x;
            this.y = y;
            this.value = value;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        public double getValue() {
            return value;
        }
    }

    private static class MatrixStatisticsCollector {

        long sum = 0;
        long sum_squared = 0;
        int count = 0;

        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        // Number of non-zeros per line
        public void addValue(int value) {
            sum += value;

            count++;

            max = Math.max(max, value);
            min = Math.min(max, value);

            // Variance counter
            sum_squared += value * value;

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

        public double getStdDev() {
            // based on http://math.stackexchange.com/questions/20593/calculate-variance-from-a-stream-of-sample-values#answer-20596
            // sig^2 = 1 / N * [ Sum_x_i^2 + ( sum_xi ) ^2 / N  ]
            return 1 / (double) count * (sum_squared - ((sum * sum) / (double) count));
        }


        @Override
        public String toString() {
            return "MatrixStatisticsCollector{" +
                    "max=" + max +
                    ", min=" + min +
                    ", avg=" + getAvg() +
                    ", std=" + getStdDev() +
                    '}';
        }
    }



}
