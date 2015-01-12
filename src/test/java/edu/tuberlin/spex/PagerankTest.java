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
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.junit.Assume;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 11.01.2015
 * Time: 00:41
 */
public class PagerankTest {

    String testData2 = "data/web-NotreDame.txt.gz";
    String testData = "data/web-BerkStan.txt.gz";
    String testData3 = "data/web-Stanford.txt.gz";

    @Test
    public void testPageRank() throws Exception {

        Assume.assumeTrue("Example data does not exist at " + testData, new File(testData).exists());

        // This is the dimension of the data - fixed to a particular dataset
        final int MAX = getDimension(testData);//685230;

        // Generate a scaled image
        final int max_scaled_size = 1000;
        final double scale = max_scaled_size / (double) MAX;

        final int[][] image = new int[max_scaled_size][max_scaled_size];

        // determine image matrix size - sub-sample size (1/scale)

        final BufferedImage bufferedImage = new BufferedImage(max_scaled_size, max_scaled_size, BufferedImage.TYPE_BYTE_BINARY);
        Graphics2D g2 = bufferedImage.createGraphics();
        g2.setColor(Color.WHITE);
        g2.fillRect(0, 0, max_scaled_size, max_scaled_size);

        // Highlight some pages (?)
        //g2.setColor(Color.RED);
        //int y1 = (int) (272918 * scale);
        //int y2 = (int) (438237 * scale);
        //int xMax = (int) (MAX * scale);

        //g2.drawLine(0, y1, xMax, y1);
        //g2.drawLine(0, y2, xMax, y2);

        FileInputStream fileInputStream = new FileInputStream(testData);
        InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);

        final FlexCompRowMatrix adjacency = new FlexCompRowMatrix(MAX, MAX);

        // Iterate over the lines
        CharStreams.readLines(new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8)), new LineProcessor<Object>() {

            long counter = 0;

            @Override
            public boolean processLine(String line) throws IOException {
                if (line.startsWith("#") || line.length() == 0) {
                    return true;
                }

                String[] splits = line.split("\t");

                int row = Integer.parseInt(splits[0]) - 1;
                int column = Integer.parseInt(splits[1]) - 1;

                int x = (int) (row * scale);
                int y = (int) (column * scale);

                image[x][y]++;

                adjacency.add(row, column, 1d);

                if (++counter % 100000 == 0) System.out.printf("Read %7d edges ...\n", counter);

                return true;
            }

            @Override
            public Object getResult() {
                return null;
            }
        });


        // build picture
        // generate submatrix
        // slide a window


        for (int x = 0; x < max_scaled_size; x++) {
            for (int y = 0; y < max_scaled_size; y++) {
                if(image[x][y] >= 7) {
                    bufferedImage.setRGB(x, y, Color.BLACK.getRGB());
                }
            }
        }


        // bufferedImage.setRGB(x, y, Color.BLACK.getRGB());


        ImageIO.write(bufferedImage, "png", new File("adjacency.png"));
        System.out.println("Written adjacency graph file to: adjacency.png");


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

        System.out.println("Converged after " + counter + " : " + p_k1.norm(Vector.Norm.One) + " in " + stopwatch);

        Multiset<Long> ranks = TreeMultiset.create(Ordering.natural());

        p_k1.scale(MAX);

        for (VectorEntry vectorEntry : p_k1) {
            long round = Math.round(Math.log(vectorEntry.get()));
            ranks.add(round);
            if (round == 9) {
                //  System.out.println(vectorEntry.index() + " - " + adjacency.getRow(vectorEntry.index()));
            }
        }

        System.out.println("Rank Distribution : log normalized ranks");
        System.out.println(ranks);

    }

    // get Dimensions
    private int getDimension(String filename) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(filename);
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
}
