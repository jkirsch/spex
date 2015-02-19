package edu.tuberlin.spex.utils.io;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import com.google.common.primitives.Ints;
import edu.tuberlin.spex.utils.CompressionHelper;

import java.io.*;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 18.02.2015
 * Time: 20:37
 *
 */
public class MatrixMarketHelper {

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
                    //LOG.info(line);
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

    public static class GraphInfo {
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

}
