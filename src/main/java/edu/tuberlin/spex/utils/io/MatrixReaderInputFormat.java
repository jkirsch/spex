package edu.tuberlin.spex.utils.io;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import edu.tuberlin.spex.utils.CompressionHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.IntParser;

import java.io.*;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * Date: 29.01.2015
 * Time: 22:48
 */
public class MatrixReaderInputFormat extends DelimitedInputFormat<Tuple3<Integer, Integer, Double>> {

    private static final long serialVersionUID = 1L;

    /**
     * Code of \r, used to remove \r from a line when the line ends with \r\n
     */
    private static final byte CARRIAGE_RETURN = (byte) '\r';

    /**
     * Code of \n, used to identify if \n is used as delimiter
     */
    private static final byte NEW_LINE = (byte) '\n';
    private final int indexOffset;
    private final int size;
    private final boolean transpose;

    /**
     * The name of the charset to use for decoding.
     */
    private String charsetName = "UTF-8";

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        if (charsetName == null) {
            throw new IllegalArgumentException("Charset must not be null.");
        }

        this.charsetName = charsetName;
    }


    /**
     * Default to 1 based file
     *
     * @param filePath
     */
    public MatrixReaderInputFormat(Path filePath) {
        this(filePath, -1);
    }

    public MatrixReaderInputFormat(Path filePath, int indexOffset) {
        this(filePath, indexOffset, Integer.MAX_VALUE, false);
    }

    public MatrixReaderInputFormat(Path filePath, int indexOffset, int size, boolean transpose) {
        super(filePath);
        this.indexOffset = indexOffset;
        this.size = size;
        this.transpose = transpose;
    }


    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (charsetName == null || !Charset.isSupported(charsetName)) {
            throw new RuntimeException("Unsupported charset: " + charsetName);
        }

    }

    private static Pattern WHITESPACE_PATTERN = Pattern.compile("[\\s]+");

    @Override
    public Tuple3<Integer, Integer, Double> readRecord(Tuple3<Integer, Integer, Double> reuse, byte[] bytes, int offset, int numBytes) throws IOException {

        //Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
        if (this.getDelimiter() != null && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE && offset + numBytes >= 1
                && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
            numBytes -= 1;
        }

        String value = new String(bytes, offset, numBytes, this.charsetName);

        if (!StringUtils.isEmpty(value) && !StringUtils.startsWithAny(value, "//", "%")) {
            //Iterable<String> split = Splitter.on(WHITESPACE_PATTERN).split(value.trim());
            //Iterator<String> iterator = split.iterator();
            try {
                int start = offset;
                while (bytes[start] == ' ' && start < offset + numBytes) start++;
                int row = IntParser.parseField(bytes, start, numBytes, ' ')  + indexOffset;

                // now iterate further
                while (bytes[start] != ' ' && start < offset + numBytes) start++;
                while (bytes[start] == ' ' && start < offset + numBytes) start++;

                int column = IntParser.parseField(bytes, start, numBytes, ' ')  + indexOffset;

            //    int row = Integer.parseInt(iterator.next()) + indexOffset;
            //    int column = Integer.parseInt(iterator.next()) + indexOffset;
                double matrixEntry;
                //if(iterator.hasNext()) {
                //matrixEntry = Double.parseDouble(iterator.next());
                //} else {
                  matrixEntry = 1;
                //}

                if (row - indexOffset == size) {
                    // this element is the matrix size -- skip it
                    return null;
                }

                Preconditions.checkElementIndex(row, size, "row");
                Preconditions.checkElementIndex(column, size, "col");

                if (!transpose) {
                    reuse.setFields(row, column, matrixEntry);
                } else {
                    reuse.setFields(column, row, matrixEntry);
                }
                return reuse;

            } catch (java.util.NoSuchElementException e) {
                throw new IllegalArgumentException("Error parsing line >" + value + "<", e);
            }

        }

        return null;
    }

    public static Integer getSize(String filename) throws IOException {
        // get Dimensions

        BufferedReader readable = null;
        try {
            FileInputStream fileInputStream = new FileInputStream(filename);
            InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);
            // Iterate over the lines
            readable = new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8));

            return CharStreams.readLines(readable, new LineProcessor<Integer>() {

                public int value;

                @Override
                public boolean processLine(String line) throws IOException {
                    // Example
                    /*
                    %%MatrixMarket matrix coordinate real general
                    %Matrix generated automatically on Sun Feb 22 17:02:04 CET 2015
                    325729     325729             1497134
                    */
                    if (StringUtils.isEmpty(line) || StringUtils.startsWith(line, "%")) return true;
                    // parse the size
                    value = Integer.parseInt(Splitter.on(WHITESPACE_PATTERN).split(line.trim()).iterator().next());
                    return false;
                }

                @Override
                public Integer getResult() {
                    return value;
                }
            });


        } finally {
            IOUtils.closeQuietly(readable);
        }

    }
}
