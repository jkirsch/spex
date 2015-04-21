package edu.tuberlin.spex.utils.io;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import edu.tuberlin.spex.utils.CompressionHelper;
import no.uib.cipr.matrix.io.MatrixInfo;
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
    private static Pattern WHITESPACE_PATTERN = Pattern.compile("[\\s]+");
    private final int indexOffset;
    private final int size;
    private final boolean transpose;
    /**
     * The name of the charset to use for decoding.
     */
    private String charsetName = "UTF-8";

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

    public static MatrixInformation getMatrixInfo(String filename) throws IOException {
        // get Dimensions

        BufferedReader readable = null;
        try {
            FileInputStream fileInputStream = new FileInputStream(filename);
            InputStream decompressionStream = CompressionHelper.getDecompressionStream(fileInputStream);
            // Iterate over the lines
            readable = new BufferedReader(new InputStreamReader(decompressionStream, Charsets.UTF_8));

            return CharStreams.readLines(readable, new LineProcessor<MatrixInformation>() {

                public MatrixInfo matrixInfo;
                public MatrixInformation matrixInformation;

                @Override
                public boolean processLine(String line) throws IOException {
                    // Example
                    /*
                    %%MatrixMarket matrix coordinate real general
                    %Matrix generated automatically on Sun Feb 22 17:02:04 CET 2015
                    325729     325729             1497134
                    */
                    if(matrixInfo == null) {
                        matrixInfo = readMatrixInfo(line);
                        return true;
                    }

                    if (StringUtils.isEmpty(line) || StringUtils.startsWith(line, "%")) return true;
                    // parse the size
                    String[] dimensions = line.trim().split(" +");
                    Preconditions.checkPositionIndex(2, dimensions.length, line);

                    int n = Integer.parseInt(dimensions[0]);
                    int m = Integer.parseInt(dimensions[1]);
                    int values = Integer.parseInt(dimensions[2]);

                    matrixInformation = new MatrixInformation(matrixInfo, n, m, values);

                    return false;
                }

                @Override
                public MatrixInformation getResult() {
                    return matrixInformation;
                }
            });


        } finally {
            IOUtils.closeQuietly(readable);
        }

    }

/**
     * Reads the matrix info for the Matrix Market exchange format. The line
     * must consist of exactly 5 space-separated entries, the first being
     * "%%MatrixMarket"
     */
    private static MatrixInfo readMatrixInfo(String line) throws IOException {
        String[] component = line.trim().split(" +");
        if (component.length != 5)
            throw new IOException(
                    "Current line unparsable. It must consist of 5 tokens");

        // Read header
        if (!component[0].equalsIgnoreCase("%%MatrixMarket"))
            throw new IOException("Not in Matrix Market exchange format");

        // This will always be "matrix"
        if (!component[1].equalsIgnoreCase("matrix"))
            throw new IOException("Expected \"matrix\", got " + component[1]);

        // Sparse or dense?
        boolean sparse = false;
        if (component[2].equalsIgnoreCase("coordinate"))
            sparse = true;
        else if (component[2].equalsIgnoreCase("array"))
            sparse = false;
        else
            throw new IOException("Unknown layout " + component[2]);

        // Dataformat
        MatrixInfo.MatrixField field = null;
        if (component[3].equalsIgnoreCase("real"))
            field = MatrixInfo.MatrixField.Real;
        else if (component[3].equalsIgnoreCase("integer"))
            field = MatrixInfo.MatrixField.Integer;
        else if (component[3].equalsIgnoreCase("complex"))
            field = MatrixInfo.MatrixField.Complex;
        else if (component[3].equalsIgnoreCase("pattern"))
            field = MatrixInfo.MatrixField.Pattern;
        else
            throw new IOException("Unknown field specification " + component[3]);

        // Matrix pattern
        MatrixInfo.MatrixSymmetry symmetry = null;
        if (component[4].equalsIgnoreCase("general"))
            symmetry = MatrixInfo.MatrixSymmetry.General;
        else if (component[4].equalsIgnoreCase("symmetric"))
            symmetry = MatrixInfo.MatrixSymmetry.Symmetric;
        else if (component[4].equalsIgnoreCase("skew-symmetric"))
            symmetry = MatrixInfo.MatrixSymmetry.SkewSymmetric;
        else if (component[4].equalsIgnoreCase("Hermitian"))
            symmetry = MatrixInfo.MatrixSymmetry.Hermitian;
        else
            throw new IOException("Unknown symmetry specification "
                    + component[4]);

        // Pack together. This also verifies the format
        return new MatrixInfo(sparse, field, symmetry);
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        if (charsetName == null) {
            throw new IllegalArgumentException("Charset must not be null.");
        }

        this.charsetName = charsetName;
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (charsetName == null || !Charset.isSupported(charsetName)) {
            throw new RuntimeException("Unsupported charset: " + charsetName);
        }

    }

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

        public static class MatrixInformation {

        MatrixInfo matrixInfo;
        int n;
        int m;
        long values;

        public MatrixInformation(MatrixInfo matrixInfo, int n, int m, long values) {
            this.matrixInfo = matrixInfo;
            this.n = n;
            this.m = m;
            this.values = values;
        }

        @Override
        public String toString() {
            return "MatrixInformation{" +
                    "matrixInfo=" + matrixInfo +
                    ", n=" + n +
                    ", m=" + m +
                    ", values=" + values +
                    '}';
        }

        public MatrixInfo getMatrixInfo() {
            return matrixInfo;
        }

        public int getN() {
            return n;
        }

        public int getM() {
            return m;
        }

        public long getValues() {
            return values;
        }
    }
public static class MatrixInformation {

        MatrixInfo matrixInfo;
        int n;
        int m;
        long values;

        public MatrixInformation(MatrixInfo matrixInfo, int n, int m, long values) {
            this.matrixInfo = matrixInfo;
            this.n = n;
            this.m = m;
            this.values = values;
        }

        @Override
        public String toString() {
            return "MatrixInformation{" +
                    "matrixInfo=" + matrixInfo +
                    ", n=" + n +
                    ", m=" + m +
                    ", values=" + values +
                    '}';
        }

        public MatrixInfo getMatrixInfo() {
            return matrixInfo;
        }

        public int getN() {
            return n;
        }

        public int getM() {
            return m;
        }

        public long getValues() {
            return values;
        }
    }
}
