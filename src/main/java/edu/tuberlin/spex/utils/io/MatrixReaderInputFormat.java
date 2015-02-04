package edu.tuberlin.spex.utils.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Scanner;

/**
 * Date: 29.01.2015
 * Time: 22:48
 *
 */
public class MatrixReaderInputFormat extends DelimitedInputFormat<Tuple3<Integer, Integer, Float>> {

    private static final long serialVersionUID = 1L;

    /**
     * Code of \r, used to remove \r from a line when the line ends with \r\n
     */
    private static final byte CARRIAGE_RETURN = (byte) '\r';

    /**
     * Code of \n, used to identify if \n is used as delimiter
     */
    private static final byte NEW_LINE = (byte) '\n';

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


    public MatrixReaderInputFormat(Path filePath) {
        super(filePath);
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (charsetName == null || !Charset.isSupported(charsetName)) {
            throw new RuntimeException("Unsupported charset: " + charsetName);
        }

    }

    @Override
    public Tuple3<Integer, Integer, Float> readRecord(Tuple3<Integer, Integer, Float> reuse, byte[] bytes, int offset, int numBytes) throws IOException {

        //Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
        if (this.getDelimiter() != null && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
                && bytes[offset+numBytes-1] == CARRIAGE_RETURN){
            numBytes -= 1;
        }

        String value = new String(bytes, offset, numBytes, this.charsetName);


        if (!StringUtils.isEmpty(value) && !StringUtils.startsWithAny(value, "//", "%")) {
            Scanner scanner = new Scanner(value);
            scanner.useLocale(Locale.ENGLISH);
            int source = scanner.nextInt();
            int dest = scanner.nextInt();
            float v = scanner.nextFloat();
            if(v < 2) {
                reuse.setFields(source, dest, v);
                return reuse;
            }
        }

        return null;
    }
}
