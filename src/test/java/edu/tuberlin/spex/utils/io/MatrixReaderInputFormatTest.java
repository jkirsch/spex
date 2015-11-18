package edu.tuberlin.spex.utils.io;

import com.google.common.base.Charsets;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MatrixReaderInputFormatTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRead() throws Exception {

        String fileContent =
                "%%MatrixMarket matrix coordinate real general\n" +
                "%Matrix generated automatically on Thu Feb 12 22:05:56 CET 2015\n" +
                "    325729     325729             1497134\n" +
                "         1          1  1.000000000000e+00\n" +
                "         1          2  1.000000000000e+00\n" +
                "         1          3  1.000000000000e+00\n" +
                "         1          4  1.000000000000e+00\n" +
                "         1          5  1.000000000000e+00\n" +
                "         1          6  1.000000000000e+00\n" +
                "         1          7  1.000000000000e+00";

        FileInputSplit tempFile = createTempFile(fileContent);

        MatrixReaderInputFormat format = new MatrixReaderInputFormat(tempFile.getPath(), 0, 325729, false);

        format.configure(new Configuration());
        format.open(tempFile);

        Tuple3<Integer, Integer, Double> result = new Tuple3<>();

        @SuppressWarnings("unchecked")
        Tuple3<Integer, Integer, Double>[] expected = new Tuple3[]{
                new Tuple3<>(1,1,1d),
                new Tuple3<>(1,2,1d),
                new Tuple3<>(1,3,1d),
                new Tuple3<>(1,4,1d),
                new Tuple3<>(1,5,1d),
                new Tuple3<>(1,6,1d),
                new Tuple3<>(1,7,1d)
        };


        int pos = -1;
        while (!format.reachedEnd()) {
            if(format.nextRecord(result) != null) {
                assertThat(result, is(expected[++pos]));
            };
        }
    }

    private FileInputSplit createTempFile(String content) throws IOException {

        File tempFile = folder.newFile("test_content.tmp");

        OutputStreamWriter wrt = new OutputStreamWriter(
                new FileOutputStream(tempFile), Charsets.UTF_8
        );
        wrt.write(content);
        wrt.close();

        return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[] {"localhost"});
    }
}