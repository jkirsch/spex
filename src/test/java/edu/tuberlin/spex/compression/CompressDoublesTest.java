package edu.tuberlin.spex.compression;

import com.github.kutschkem.fpc.FpcCompressor;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;

/**
 * Created by Johannes on 08.04.2016.
 */
public class CompressDoublesTest {

    @Test
    public void compress() throws Exception {


        double[] data = new double[10000];

        FpcCompressor fpcCompressor = new FpcCompressor();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
        fpcCompressor.compress(byteBuffer, data);

        System.out.println("Before: "  + byteBuffer.position());

        byteBuffer.flip();
        System.out.println("After: "  + byteBuffer.position());

        System.out.println(byteBuffer.limit());

        System.out.println(byteBuffer.arrayOffset());

        double[] re = new double[10000];

        fpcCompressor.decompress(byteBuffer, re);

        Assert.assertThat(re, is(data));

    }

    @Test
    public void compressMatrix() throws Exception {

        AdaptedCompRowMatrix matrix = new AdaptedCompRowMatrix(new DenseMatrix(5, 5));
        System.out.println(matrix);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();


        TestDataOutputView out = new TestDataOutputView(baos);
        matrix.write(out);

        TestDataInputView input = new TestDataInputView(new ByteArrayInputStream(baos.toByteArray()));

        matrix.read(input);

        System.out.println(matrix);

        //System.out.println(Arrays.toString(out.getBuffer()));


    }

    public static class TestDataOutputView extends DataOutputStream implements DataOutputView {

        /**
         * Creates a new data output stream to write data to the specified
         * underlying output stream. The counter <code>written</code> is
         * set to zero.
         *
         * @param out the underlying output stream, to be saved for later
         *            use.
         * @see FilterOutputStream#out
         */
        public TestDataOutputView(OutputStream out) {
            super(out);
        }

        @Override
        public void skipBytesToWrite(int i) throws IOException {

        }

        @Override
        public void write(DataInputView dataInputView, int i) throws IOException {

        }
    }

    public static class TestDataInputView extends DataInputStream implements DataInputView {


        /**
         * Creates a DataInputStream that uses the specified
         * underlying InputStream.
         *
         * @param in the specified input stream
         */
        public TestDataInputView(InputStream in) {
            super(in);
        }

        @Override
        public void skipBytesToRead(int i) throws IOException {

        }
    }


}
