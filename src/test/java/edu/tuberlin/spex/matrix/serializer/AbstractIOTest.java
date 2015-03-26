package edu.tuberlin.spex.matrix.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompColMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.commons.io.IOUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.Before;

import java.io.*;

/**
 * Date: 10.02.2015
 * Time: 23:43
 */
public abstract class AbstractIOTest {

    private Kryo kryo;
    private File tempFile;

    @Before
    public void setUp() throws Exception {
        kryo = new Kryo();

        kryo.addDefaultSerializer(DenseMatrix.class, new DenseMatrixSerializer());
        kryo.addDefaultSerializer(LinkedSparseMatrix.class, new LinkedSparseMatrixSerializer());
        kryo.addDefaultSerializer(AdaptedCompColMatrix.class, new AdaptedCompColMatrixSerializer());
        kryo.addDefaultSerializer(DenseVector.class, new DenseVectorSerializer());

        tempFile = File.createTempFile("prefix", "ser");
        tempFile.deleteOnExit();

    }

    public void serialize(Object object) throws FileNotFoundException {
        Output output = null;
        try {
            FileOutputStream outputStream = new FileOutputStream(tempFile);
            output = new Output(outputStream);
            kryo.writeObject(output, object);

        } finally {

            IOUtils.closeQuietly(output);
        }

    }

    public <T> T deserialize(Class<T> type) throws FileNotFoundException {

        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(tempFile);
            Input input = new Input(inputStream);
            return kryo.readObject(input, type);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    static final class TestOutputView extends DataOutputStream implements DataOutputView {

        public TestOutputView() {
            super(new ByteArrayOutputStream(4096));
        }

        public TestInputView getInputView() {
            ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
            return new TestInputView(baos.toByteArray());
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            for (int i = 0; i < numBytes; i++) {
                write(0);
            }
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {
            byte[] buffer = new byte[numBytes];
            source.readFully(buffer);
            write(buffer);
        }
    }

    private static final class TestInputView extends DataInputStream implements DataInputView {

        public TestInputView(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public void skipBytesToRead(int numBytes) throws IOException {
            while (numBytes > 0) {
                int skipped = skipBytes(numBytes);
                numBytes -= skipped;
            }
        }
    }
}
