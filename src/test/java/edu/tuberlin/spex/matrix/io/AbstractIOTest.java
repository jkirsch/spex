package edu.tuberlin.spex.matrix.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.tuberlin.spex.matrix.io.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.commons.io.IOUtils;
import org.junit.Before;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

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
        kryo.addDefaultSerializer(AdaptedCompRowMatrix.class, new AdaptedCompRowMatrixSerializer());

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
}
