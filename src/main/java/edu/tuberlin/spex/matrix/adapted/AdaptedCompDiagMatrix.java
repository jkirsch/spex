package edu.tuberlin.spex.matrix.adapted;

import no.uib.cipr.matrix.AbstractMatrix;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * 25.04.2015.
 *
 * Compressed diagonal storage (CDS) matrix - adapted with byte buffers
 */
public class AdaptedCompDiagMatrix extends AbstractMatrix implements Value {

    // The data for this matrix serialized into a bytebuffer
    ByteBuffer byteBuffer;

    /**
     * The diagonals
     */
    double[][] diag;

    /**
     * Indices to the start of the diagonal, relative to the main diagonal.
     * Positive means the number of diagonals shifted up, while negative is the
     * number of diagonals shifted down
     */
    IntBuffer ind;

    public AdaptedCompDiagMatrix(int numRows, int numColumns) {
        super(numRows, numColumns);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        // write the current data
        out.writeInt(numRows);
        out.writeInt(numColumns);

        // write array sizes
        out.writeInt(ind.limit());

    }

    @Override
    public void read(DataInputView in) throws IOException {
        numRows = in.readInt();
        numColumns = in.readInt();

        // read array sizes
        int indSize = in.readInt();

        //byteBuffer = ByteBuffer.allocate(indSize * 4 + rowSize * 4 + dataSize * 8);
        in.read(byteBuffer.array());

        ind = (IntBuffer) byteBuffer.asIntBuffer().limit(indSize);
    }


}
