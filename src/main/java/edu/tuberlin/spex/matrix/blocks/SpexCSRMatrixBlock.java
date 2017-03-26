package edu.tuberlin.spex.matrix.blocks;

import com.google.common.base.Preconditions;
import no.uib.cipr.matrix.DenseVector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;

/**
 * Created by Johannes on 14.04.2016.
 */
public class SpexCSRMatrixBlock extends SpexMatrixBlock {

    /**
     * The buffer holds all the data.
     */
    private ByteBuffer byteBuffer;


    /**
     * Matrix data - view.
     */
    DoubleBuffer data;

    /**
     * Column indices. These are kept sorted within each row.
     */
    IntBuffer columnIndex;

    /**
     * Indices to the start of each row
     */
    IntBuffer rowPointer;

    @Override
    public void mult(DenseVector v) {

    }

    @Override
    public void write(DataOutputView out) throws IOException {

    }

    @Override
    public void read(DataInputView in) throws IOException {
        numRows = in.readInt();
        numColumns = in.readInt();

        // read array sizes
        int colSize = in.readInt();
        int rowSize = in.readInt();
        int dataSize = in.readInt();


        byteBuffer = ByteBuffer.allocateDirect(colSize * 4 + rowSize * 4 + dataSize * 8);
        int read = in.read(byteBuffer.array());

        Preconditions.checkArgument(read == byteBuffer.capacity(), "Did not read enough elements");

        columnIndex = (IntBuffer) byteBuffer.asIntBuffer().limit(colSize);

        // advance the buffer
        byteBuffer.position(colSize * 4);

        rowPointer = (IntBuffer) byteBuffer.asIntBuffer().limit(rowSize);

        byteBuffer.position(colSize * 4 + rowSize * 4);

        data = byteBuffer.asDoubleBuffer();

    }
}
