package edu.tuberlin.spex.compression;

import me.lemire.integercompression.*;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.CompRowMatrix;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;

/**
 * 17.06.2015
 */
public class SimpleCompressedMatrix {

    final double[] data;
    final int[] compressedColumnIndices;
    final int[] compressedRowPointer;
    final int ChunkSize = 128 * 20; // size of each chunk, choose a multiple of 128
    /**
     * Number of rows
     */
    final int numRows;
    /**
     * Number of columns
     */
    final int numColumns;
    IntegerCODEC regularcodec = new FastPFOR();
    IntegerCODEC ivb = new VariableByte();
    IntegerCODEC lastcodec = new Composition(regularcodec, ivb);


    public SimpleCompressedMatrix(CompRowMatrix matrix) {

        // compress this matrix
        IntWrapper inputoffset = new IntWrapper(0);
        IntWrapper outputoffset = new IntWrapper(0);


        int[] compressedColumnIndices = new int[matrix.getColumnIndices().length + 1024];
        int TotalSize = matrix.getColumnIndices().length;

        for (int k = 0; k < TotalSize / ChunkSize; ++k)
            regularcodec.compress(matrix.getColumnIndices(), inputoffset, ChunkSize, compressedColumnIndices, outputoffset);
        lastcodec.compress(matrix.getColumnIndices(), inputoffset, TotalSize % ChunkSize, compressedColumnIndices, outputoffset);

        // we can repack the data: (optional)
        compressedColumnIndices = Arrays.copyOf(compressedColumnIndices, outputoffset.intValue());


        int[] compressedRowPointer = new int[matrix.getRowPointers().length + 1024];
        inputoffset = new IntWrapper(0);
        outputoffset = new IntWrapper(0);
        TotalSize = matrix.getRowPointers().length;

        for (int k = 0; k < TotalSize / ChunkSize; ++k)
            regularcodec.compress(matrix.getRowPointers(), inputoffset, ChunkSize, compressedRowPointer, outputoffset);
        lastcodec.compress(matrix.getRowPointers(), inputoffset, TotalSize % ChunkSize, compressedRowPointer, outputoffset);

        // we can repack the data: (optional)
        compressedRowPointer = Arrays.copyOf(compressedRowPointer, outputoffset.intValue());


        data = matrix.getData();

        this.compressedRowPointer = compressedRowPointer;
        this.compressedColumnIndices = compressedColumnIndices;

        numColumns = matrix.numColumns();
        numRows = matrix.numRows();
    }

    public DenseVector multiplication(DenseVector x) throws IOException {

        double[] xd = ((DenseVector) x).getData();
        DenseVector y = new DenseVector(numRows);

        // we are iterating over compressed chunks

        BufferedArrayIterator rowPointer = new BufferedArrayIterator(compressedRowPointer, "compressedRowPointer");
        BufferedArrayIterator columnIndex = new BufferedArrayIterator(compressedColumnIndices, "compressedColumnIndices");

        // rowPointer will be build up in chunks of chunksize


        int to = rowPointer.get(0);

        for (int i = 0; i < numRows; ++i) {

            double dot = 0;

            int from = to;
            to = rowPointer.get(i + 1);

            for (int j = from; j < to; j++) {
                dot += data[j] * xd[columnIndex.get(j)];
            }
            if (dot != 0) {
                y.set(i, dot);
            }


        }


        return y;
    }

    private class BufferedArrayIteratorIntBuffer {

        // Current positions in compressed array

        final int[] compressedArray;
        final int compressedArrayLength;
        // how far have we uncompressed
        IntWrapper compoff = new IntWrapper(0);
        int[] buf = new int[ChunkSize];
        IntBuffer buffer = IntBuffer.wrap(buf);
        int currentpos = 0;
        IntWrapper recoffset = new IntWrapper(0);
        private String name;

        private BufferedArrayIteratorIntBuffer(int[] compressedArray, String name) {
            this.compressedArray = compressedArray;
            this.compressedArrayLength = compressedArray.length;
            this.name = name;

        }


        // unpack next
        private void update() {

            // read a chunk
            currentpos += recoffset.get();

            recoffset = new IntWrapper(0);
            regularcodec.uncompress(compressedArray, compoff, compressedArrayLength - compoff.get(), buf, recoffset);

            if (recoffset.get() < ChunkSize) {// last chunk detected
                ivb.uncompress(compressedArray, compoff, compressedArrayLength - compoff.get(), buf, recoffset);
            }

            // now we have an array of buffer ... from currentPos -> currentPos + recoffset.get()
            buffer.rewind();

        }

    }


    private class BufferedArrayIterator {

        // Current positions in compressed array

        final int[] compressedArray;
        final int compressedArrayLength;
        // how far have we uncompressed
        IntWrapper compoff = new IntWrapper(0);
        int[] buf = new int[ChunkSize];
        int currentpos = 0;
        IntWrapper recoffset = new IntWrapper(0);
        private String name;

        private BufferedArrayIterator(int[] compressedArray, String name) {
            this.compressedArray = compressedArray;
            this.compressedArrayLength = compressedArray.length;
            this.name = name;

        }

        public int get(int position) {
            // get the position X in the compressed array

            if (position >= currentpos + recoffset.get()) {

                upDate();
            }

            return buf[position - currentpos];
        }

        // unpack next
        private void upDate() {

            // read a chunk
            currentpos += recoffset.get();

            recoffset = new IntWrapper(0);
            regularcodec.uncompress(compressedArray, compoff, compressedArrayLength - compoff.get(), buf, recoffset);

            if (recoffset.get() < ChunkSize) {// last chunk detected
                ivb.uncompress(compressedArray, compoff, compressedArrayLength - compoff.get(), buf, recoffset);
            }

            // now we have an array of buffer ... from currentPos -> currentPos + recoffset.get()


        }

    }

}
