package edu.tuberlin.spex.utils;

import com.google.common.collect.Lists;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.io.MatrixInfo;
import no.uib.cipr.matrix.io.MatrixSize;
import no.uib.cipr.matrix.io.MatrixVectorWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Date: 18.01.2015
 * Time: 23:16
 *
 */
public class MatrixMarketWriter {

    public static void write(Matrix matrix, File file) throws IOException {

        // write out the file
        // skip back to the beginning and write the header
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        MatrixVectorWriter matrixVectorWriter = new MatrixVectorWriter(fileOutputStream);

        List<MatrixEntry> entries = Lists.newArrayList(matrix);

        MatrixInfo matrixInfo = new MatrixInfo(true, MatrixInfo.MatrixField.Real, MatrixInfo.MatrixSymmetry.General);
        MatrixSize matrixSize = new MatrixSize(matrix.numRows(), matrix.numColumns(), entries.size());
        matrixVectorWriter.printMatrixInfo(matrixInfo);
        matrixVectorWriter.printMatrixSize(matrixSize);
        matrixVectorWriter.printComments(new String[] {"Matrix generated automatically on " + new Date().toString()});

        for (MatrixEntry matrixEntry : matrix) {
            int[] row = {matrixEntry.row()};
            int[] col = {matrixEntry.column()};
            double[] data = {matrixEntry.get()};
            matrixVectorWriter.printCoordinate(row, col, data, 1);
        }

        matrixVectorWriter.flush();
        fileOutputStream.close();


    }
}
