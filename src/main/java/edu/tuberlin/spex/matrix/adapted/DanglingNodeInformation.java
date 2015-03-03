package edu.tuberlin.spex.matrix.adapted;

import java.io.Serializable;

/**
 * Date: 03.03.2015
 * Time: 00:49
 *
 */
public class DanglingNodeInformation implements Serializable {

    public int startRow;
    EWAHCompressedBitmapHolder ewahCompressedBitmapHolder;

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public EWAHCompressedBitmapHolder getEwahCompressedBitmapHolder() {
        return ewahCompressedBitmapHolder;
    }

    public void setEwahCompressedBitmapHolder(EWAHCompressedBitmapHolder ewahCompressedBitmapHolder) {
        this.ewahCompressedBitmapHolder = ewahCompressedBitmapHolder;
    }

    public boolean set(int i) {
        return ewahCompressedBitmapHolder.set(i - startRow);
    }
}
