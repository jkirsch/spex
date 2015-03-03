package edu.tuberlin.spex.matrix.adapted;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Date: 03.03.2015
 * Time: 00:49
 *
 */
public class DanglingNodeInformationBitSet implements Serializable {

    public int startRow;
    BitSet bitSet;

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public BitSet getBitSet() {
        return bitSet;
    }

    public void setBitSet(BitSet bitSet) {
        this.bitSet = bitSet;
    }
}
