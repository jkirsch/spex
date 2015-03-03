package edu.tuberlin.spex.matrix.adapted;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

/**
 * Date: 02.03.2015
 * Time: 21:39
 *
 */
public class EWAHCompressedBitmapHolder implements Value {

    public EWAHCompressedBitmap integers;

    public EWAHCompressedBitmapHolder() {
        integers = new EWAHCompressedBitmap();
    }

    public EWAHCompressedBitmapHolder(EWAHCompressedBitmap integers) {
        this.integers = integers;
    }

    public void setIntegers(EWAHCompressedBitmap integers) {
        this.integers = integers;
    }


    @Override
    public void write(DataOutputView dataOutputView) throws IOException {
        integers.serialize(dataOutputView);
    }

    @Override
    public void read(DataInputView dataInputView) throws IOException {
        integers.deserialize(dataInputView);
    }

    public void not() {
        integers.not();
    }

    public void trim() {
        integers.trim();
    }

    public boolean get(int i) {
        return integers.get(i);
    }

    public boolean set(int i) {
        return integers.set(i);
    }

    public EWAHCompressedBitmap getIntegers() {
        return integers;
    }
}
