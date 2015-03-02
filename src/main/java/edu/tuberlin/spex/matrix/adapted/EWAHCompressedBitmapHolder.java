package edu.tuberlin.spex.matrix.adapted;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.Iterator;

/**
 * Date: 02.03.2015
 * Time: 21:39
 *
 */
public class EWAHCompressedBitmapHolder implements Value {

    EWAHCompressedBitmap integers = new EWAHCompressedBitmap();

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

    public Iterator<Integer> iterator() {
        return integers.iterator();
    }

    public EWAHCompressedBitmap getIntegers() {
        return integers;
    }
}
