package edu.tuberlin.spex.algorithms.accumulator;

/**
 * Taken from Flink java examples
 */

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This accumulator lets you increase vector components distributedly. The {@link #add(Integer)} method lets you
 * increase the <i>n</i>-th vector component by 1, whereat <i>n</i> is the methods parameter. The size of the vector
 * is automatically managed.
 */
public class VectorAccumulator implements Accumulator<Integer, ArrayList<Integer>> {

    /**
     * Stores the accumulated vector components.
     */
    private final ArrayList<Integer> resultVector;

    public VectorAccumulator() {
        this(new ArrayList<Integer>());
    }

    public VectorAccumulator(ArrayList<Integer> resultVector) {
        this.resultVector = resultVector;
    }

    /**
     * Increases the result vector component at the specified position by 1.
     */
    @Override
    public void add(final Integer position) {
        updateResultVector(position, 1);
    }

    /**
     * Increases the result vector component at the specified position by the specified delta.
     */
    private void updateResultVector(final int position, final int delta) {
        // inflate the vector to contain the given position
        while (this.resultVector.size() <= position) {
            this.resultVector.add(0);
        }

        // increment the component value
        final int component = this.resultVector.get(position);
        this.resultVector.set(position, component + delta);
    }

    @Override
    public ArrayList<Integer> getLocalValue() {
        return this.resultVector;
    }

    @Override
    public void resetLocal() {
        // clear the result vector if the accumulator instance shall be reused
        this.resultVector.clear();
    }

    @Override
    public void merge(final Accumulator<Integer, ArrayList<Integer>> other) {
        // merge two vector accumulators by adding their up their vector components
        final List<Integer> otherVector = other.getLocalValue();
        for (int index = 0; index < otherVector.size(); index++) {
            updateResultVector(index, otherVector.get(index));
        }
    }

    @Override
    public void write(final DataOutputView out) throws IOException {
        // binary serialization of the result vector:
        // [number of components, component 0, component 1, ...]
        out.writeInt(this.resultVector.size());
        for (final Integer component : this.resultVector) {
            out.writeInt(component);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        // binary deserialization of the result vector
        final int size = in.readInt();
        for (int numReadComponents = 0; numReadComponents < size; numReadComponents++) {
            final int component = in.readInt();
            this.resultVector.add(component);
        }
    }

    @Override
    public Accumulator<Integer, ArrayList<Integer>> clone() {

        return new VectorAccumulator(new ArrayList<>(resultVector));
    }

}