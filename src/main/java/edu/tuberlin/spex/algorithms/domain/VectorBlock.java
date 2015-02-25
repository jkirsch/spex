package edu.tuberlin.spex.algorithms.domain;

import no.uib.cipr.matrix.AbstractVector;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This is a vector slice of vector elements
 *
 * Date: 18.02.2015
 * Time: 22:46
 *
 */
public class VectorBlock extends AbstractVector implements Serializable {

    /** Identifier to join with the corresponding matrix block. */
    public int startRow;

    /**
     * Vector data
     */
    private double[] data;

    public VectorBlock() {
        super(0);
        data = new double[size];
    }


    public VectorBlock(int size) {
        super(size);
        data = new double[size];
    }

    public VectorBlock(int startRow, DenseVector vector) {
        // store size
        super(vector);
        this.startRow = startRow;
        this.data = vector.getData();
    }

    public VectorBlock(int startRow, VectorBlock vector) {
        // store size
        super(vector);
        this.startRow = startRow;
        this.data = vector.getData();
    }


    @Override
    public double get(int index) {
        check(index);
        return data[index];
    }

    @Override
    public void set(int index, double value) {
        check(index);
        data[index] = value;
    }

    @Override
    public VectorBlock copy() {

        VectorBlock block = new VectorBlock(this.size());
        block.setStartRow(startRow);
        block.setData(getData().clone());

        return block;
    }

    @Override
    public VectorBlock zero() {
        Arrays.fill(data, 0);
        return this;
    }

    @Override
    public Vector add(Vector y) {
        if (!(y instanceof VectorBlock))
            return super.add(y);

        checkSize(y);

        double[] yd = ((VectorBlock) y).getData();

        for (int i = 0; i < size; i++)
            data[i] += yd[i];

        return this;
    }

    @Override
    public VectorBlock scale(double alpha) {
        for (int i = 0; i < size; ++i)
            data[i] *= alpha;
        return this;
    }

    @Override
    public double dot(Vector y) {
        if (!(y instanceof VectorBlock))
            return super.dot(y);

        checkSize(y);

        double[] yd = ((VectorBlock) y).getData();

        double dot = 0.;
        for (int i = 0; i < size; ++i)
            dot += data[i] * yd[i];
        return dot;
    }

    @Override
    protected double norm1() {
        double sum = 0;
        for (int i = 0; i < size; ++i)
            sum += Math.abs(data[i]);
        return sum;
    }

    @Override
    protected double norm2() {
        double norm = 0;
        for (int i = 0; i < size; ++i)
            norm += data[i] * data[i];
        return Math.sqrt(norm);
    }

    @Override
    protected double norm2_robust() {
        double scale = 0, ssq = 1;
        for (int i = 0; i < size; ++i)
            if (data[i] != 0) {
                double absxi = Math.abs(data[i]);
                if (scale < absxi) {
                    ssq = 1 + ssq * (scale / absxi) * (scale / absxi);
                    scale = absxi;
                } else
                    ssq += (absxi / scale) * (absxi / scale);
            }
        return scale * Math.sqrt(ssq);
    }

    @Override
    protected double normInf() {
        double max = 0;
        for (int i = 0; i < size; ++i)
            max = Math.max(Math.abs(data[i]), max);
        return max;
    }


    // GETTER AND SETTERS JUST FOR SERIALIZATION ... DON'T USE //

    public int getStartRow() {
        return startRow;
    }


    /**
     * Returns the internal vector contents. The array indices correspond to the
     * vector indices
     */
    public double[] getData() {
        return data;
    }

    public void setData(double[] data) {
        this.data = data;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public void setSize(int size) {
        this.size = size;
    }

/*
    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(startRow);
        out.write(size);

        ByteBuffer allocate = ByteBuffer.allocate(size * 8);
        allocate.asDoubleBuffer().put(data);
        out.write(allocate.array());

    }

    @Override
    public void read(DataInputView in) throws IOException {
        startRow = in.readInt();
        size = in.readInt();
        data = new double[size];

        ByteBuffer allocate = ByteBuffer.allocate(size * 8);
        in.read(allocate.array());

        DoubleBuffer.wrap(data).put(allocate.asDoubleBuffer());

    }*/

/*    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(startRow);
        output.writeInt(data.length);
        output.writeDoubles(data);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        startRow = input.readInt();
        int size = input.readInt();
        data = input.readDoubles(size);
    } */


}
