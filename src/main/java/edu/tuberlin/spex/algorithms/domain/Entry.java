package edu.tuberlin.spex.algorithms.domain;

/**
 * Date: 21.01.2015
 * Time: 23:03
 *
 */
public class Entry {
    int index;
    double value;

    public Entry() {
    }

    public Entry(int index, double value) {
        this.index = index;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "index=" + index +
                ", value=" + value +
                '}';
    }
}
