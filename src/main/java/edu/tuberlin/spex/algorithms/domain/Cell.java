package edu.tuberlin.spex.algorithms.domain;

import java.io.Serializable;

public class Cell implements Serializable {
    int row;
    int column;
    double value;

    public Cell() {
    }

    public Cell(int row, int column, double value) {
        this.row = row;
        this.column = column;
        this.value = value;
    }

    public int getColumn() {
        return column;
    }

    public void setColumn(int column) {
        this.column = column;
    }

    public int getRow() {
        return row;
    }

    public void setRow(int row) {
        this.row = row;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Cell{" +
                "row=" + row +
                ", column=" + column +
                ", value=" + value +
                '}';
    }
}