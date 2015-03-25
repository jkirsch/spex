package edu.tuberlin.spex.evaluation.matrixtypes;

import com.google.common.primitives.Doubles;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * 24.03.2015.
 * <p/>
 * Compressed diagonal storage (CDS) matrix.
 *
 */
public class CDSSpecialMatrix extends BaseMatrix {

    private double[][] diag;
    private int[] ind;

    public static CDSSpecialMatrix loadData(double[][] matrix) {

        // here we need to load the data

        CDSSpecialMatrix m = new CDSSpecialMatrix();

        // assume rectangular
        m.numRows = matrix.length;
        m.numColumns = matrix.length;

        return m.loadDataInternal(matrix);

    }

    /**
     * Searches for a key in a sorted array, and returns an index to an element
     * which is greater than or equal key.
     *
     * @param index Sorted array of integers
     * @param key   Search for something equal or greater
     * @return index.length if nothing greater or equal was found, else an index
     * satisfying the search criteria
     */
    public static int binarySearchGreater(int[] index, int key) {
        return binarySearchInterval(index, key, 0, index.length, true);
    }

    private static int binarySearchInterval(int[] index, int key, int begin,
                                            int end, boolean greater) {

        // Zero length array?
        if (begin == end)
            if (greater)
                return end;
            else
                return begin - 1;

        end--; // Last index
        int mid = (end + begin) >> 1;

        // The usual binary search
        while (begin <= end) {
            mid = (end + begin) >> 1;

            if (index[mid] < key)
                begin = mid + 1;
            else if (index[mid] > key)
                end = mid - 1;
            else
                return mid;
        }

        // No direct match, but an inf/sup was found
        if ((greater && index[mid] >= key) || (!greater && index[mid] <= key))
            return mid;
            // No inf/sup, return at the end of the array
        else if (greater)
            return mid + 1; // One past end
        else
            return mid - 1; // One before start
    }

    private CDSSpecialMatrix loadDataInternal(double[][] matrix) {

        int numEntries = getNNZ(matrix);

        int[] row = new int[numEntries], column = new int[numEntries];
        double[] entry = new double[numEntries];

        // read the entries
        int counter = 0;
        for (int rowIndex = 0; rowIndex < matrix.length; rowIndex++) {
            for (int columnIndex = 0; columnIndex < matrix[rowIndex].length; columnIndex++) {
                double val = matrix[rowIndex][columnIndex];
                if (Doubles.compare(val, 0) != 0) {
                    // we need this entry
                    row[counter] = rowIndex;
                    column[counter] = columnIndex;
                    entry[counter] = columnIndex;

                    counter++;
                }
            }
        }


        // Find all the diagonals so that we can preallocate
        Set<Integer> diags = new TreeSet<Integer>();
        for (int i = 0; i < numEntries; ++i)
            diags.add(getDiagonal(row[i], column[i]));

        // Convert into an integer array
        int[] ind = new int[diags.size()];
        {
            Integer[] ints = new Integer[diags.size()];
            diags.toArray(ints);
            for (int i = 0; i < diags.size(); ++i)
                ind[i] = ints[i];
        }

        // Create the structure with preallocation
        construct(ind);

        // Insert the entries
        for (int i = 0; i < numEntries; ++i)
            set(row[i], column[i], entry[i]);

        return this;
    }

    public void set(int row, int column, double value) {
        check(row, column);

        int diagonal = getCompDiagIndex(row, column);

        diag[diagonal][getOnDiagIndex(row, column)] = value;
    }

    private int getCompDiagIndex(int row, int column) {
        int diagonal = getDiagonal(row, column);

        // Check if the diagonal is already present
        int index = binarySearchGreater(ind,
                diagonal);
        if (index < ind.length && ind[index] == diagonal)
            return index;

        // Need to allocate new diagonal. Get the diagonal size
        int size = getDiagSize(diagonal);

        // Allocate new primary structure
        double[] newDiag = new double[size];
        double[][] newDiagArray = new double[diag.length + 1][];
        int[] newInd = new int[ind.length + 1];

        // Move data from the old into the new structure
        System.arraycopy(ind, 0, newInd, 0, index);
        System.arraycopy(ind, index, newInd, index + 1, ind.length - index);
        for (int i = 0; i < index; ++i)
            newDiagArray[i] = diag[i];
        for (int i = index; i < diag.length; ++i)
            newDiagArray[i + 1] = diag[i];

        newInd[index] = diagonal;
        newDiagArray[index] = newDiag;

        // Update pointers
        ind = newInd;
        diag = newDiagArray;

        return index;
    }

    private int getOnDiagIndex(int row, int column) {
        return row > column ? column : row;
    }

    private void construct(int[] diagonal) {
        diag = new double[diagonal.length][];
        ind = new int[diagonal.length];

        // Keep the diagonal indices sorted
        int[] sortedDiagonal = new int[diagonal.length];
        System.arraycopy(diagonal, 0, sortedDiagonal, 0, diagonal.length);
        Arrays.sort(sortedDiagonal);

        for (int i = 0; i < diagonal.length; ++i) {
            ind[i] = sortedDiagonal[i];
            diag[i] = new double[getDiagSize(sortedDiagonal[i])];
        }
    }

    private int getDiagonal(int row, int column) {
        return column - row;
    }

    /**
     * Finds the size of the requested diagonal to be allocated
     */
    private int getDiagSize(int diagonal) {
        if (diagonal < 0)
            return Math.min(numRows + diagonal, numColumns);
        else
            return Math.min(numRows, numColumns - diagonal);
    }

    @Override
    public int getStorageSize() {
        int size = 0;
        for (double[] doubles : diag) {
            size += doubles.length;
        }

        return ind.length + size;
    }

    @Override
    public OperationsCounter getMultComplexity() {

/**
 for (int i = 0; i < ind.length; ++i) {
 int row = ind[i] < 0 ? -ind[i] : 0;
 int column = ind[i] > 0 ? ind[i] : 0;
 double[] locDiag = diag[i];
 for (int j = 0; j < locDiag.length; ++j, ++row, ++column)
 yd[row] += locDiag[j] * xd[column];
 }
 */
        int memoryAccess = 0;
        int multiplications = 0;

        for (int i = 0; i < ind.length; ++i) {

            // access the row index
            memoryAccess++;

            int row = ind[i] < 0 ? -ind[i] : 0;

            // access the colum
            memoryAccess++;

            int column = ind[i] > 0 ? ind[i] : 0;

            // get the locDiag

            memoryAccess++;
            double[] locDiag = diag[i];

            for (int j = 0; j < locDiag.length; ++j, ++row, ++column) {

                // access locDiag
                memoryAccess++;
                // access the xd[column]
                memoryAccess++;

                multiplications++;

                // write the result
                memoryAccess++;

                //yd[row] += locDiag[j] * xd[column];
            }
        }


        return new OperationsCounter(memoryAccess, multiplications);
    }
}
