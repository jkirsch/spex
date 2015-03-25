package edu.tuberlin.spex.evaluation;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import edu.tuberlin.spex.evaluation.matrixtypes.*;

import java.util.List;

/**
 * 24.03.2015.
 */
public class Evaluate {

    public static void main(String[] args) {

        // different kinds of matrices

        double[][] diagonalMatrix = {
                {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
        };

       double[][] smalldiagonalMatrix = {
                {1, 1, 1, 1},
                {0, 1, 1, 0},
                {0, 0, 1, 0},
                {0, 0, 1, 0}
        };

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("#" + Joiner.on(" ").join("Blocks", "Storage", "multComplexity", "memoryComplexity") + "\n");

        double[][] thedata = smalldiagonalMatrix;

        CSRSpecialMatrix.loadData(thedata).print();

        for (int i = 1; i <= thedata.length; i++) {

            if(thedata.length % i != 0) continue;

            List<double[][]> blocks = BlockSlicer.createBlocks(thedata, i);

            Multiset<String> storageCounter = HashMultiset.create();
            Multiset<String> multComplexity = HashMultiset.create();
            Multiset<String> memoryComplexity = HashMultiset.create();

            for (double[][] block : blocks) {
                CSCSpecialMatrix cscSpecialMatrix = CSCSpecialMatrix.loadData(block);
                CSRSpecialMatrix csrSpecialMatrix = CSRSpecialMatrix.loadData(block);
                CDSSpecialMatrix cdsSpecialMatrix = CDSSpecialMatrix.loadData(block);
                DenseSpecialMatrix denseSpecialMatrix = DenseSpecialMatrix.loadData(block);
                for (BaseMatrix matrix : Lists.newArrayList(cscSpecialMatrix, csrSpecialMatrix, cdsSpecialMatrix, denseSpecialMatrix)) {
                    storageCounter.add(matrix.getName(), matrix.getStorageSize());
                    multComplexity.add(matrix.getName(), matrix.getMultComplexity().getOperationsCounter());
                    memoryComplexity.add(matrix.getName(), matrix.getMultComplexity().getMemoryAccess());
                }
            }

            System.out.println("Blocks " + i);
            System.out.println(Joiner.on(" -> ").join("Storage         ", storageCounter));
            System.out.println(Joiner.on(" -> ").join("multComplexity  ", multComplexity));
            System.out.println(Joiner.on(" -> ").join("memoryComplexity", memoryComplexity));



            System.out.println("----");
        }


    }
}
