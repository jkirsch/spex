package edu.tuberlin.spex.utils;

import com.google.common.collect.Iterators;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ParallelVectorIteratorTest {


    @Test
    public void testSplitRegular() {
        //testSplitting(new ParallelVectorIterator(8,2,1), 2);
        testSplitting(new ParallelVectorIterator(8, 2, 1), 2);
        testSplitting(new ParallelVectorIterator(8, 4, 1), 4);
        //testSplitting(new ParallelVectorIterator(8,2,1), 3);
    }

    @Test
    public void testSplitRegularElements() {
        testSplittingElements(new ParallelVectorIterator(8, 2, 1), 2);
        testSplittingElements(new ParallelVectorIterator(8, 4, 1), 4);
        testSplittingElements(new ParallelVectorIterator(8, 8, 1), 8);
        testSplittingElements(new ParallelVectorIterator(8, 1, 1), 1);
    }

    private static final void testSplittingElements(ParallelVectorIterator iter, int splits) {

        int size = Iterators.size(iter);
        assertThat(size, is(splits));


    }


    private static final void testSplitting(ParallelVectorIterator iter, int numSplits) {
        ParallelVectorIterator[] splits = iter.split(numSplits);

        assertEquals(numSplits, splits.length);

        // test start and end of range
        assertEquals(iter.getCurrent(), splits[0].getCurrent());
        assertEquals(iter.getTo(), splits[numSplits - 1].getTo());

        // test continuous range
        for (int i = 1; i < splits.length; i++) {
            assertEquals(splits[i - 1].getTo() + 1, splits[i].getCurrent());
        }

        testMaxSplitDiff(splits);
    }


    private static final void testMaxSplitDiff(ParallelVectorIterator[] iters) {
        long minSplitSize = Long.MAX_VALUE;
        long maxSplitSize = Long.MIN_VALUE;

        for (ParallelVectorIterator iter : iters) {
            long diff;
            if (iter.getTo() < iter.getCurrent()) {
                diff = 0;
            } else {
                diff = iter.getTo() - iter.getCurrent();
            }
            if (diff < 0) {
                diff = Long.MAX_VALUE;
            }

            minSplitSize = Math.min(minSplitSize, diff);
            maxSplitSize = Math.max(maxSplitSize, diff);
        }

        assertTrue(maxSplitSize == minSplitSize || maxSplitSize - 1 == minSplitSize);
    }

}