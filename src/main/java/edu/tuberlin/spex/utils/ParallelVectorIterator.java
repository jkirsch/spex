package edu.tuberlin.spex.utils;

import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import org.apache.flink.util.SplittableIterator;

import java.util.NoSuchElementException;

/**
 * Date: 22.02.2015
 * Time: 17:46
 */
public class ParallelVectorIterator extends SplittableIterator<VectorBlock> {

    private final long to;
    private final double initValue;
    private final int blockSize;
    private final int blocks;

    private long current;


    public ParallelVectorIterator(int n, int blocks, double initValue) {
        this(n / blocks, blocks, initValue, 0, blocks - 1);
    }

    private ParallelVectorIterator(int blockSize, int blocks, double initValue, long from, long to) {
        this.current = from;
        this.to = to;
        this.initValue = initValue;
        this.blockSize = blockSize;
        this.blocks = blocks;
    }

    @Override
    public boolean hasNext() {
        return current <= to;
    }


    @Override
    public VectorBlock next() {
        if (current <= to) {
            VectorBlock vectorBlock = new VectorBlock(Utils.safeLongToInt(current * blockSize), VectorHelper.identical(blockSize, initValue));
            current++;
            return vectorBlock;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParallelVectorIterator[] split(int numPartitions) {
        if (numPartitions < 1) {
            throw new IllegalArgumentException("The number of partitions must be at least 1.");
        }

        if (numPartitions == 1) {
            return new ParallelVectorIterator[] { new ParallelVectorIterator(blockSize, blocks, initValue, current, to) };
        }

        // here, numPartitions >= 2 !!!

        long elementsPerSplit;

        if (to - current + 1 >= 0) {
            elementsPerSplit = (to - current + 1) / numPartitions;
        }
        else {
            // long overflow of the range.
            // we compute based on half the distance, to prevent the overflow.
            // in most cases it holds that: current < 0 and to > 0, except for: to == 0 and current == Long.MIN_VALUE
            // the later needs a special case
            final long halfDiff; // must be positive

            if (current == Long.MIN_VALUE) {
                // this means to >= 0
                halfDiff = (Long.MAX_VALUE/2+1) + to/2;
            } else {
                long posFrom = -current;
                if (posFrom > to) {
                    halfDiff = to + ((posFrom - to) / 2);
                } else {
                    halfDiff = posFrom + ((to - posFrom) / 2);
                }
            }
            elementsPerSplit = halfDiff / numPartitions * 2;
        }

        if (elementsPerSplit < Long.MAX_VALUE) {
            // figure out how many get one in addition
            long numWithExtra = -(elementsPerSplit * numPartitions) + to - current + 1;

            // based on rounding errors, we may have lost one)
            if (numWithExtra > numPartitions) {
                elementsPerSplit++;
                numWithExtra -= numPartitions;

                if (numWithExtra > numPartitions) {
                    throw new RuntimeException("Bug in splitting logic. To much rounding loss.");
                }
            }

            ParallelVectorIterator[] iters = new ParallelVectorIterator[numPartitions];
            long curr = current;
            int i = 0;
            for (; i < numWithExtra; i++) {
                long next = curr + elementsPerSplit + 1;
                iters[i] = new ParallelVectorIterator(blockSize, blocks, initValue, curr, next-1);
                curr = next;
            }
            for (; i < numPartitions; i++) {
                long next = curr + elementsPerSplit;
                iters[i] = new ParallelVectorIterator(blockSize, blocks, initValue, curr, next-1);
                curr = next;
            }

            return iters;
        }
        else {
            // this can only be the case when there are two partitions
            if (numPartitions != 2) {
                throw new RuntimeException("Bug in splitting logic.");
            }

            return new ParallelVectorIterator[] {
                    new ParallelVectorIterator(blockSize, blocks, initValue, current, current + elementsPerSplit),
                    new ParallelVectorIterator(blockSize, blocks, initValue, current + elementsPerSplit, to)
            };
        }
    }


    @Override
    public int getMaximumNumberOfSplits() {
        if (to >= Integer.MAX_VALUE || current <= Integer.MIN_VALUE || to-current+1 >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        else {
            return (int) (to-current+1);
        }
    }

    public long getCurrent() {
        return this.current;
    }

    public long getTo() {
        return this.to;
    }

}
