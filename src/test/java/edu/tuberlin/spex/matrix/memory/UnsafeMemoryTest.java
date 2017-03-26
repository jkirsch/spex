package edu.tuberlin.spex.matrix.memory;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

/**
 * Created by Johannes on 16.04.2016.
 */
public class UnsafeMemoryTest {

    @Test
    public void intArrayTest() throws Exception {

        int[] x = new int[] {1,2,3,4};

        UnsafeMemory unsafeMemory = new UnsafeMemory(new byte[x.length * 4 + 4]);

        unsafeMemory.putIntArray(x);

        unsafeMemory.reset();

        int[] intArray = unsafeMemory.getIntArray();

        Assert.assertThat(intArray, is(x));

    }
}