package edu.tuberlin.spex.utils;

import com.google.common.primitives.Ints;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ArgPaserTest {

    @Test
    public void testParse() throws Exception {

        String arg = "1 text 12 1 2 3 4 5 6 7 8";
        String[] args = arg.split("\\W");

        Integer n = Ints.tryParse(args[1]);
        Integer degree = Ints.tryParse(args[2]);
        String[] indices = ArrayUtils.subarray(args, 3, args.length);
        int[] blockSizes = new int[indices.length];

        for (int i = 0; i < indices.length; i++) {
            String index = indices[i];
            blockSizes[i] = Ints.tryParse(index);
        }

        Assert.assertArrayEquals(blockSizes, new int[]{1, 2, 3, 4, 5, 6, 7, 8});

    }
}
