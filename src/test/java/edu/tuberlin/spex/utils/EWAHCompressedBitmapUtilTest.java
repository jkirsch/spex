package edu.tuberlin.spex.utils;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EWAHCompressedBitmapUtilTest {

    @Test
    public void testBuildWindow() throws Exception {

        EWAHCompressedBitmap ewahCompressedBitmap;

        ewahCompressedBitmap = EWAHCompressedBitmapUtil.buildWindow(249410, 5090, 325729);

        assertTrue(ewahCompressedBitmap.get(249410));
        assertTrue(ewahCompressedBitmap.get(249411));
        assertThat(ewahCompressedBitmap.cardinality(), is(5090));

        ewahCompressedBitmap = EWAHCompressedBitmapUtil.buildWindow(63, 1, 128);

        assertTrue(ewahCompressedBitmap.get(63));
        assertThat(ewahCompressedBitmap.cardinality(), is(1));

        ewahCompressedBitmap = EWAHCompressedBitmapUtil.buildWindow(0, 1, 128);

        assertTrue(ewahCompressedBitmap.get(0));
        assertThat(ewahCompressedBitmap.cardinality(), is(1));


        ewahCompressedBitmap = EWAHCompressedBitmapUtil.buildWindow(63, 2, 128);

        assertTrue(ewahCompressedBitmap.get(63));
        assertTrue(ewahCompressedBitmap.get(64));
        assertThat(ewahCompressedBitmap.cardinality(), is(2));

        ewahCompressedBitmap = EWAHCompressedBitmapUtil.buildWindow(0, 128, 128);

        assertTrue(ewahCompressedBitmap.get(0));
        assertTrue(ewahCompressedBitmap.get(1));
        assertThat(ewahCompressedBitmap.cardinality(), is(128));


    }
}