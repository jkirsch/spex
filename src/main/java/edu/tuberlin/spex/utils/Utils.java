package edu.tuberlin.spex.utils;

/**
 * Date: 20.02.2015
 * Time: 22:31
 *
 */
public class Utils {

    public static int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                    (l + " cannot be cast to int without changing its value.");
        }
        return (int) l;
    }

}
