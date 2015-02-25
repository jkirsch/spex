package edu.tuberlin.spex.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 24.02.2015
 * Time: 15:08
 */
public class TicToc {

    private static final Logger LOG = LoggerFactory.getLogger(TicToc.class);

    public static void tic(String marker, String message) {
        LOG.info("TIC - {} - {}", marker, message);
    }

    public static void toc(String marker, String message) {
        LOG.info("TOC - {} - {}", marker, message);
    }
}
