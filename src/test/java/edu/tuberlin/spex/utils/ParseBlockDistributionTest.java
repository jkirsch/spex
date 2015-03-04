package edu.tuberlin.spex.utils;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.common.io.Resources;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import no.uib.cipr.matrix.DenseMatrix;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 15.02.2015
 * Time: 12:47
 *
 */
public class ParseBlockDistributionTest {

    @Test
    public void testParse() throws Exception {

        List<String> lines = Resources.readLines(Resources.getResource("blockcounts.txt"), Charsets.UTF_8);


        final Pattern matches = Pattern.compile("(\\d+) \\[([\\d ,\\(\\)]+)\\]");
        Pattern pattern = Pattern.compile("(\\d+),(\\d+)");

        StringBuilder stringBuilder = new StringBuilder();

        Collections.sort(lines, Ordering.natural().nullsLast().onResultOf(new Function<String, Comparable>() {
            @Override
            public Comparable apply(String line) {
                Matcher yep = matches.matcher(line);
                if (yep.find()) {
                    return Ints.tryParse(yep.group(1));
                }
                return null;
            }
        }));

        for (String line : lines) {

            Integer size;
            Matcher yep = matches.matcher(line);
            if(yep.find()) {
                size = Ints.tryParse(yep.group(1));
            } else {
                continue;
            }

            List<Double> doubles = new ArrayList<>();
            List<Integer> blocks = new ArrayList<>();
            Matcher matcher = pattern.matcher(yep.group(2));

            // we now have the values . create heatmap
            DenseMatrix matrix = new DenseMatrix(size, size);

            while (matcher.find()) {
                Integer blockId = Ints.tryParse(matcher.group(1));
                Double counts = Doubles.tryParse(matcher.group(2));
                doubles.add(counts);
                blocks.add(blockId);

                // get the row and the column from the blockID

                matrix.set(size - blockId / size - 1, blockId % size, counts);

            }

            //System.out.println(matrix);

            double[] values = new double[doubles.size()];

            int pos = -1;
            for (Double aDouble : doubles) {
                values[++pos] = aDouble;
            }
            String format = String.format("%4s %5d (%6d) %3.2f\n", size, blocks.size(), size * size, blocks.size() / (double) (size * size));
            System.out.printf(format);
            stringBuilder.append(format);

            EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution();
            empiricalDistribution.load(values);

            System.out.println(empiricalDistribution.getSampleStats());


        }

        System.out.println(stringBuilder.toString());

    }



}
