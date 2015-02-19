package edu.tuberlin.spex.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.junit.Test;

import java.util.ArrayList;
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

        Pattern pattern = Pattern.compile("(\\d+),(\\d+)");

        StringBuilder stringBuilder = new StringBuilder();

        for (String line : lines) {
            Matcher matcher = pattern.matcher(line);

            Integer size = Ints.tryParse(StringUtils.substringBefore(line, "[").trim());
            List<Double> doubles = new ArrayList<>();
            List<Integer> blocks = new ArrayList<>();
            while (matcher.find()) {
                Integer blockId = Ints.tryParse(matcher.group(1));
                Double counts = Doubles.tryParse(matcher.group(2));
                doubles.add(counts);
                blocks.add(blockId);
            }
            double[] values = new double[doubles.size()];
            int pos = -1;
            for (Double aDouble : doubles) {
                values[++pos] = aDouble;
            }
            String format = String.format("%4s %4d (%5d) %3.2f\n", size, blocks.size(), size * size, blocks.size() / (double) (size * size));
            System.out.printf(format);
            stringBuilder.append(format);

            EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution();
            empiricalDistribution.load(values);

            System.out.println(empiricalDistribution.getSampleStats());

        }

        System.out.println(stringBuilder.toString());

    }
}
