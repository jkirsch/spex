package edu.tuberlin.spex.utils;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Taken and adapted from
 * https://github.com/project-flink/flink-perf/blob/master/flink-jobs/src/main/java/com/github/projectflink/utils/IterationParser.java
 *
 * Little stupid util to get the iteration times
 */
public class IterationParser {

    public static final Pattern COMPILE = Pattern.compile("(TIC|TOC) - (.*) - (.*)");
    private static final Pattern ticTocPattern = Pattern.compile("(TIC|TOC) - (.*) - (.*)");


    private static final Pattern startOfSomething = Pattern.compile("org\\.apache\\.flink\\.runtime\\.executiongraph\\.ExecutionGraph  - Deploying [^\\(]+ (.+) \\(attempt .*\\) to");
    private static final Pattern endOfSomething = Pattern.compile("org\\.apache\\.flink\\.runtime\\.taskmanager\\.Task  - (.+) switched to FINISHED");

    enum States {
        START_ITERATION,
        NONE,
    }


    private static String fileName = "log/flink.log";

    public static void mainReadDelta(String[] args) throws Exception {
        BufferedReader br = null;
        try {
            br = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(fileName), Charsets.UTF_8));
            String line;
            int iteration = 1;

            Date iterationStart = null;
            Date iterationEnd;
            //	long iterationStart = 0;

            while ((line = br.readLine()) != null) {
                // System.err.println("line = "+line);
                // find first iteration start
                if (iterationStart == null && line.contains("starting iteration [" + iteration + "]")) {
                    //	System.err.println("found start");
                    iterationStart = getDate(line);
                }
                // find last iteration end
                if (line.contains("done in iteration [" + iteration + "]")) {
                    //	System.err.println("found end");
                    iterationEnd = getDate(line, iterationStart);
                    long duration = iterationEnd.getTime() - iterationStart.getTime();
                    System.err.println(iteration + "," + duration);
                    iteration++;
                    iterationStart = null;
                }
            }
        } catch (Throwable t) {
            System.err.println("ex : " + t.getMessage());
            t.printStackTrace();
        } finally {
            IOUtils.closeQuietly(br);
        }
    }

    private static States state = States.NONE;


	// read bulk
	public static void main(String[] args) throws Exception {
        try {
            int iteration = 1;

            Date iterationStart = null;
            Date iterationEnd;
            //	long iterationStart = 0;

            int global = 0;
            int maxIt = 0;

            Map<Integer, List<Long>> parsed = new HashMap<>();

            // String -> marker -> start -> end
            Map<String, Tuple2<Long, Long>> ticToc = Maps.newHashMap();
            List<Map<String, Tuple2<Long, Long>>> annotations = new ArrayList<>();

            final List<String> lines = Files.readLines(new File(fileName), Charsets.UTF_8, new LineProcessor<List<String>>() {
                List<String> arrayList = Lists.newArrayList();

                @Override
                public boolean processLine(String input) throws IOException {
                    // now filter the lines
                    if (StringUtils.contains(input, "Bulk") &&
                            (StringUtils.contains(input, "starting iteration [")
                                    || StringUtils.contains(input, "finishing iteration [")
                                    || StringUtils.contains(input, "switched to FINISHED"))
                            || StringUtils.contains(input, "TIC")
                            || StringUtils.contains(input, "TOC")
                            || StringUtils.contains(input, "Deploying")) {

                        arrayList.add(input);

                    }
                    return true;
                }

                @Override
                public List<String> getResult() {
                    return arrayList;
                }
            });

            for (int pos = 0; pos < lines.size(); pos++) {
                String line = lines.get(pos);

                // find first iteration start
                if (iterationStart == null && line.contains("starting iteration [" + iteration + "]") && state == States.NONE) {
                    //	System.err.println("found start");
                    iterationStart = getDate(line);
                    state = States.START_ITERATION;
                }
                if (line.contains("finishing iteration [" + (iteration) + "]") && state == States.START_ITERATION) {

                    // find last iteration end
                    while (pos + 1 < lines.size() && lines.get(pos + 1).contains("finishing iteration [" + (iteration) + "]")) {
                        pos++;
                        line = lines.get(pos);
                    }

                    //	System.err.println("found end");
                    iterationEnd = getDate(line, iterationStart);
                    long duration = iterationEnd.getTime() - iterationStart.getTime();
                    //System.err.println(iteration+" , "+ duration);
                    iteration++;
                    iterationStart = null;
                    List<Long> integers = parsed.get(global);
                    if (integers == null) {
                        integers = new ArrayList<>();
                        parsed.put(global, integers);
                    }
                    integers.add(duration);
                    maxIt = Math.max(maxIt, iteration);
                    state = States.NONE;
                }
                if (line.contains("switched to FINISHED")) {
                    // reset
                    if (iteration > 1) global++;
                    iteration = 1;
                    //System.err.println(Strings.repeat("--", 50));


                    // flush TIC ToC
                    annotations.add(ticToc);
                    ticToc = new HashMap<>();
                }


                Matcher matcher = ticTocPattern.matcher(line);
                if (matcher.find()) {
                    // we have TIC or TOC
                    String identifier = matcher.group(2);
                    long timeStamp = getDate(line).getTime();
                    switch (matcher.group(1)) {
                        case "TIC":
                            addTic(ticToc, identifier, timeStamp);
                            break;
                        case "TOC":
                            addToc(ticToc, identifier, timeStamp);
                            break;
                        default:
                            throw new IllegalStateException("Don't know how to handle " + matcher.group(2));
                    }
                }

                Matcher startOf = startOfSomething.matcher(line);
               /* if(startOf.find()) {
                    // we have TIC or TOC
                    String identifier = startOf.group(1);
                    long timeStamp = getDate(line).getTime();
                    addTic(ticToc, identifier, timeStamp);
                }

                Matcher endOf = endOfSomething.matcher(line);

                if(endOf.find()) {
                    // we have TIC or TOC
                    String identifier = endOf.group(1);
                    long timeStamp = getDate(line).getTime();
                    addToc(ticToc, identifier, timeStamp);
                }*/

            }
            if (state == States.START_ITERATION) {
                // last iteration is not finished yet
                global++;
                // flush TIC ToC
                annotations.add(ticToc);
            }

            maxIt--;

            Set<String> names = Sets.newHashSet();

            for (Map<String, Tuple2<Long, Long>> annotation : annotations) {
                for (String s : annotation.keySet()) {
                    names.add(s);
                }
            }


            if(parsed.size() < global) global--;

            for (String name : names) {
                System.err.printf("%30s\t", name);
                for (int pos = 0; pos < global; pos++) {
                    if(annotations.size() > pos) {
                        if(annotations.get(pos).containsKey(name)) {
                            Tuple2<Long, Long> timing = annotations.get(pos).get(name);
                            if(timing.f0 == null || timing.f1 == null) {
                                System.err.print("\t");
                            } else {
                                System.err.printf("%6d",timing.f1 - timing.f0);
                            }
                        }
                    } else {
                        System.err.print("\t");
                    }
                }
                System.err.println();
            }

            for (int i = 0; i < maxIt; i++) {

                System.err.printf("%30d\t", i + 1);
                for (int pos = 0; pos < global; pos++) {
                    if (parsed.size() > pos && parsed.get(pos).size() > i) {
                        System.err.printf("%6d",parsed.get(pos).get(i));
                    }
                    // last ?
                    if(pos < global-1) {
                        System.err.print("\t");
                    }

                }
                System.err.println();
            }


        } catch (Throwable t) {
            System.err.println("ex : " + t.getMessage());
            t.printStackTrace();
        }


    }

    private static void addTic(Map<String, Tuple2<Long, Long>> ticToc, String identifier, long timeStamp) {
        // from
        if (ticToc.containsKey(identifier)) {
            // check if earlier
            Tuple2<Long, Long> pair = ticToc.get(identifier);
            pair.f0 = Math.min(pair.f0, timeStamp);
        } else {
            ticToc.put(identifier, new Tuple2<Long, Long>(timeStamp, 0L));
        }
    }

    private static void addToc(Map<String, Tuple2<Long, Long>> ticToc, String identifier, long timeStamp) {

        // to
        if (ticToc.containsKey(identifier)) {
            // check if earlier
            Tuple2<Long, Long> pair = ticToc.get(identifier);
            pair.f1 = Math.max(pair.f1, timeStamp);
        } else {
            System.out.println("End before begin .... for " + identifier);
        }


    }

    private static Date getDate(String line) throws ParseException {
        String[] sp = line.split(" ");
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        return sdf.parse(sp[0]);
    }

	private static Date getDate(String line, Date iterationStart) throws ParseException {
        Date parse = getDate(line);
        // do we have a roundtrip ?
        if(parse.getTime() < iterationStart.getTime()) {
            // add a day to parse
            Calendar c = Calendar.getInstance();
            c.setTime(parse);
            c.add(Calendar.DATE, 1);
            parse = c.getTime();
        }
        return parse;
	}
}