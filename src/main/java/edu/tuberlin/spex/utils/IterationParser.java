package edu.tuberlin.spex.utils;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * Taken from https://github.com/project-flink/flink-perf/blob/master/flink-jobs/src/main/java/com/github/projectflink/utils/IterationParser.java
 *
 * Little stupid util to get the iteration times
 */
public class IterationParser {

    enum States {
        START_ITERATION,
        NONE,
    }


    private static String fileName = "log/flink.log";

    public static void mainReadDelta(String[] args) throws Exception {
		try {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(fileName), Charsets.UTF_8));
			String line;
			int iteration = 1;

			Date iterationStart = null;
			Date iterationEnd = null;
			//	long iterationStart = 0;

			while ((line = br.readLine()) != null) {
				// System.err.println("line = "+line);
				// find first iteration start
				if (iterationStart == null && line.contains("starting iteration [" + iteration + "]")) {
				//	System.err.println("found start");
					iterationStart = getDate(line);
				}
				// find last iteration end
				if(line.contains("done in iteration ["+iteration+"]")) {
				//	System.err.println("found end");
					iterationEnd = getDate(line, iterationStart);
					long duration = iterationEnd.getTime() - iterationStart.getTime();
					System.err.println(iteration+","+ duration);
					iteration++;
					iterationStart = null;
				}
			}
			br.close();
		} catch (Throwable t) {
			System.err.println("ex : "+t.getMessage());
			t.printStackTrace();
		}
	}

    private static States state = States.NONE;


	// read bulk
	public static void main(String[] args) throws Exception {
		try {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(fileName), Charsets.UTF_8));

			int iteration = 1;

			Date iterationStart = null;
			Date iterationEnd = null;
			//	long iterationStart = 0;

            int global = 0;
            int maxIt = 0;

            Map<Integer,List<Long>> parsed = new HashMap<>();

            List<String> lines = Files.readLines(new File(fileName), Charsets.UTF_8);
            // now filter the lines
            Iterable<String> filter = Iterables.filter(lines, new Predicate<String>() {
                @Override
                public boolean apply(String input) {
                    return  StringUtils.contains(input, "Bulk") &&
                            (StringUtils.contains(input, "starting iteration [")
                            || StringUtils.contains(input, "finishing iteration [")
                            || StringUtils.contains(input, "switched to FINISHED"));
                }
            });

            List<String> arrayList = Lists.newArrayList(filter);


            for (int pos = 0; pos < arrayList.size(); pos++) {
                String line = arrayList.get(pos);

				// find first iteration start
				if (iterationStart == null && line.contains("starting iteration [" + iteration + "]") && state == States.NONE) {
					//	System.err.println("found start");
					iterationStart = getDate(line);
                    state = States.START_ITERATION;
				}
				if(line.contains("finishing iteration ["+(iteration)+"]") && state == States.START_ITERATION) {

                    // find last iteration end
                    while (pos+1 < arrayList.size() && arrayList.get(pos+1).contains("finishing iteration ["+(iteration)+"]"))  {
                        pos++;
                        line = arrayList.get(pos);
                    }

					//	System.err.println("found end");
					iterationEnd = getDate(line, iterationStart);
					long duration = iterationEnd.getTime() - iterationStart.getTime();
					//System.err.println(iteration+" , "+ duration);
					iteration++;
					iterationStart = null;
                    List<Long> integers = parsed.get(global);
                    if(integers == null) {
                        integers = new ArrayList<>();
                        parsed.put(global, integers);
                    }
                    integers.add(duration);
                    maxIt = Math.max(maxIt, iteration);
                    state = States.NONE;
                }
                if(line.contains("switched to FINISHED")) {
                    // reset
                    if(iteration > 1) global++;
                    iteration=1;
                    //System.err.println(Strings.repeat("--", 50));

                }
			}
            if(state == States.START_ITERATION) {
                // last teration is not finished yet
                global++;

            }
			br.close();

            maxIt--;
            for (int i = 0; i < maxIt; i++) {
                System.err.print(i+1 + " \t ");
                for (int pos = 0; pos < global; pos++) {
                    if(parsed.size() > pos && parsed.get(pos).size() > i) {
                        System.err.print(parsed.get(pos).get(i) + ",");
                    } else {
                        System.err.print(" ,");
                    }
                }
                System.err.println();
            }


		} catch (Throwable t) {
			System.err.println("ex : "+t.getMessage());
			t.printStackTrace();
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