package edu.tuberlin.spex.algorithms;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.Cell;
import edu.tuberlin.spex.algorithms.domain.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Scanner;

/**
 * Date: 22.01.2015
 * Time: 21:12
 */
public class Normalizer {

    public static void main(String[] args) throws Exception {

        final int numberOfRows = 325729;
        final double c = 0.85;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FlatMapOperator<String, Cell> items = env.readTextFile("datasets/webNotreDame.mtx").flatMap(new RichFlatMapFunction<String, Cell>() {
            @Override
            public void flatMap(String value, Collector<Cell> out) throws Exception {

                if (!StringUtils.isEmpty(value) && !StringUtils.startsWithAny(value, "//", "%")) {
                    Scanner scanner = new Scanner(value);
                    int source = scanner.nextInt();
                    int dest = scanner.nextInt();
                    out.collect(new Cell(source, dest, 1));
                }
            }
        });
        // row normalize // and build transpose
        GroupReduceOperator<Cell, Cell> rowNormalized = items.groupBy("row").reduceGroup(new GroupReduceFunction<Cell, Cell>() {
            @Override
            public void reduce(Iterable<Cell> values, Collector<Cell> out) throws Exception {
                List<Cell> cells = Lists.newArrayList();
                for (Cell value : values) {
                    cells.add(new Cell(value.getRow(), value.getColumn(), value.getValue()));
                }
                for (Cell cell : cells) {
                    // build transpose
                    //cell.setValue(cell.getValue() / (double) cells.size());
                    out.collect(new Cell(cell.getColumn(), cell.getRow(), cell.getValue() / (double) cells.size()));
                }
            }
        });

        // check if we have dangling nodes .. they have no empty rows
        DataSource<Entry> vector = env.fromCollection(PageRankFlink.generateVector(numberOfRows, 1));

        CoGroupOperator<Cell, Entry, Cell> danglingNormalized = rowNormalized.coGroup(vector).where("column").equalTo("index").with(new CoGroupFunction<Cell, Entry, Cell>() {
            @Override
            public void coGroup(Iterable<Cell> cells, Iterable<Entry> entries, Collector<Cell> collector) throws Exception {

                boolean found = false;
                for (Cell cell : cells) {
                    collector.collect(cell);
                    found = true;
                }

                if (!found) {
                    Entry entry = Iterables.getOnlyElement(entries);
                    for (int i = 1; i <= numberOfRows; i++) {
                        collector.collect(new Cell(i, entry.getIndex(), 1 / (double) numberOfRows));
                    }
                }
            }
        });

        danglingNormalized.writeAsFormattedText("datasets/normalized-data", new TextOutputFormat.TextFormatter<Cell>() {
            @Override
            public String format(Cell value) {
                return Joiner.on(" ").join(value.getRow(), value.getColumn(), value.getValue());
            }
        });

        env.execute();

    }

}
