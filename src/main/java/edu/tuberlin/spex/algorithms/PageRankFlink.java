package edu.tuberlin.spex.algorithms;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.Cell;
import edu.tuberlin.spex.algorithms.domain.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Scanner;

/**
 * Date: 21.01.2015
 * Time: 21:51
 *
 */
public class PageRankFlink {

    public static void main(String[] args) throws Exception {

        final int numberOfRows = 8;
        final double c = 0.80;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setDegreeOfParallelism(1);


        FlatMapOperator<String, Cell> items = env.readTextFile("datasets/smallTest.csv").flatMap(new RichFlatMapFunction<String, Cell>() {
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
                    out.collect(new Cell(cell.getColumn(), cell.getRow(), cell.getValue() / (double) cells.size() ));
                }
            }
        });

        // check if we have dangling nodes .. they have no empty rows
        DataSource<Entry> vector = env.fromCollection(generateVector(numberOfRows, 1));

        CoGroupOperator<Cell, Entry, Cell> danglingNormalized = rowNormalized.coGroup(vector).where("column").equalTo("index").with(new CoGroupFunction<Cell, Entry, Cell>() {
            @Override
            public void coGroup(Iterable<Cell> cells, Iterable<Entry> entries, Collector<Cell> collector) throws Exception {

                boolean found = false;
                for (Cell cell : cells) {
                    collector.collect(cell);
                    found = true;
                }

                if(!found) {
                    Entry entry = Iterables.getOnlyElement(entries);
                    for (int i = 1; i <= numberOfRows; i++) {
                        collector.collect(new Cell(i, entry.getIndex(), 1 / (double) numberOfRows));
                    }
                }
            }
        });

        IterativeDataSet<Entry> ranks = env.fromCollection(generateVector(numberOfRows, 1 / (double) numberOfRows)).iterate(100);

        // first build the partial sums
        // multiply one entry in row with the row_index in the vector -> emit Entry(row, double)
        //    second:
        //      add all new Entries per row

        MapOperator<Entry, Entry> iteration = ranks.coGroup(danglingNormalized).where("index").equalTo("column").with(new RichCoGroupFunction<Entry, Cell, Entry>() {

            @Override
            public void coGroup(Iterable<Entry> iterable, Iterable<Cell> iterable1, Collector<Entry> collector) throws Exception {

                Entry entry = Iterables.getOnlyElement(iterable, null);

                if (entry != null) {
                    boolean found = false;
                    for (Cell cell : iterable1) {
                        collector.collect(new Entry(cell.getRow(), (c * cell.getValue() * entry.getValue())));
                        found = true;
                    }
                    if (!found) {
                        // empty column
                        //      fix cell value to 1 / numberOfRows
                        double value = c / numberOfRows * entry.getValue();
                        for (int i = 1; i <= numberOfRows; i++) {
                           // collector.collect(new Entry(i, value));
                            // Problematic LINE!!!!!!!
                        }
                    }
                }
            }
        }).groupBy("index").
                reduce(new ReduceFunction<Entry>() {
                    @Override
                    public Entry reduce(Entry entry, Entry t1) throws Exception {
                        return new Entry(entry.getIndex(), entry.getValue() + t1.getValue());
                    }
                }).map(new MapFunction<Entry, Entry>() {
            @Override
            public Entry map(Entry entry) throws Exception {
                entry.setValue(entry.getValue() + (1 - c) / (double) numberOfRows);
                return entry;
            }
        });


        DataSet<Entry> cellDataSet = ranks.closeWith(iteration);

        ReduceOperator<Entry> aggregate = cellDataSet.reduce(new ReduceFunction<Entry>() {
            @Override
            public Entry reduce(Entry entry, Entry t1) throws Exception {
                return new Entry(1, entry.getValue() + t1.getValue());
            }
        });

        aggregate.print();
        cellDataSet.print();

        System.out.println(env.getExecutionPlan());

        env.execute();

    }

    static List<Entry> generateVector(int size, double value) {

        List<Entry> vector = Lists.newArrayListWithCapacity(size);

        for (int i = 1; i <= size; i++) {
            vector.add(new Entry(i, value));
        }

        return vector;
    }


}
