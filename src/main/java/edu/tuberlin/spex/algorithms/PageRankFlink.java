package edu.tuberlin.spex.algorithms;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.Cell;
import edu.tuberlin.spex.algorithms.domain.Entry;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
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

        final int numberOfRows = 325729;
        final double c = 0.85;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FlatMapOperator<String, Cell> items = env.readTextFile("/work/code/spex/datasets/smallTest.csv").flatMap(new RichFlatMapFunction<String, Cell>() {
            @Override
            public void flatMap(String value, Collector<Cell> out) throws Exception {

                if (!value.startsWith("//")) {
                    Scanner scanner = new Scanner(value);
                    int source = scanner.nextInt();
                    int dest = scanner.nextInt();
                    out.collect(new Cell(source, dest, 1));
                }
            }
        });
        // row normalize
        GroupReduceOperator<Cell, Cell> rowNormalized = items.groupBy("row").reduceGroup(new GroupReduceFunction<Cell, Cell>() {
            @Override
            public void reduce(Iterable<Cell> values, Collector<Cell> out) throws Exception {
                List<Cell> cells = Lists.newArrayList();
                for (Cell value : values) {
                    cells.add(new Cell(value.getRow(), value.getColumn(), value.getValue()));
                }
                for (Cell cell : cells) {
                    cell.setValue(cell.getValue() / (double) cells.size());
                    out.collect(cell);
                }
            }
        });

        IterativeDataSet<Entry> ranks = env.fromCollection(generateVector(numberOfRows, 1 / (double) numberOfRows)).iterate(1);



        CoGroupOperator<Entry, Cell, Entry> iteration = ranks.coGroup(rowNormalized).where("index").equalTo("row").with(new RichCoGroupFunction<Entry, Cell, Entry>() {

            @Override
            public void coGroup(Iterable<Entry> iterable, Iterable<Cell> iterable1, Collector<Entry> collector) throws Exception {
                Entry entry = Iterables.getFirst(iterable, null);
                if (entry != null) {
                    double sumRank = 0;
                    for (Cell cell : iterable1) {
                        sumRank += (c * cell.getValue() + (1 - c) * 1 / (double) numberOfRows);
                    }
                    collector.collect(new Entry(entry.getIndex(), sumRank * entry.getValue()));
                }
            }
        });


        DataSet<Entry> cellDataSet = ranks.closeWith(iteration);

        cellDataSet.print();

        env.execute();

    }

    private static List<Entry> generateVector(int size, double value) {

        List<Entry> vector = Lists.newArrayListWithCapacity(size);

        for (int i = 1; i <= size; i++) {
            vector.add(new Entry(i, value));
        }

        return vector;
    }


}
