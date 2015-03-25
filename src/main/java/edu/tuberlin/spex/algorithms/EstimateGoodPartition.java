package edu.tuberlin.spex.algorithms;

import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.SortPartitionOperator;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 09.02.2015
 * Time: 23:42
 */
public class EstimateGoodPartition {

    private static final Logger LOG = LoggerFactory.getLogger(EstimateGoodPartition.class);


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setDegreeOfParallelism(1);

        String path = "datasets/smallTest.csv";

        // read the size information
        int n = MatrixReaderInputFormat.getSize(path);

        // now read the dataset
        EstimateGoodPartition estimateGoodPartition = new EstimateGoodPartition();

        estimateGoodPartition.read(env, path, n);

    }

    void read(ExecutionEnvironment env, String path, int n) throws Exception {
        DataSource<Tuple3<Integer, Integer, Double>> input = env.createInput(new MatrixReaderInputFormat(new Path(path), -1, n, true)).name("Edge list");

        // data row, column, value ( flipped )
        // rowsum and column sum
       SortPartitionOperator<Tuple2<Integer, Double>> colSumsDataSet = input.<Tuple2<Integer, Double>>project(1, 2).name("Select column id").groupBy(0).aggregate(Aggregations.SUM, 1).sortPartition(0, Order.ASCENDING).name("Calculate RowSums");
       SortPartitionOperator<Tuple2<Integer, Double>> rowSumsDataSet = input.<Tuple2<Integer, Double>>project(0, 2).name("Select row id").groupBy(0).aggregate(Aggregations.SUM, 1).sortPartition(0, Order.ASCENDING).name("Calculate RowSums");


  //      rowSumsDataSet.printToErr();

        // this is actually the row sum (transpose)
        colSumsDataSet.print();
        //rowSumsDataSet.print();

        env.execute();

    }
}
