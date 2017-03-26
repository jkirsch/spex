package edu.tuberlin.spex.algorithms;

import com.google.common.collect.Lists;
import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.serializer.SerializerRegistry;
import edu.tuberlin.spex.utils.VectorBlockHelper;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.closeTo;

public class FlinkMatrixReaderTest {

    // Implementing Fisher–Yates shuffle
    static void shuffleArray(int[][] ar)
    {
        Random rnd = new Random();
        for (int i = ar.length - 1; i > 0; i--)
        {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            int[] a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    @Test
    public void testKnown() throws Exception {

        createKnownMatrix(8, true);

    }

    @Test
    public void testCreate() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 4096);

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);

        final double alpha = 0.80;
        final int n = 8;
        final int blocks = 2;

        boolean tranpose = true;

        DataSource<Tuple3<Integer, Integer, Double>> input = env.fromCollection(createKnownMatrix(n, tranpose));

        DataSource<Tuple2<Integer, VectorBlock>> denseVectorDataSource = env.fromCollection(VectorBlockHelper.createTupleBlocks(n, blocks, 1 / (double) n));

        //final IterativeDataSet<VectorBlock> iterate = denseVectorDataSource.iterate(iteration);
        final DeltaIteration<Tuple2<Integer, VectorBlock>, Tuple2<Integer, VectorBlock>> iterate = denseVectorDataSource.iterateDelta(denseVectorDataSource, 10, 0);

        GroupReduceOperator<Tuple2<Integer, VectorBlock>, Tuple2<Integer, VectorBlock>> delta = iterate.getWorkset().first(1);

        DataSet<Tuple2<Integer, VectorBlock>> tuple2DataSet = iterate.closeWith(delta, delta);

        tuple2DataSet.printOnTaskManager("Local");

        env.execute();


    }

    private List<Tuple3<Integer, Integer, Double>> createKnownMatrix(int n, boolean transpose) {

        // recreate the matrix from
        // http://de.wikipedia.org/wiki/Google-Matrix#Beispiel

        List<Tuple3<Integer, Integer, Double>> list = Lists.newArrayList();

        int[][] pos = {{1, 3}, {2, 1}, {2, 6}, {3, 4}, {3, 5}, {4, 2}, {4, 7}, {7, 8}, {8, 7}};

        // shuffle to ensure that sorting works
        shuffleArray(pos);

        // Matrix is just for visual inspection
        DenseMatrix m = new DenseMatrix(n, n);

        for (int[] positions : pos) {
            // subtract -1 .. as the index is 0 based
            int row = positions[0] - 1;
            int col = positions[1] - 1;

            m.set(row, col, 1d);
            if(!transpose) {
                list.add(new Tuple3<>(row, col, 1d));
            } else {
                list.add(new Tuple3<>(col, row, 1d));
            }
        }

        System.out.println(m);

        return list;

    }

    @Test
    public void testExecute() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 4096);

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);

        SerializerRegistry.register(env);

        final double alpha = 0.80;
        final int n = 8;

        boolean tranpose = true;

        DataSource<Tuple3<Integer, Integer, Double>> input = env.fromCollection(createKnownMatrix(n, tranpose));

        FlinkMatrixReader flinkMatrixReader = new FlinkMatrixReader();

        for (int b = 1; b < n; b++) {
            final int blocks = b;
            FlinkMatrixReader.TimingResult timingResult = flinkMatrixReader.executePageRank(env, alpha, blocks, input, n, 100);
            DenseVector p_k1 = new DenseVector(n);

            for (VectorBlock vectorBlock : timingResult.vectorBlocks) {
                for (VectorEntry entry : vectorBlock) {
                    if(p_k1.size() > entry.index() + vectorBlock.getStartRow()) {
                        p_k1.set(entry.index() + vectorBlock.getStartRow(), entry.get());
                    }
                }
            }

            Assert.assertThat(" Setting : b = " + b, p_k1.norm(Vector.Norm.One), closeTo(1, 0.00001));
            Assert.assertThat(p_k1.get(0), closeTo(0.0675, 0.0001));
            Assert.assertThat(p_k1.get(1), closeTo(0.0701, 0.0001));
            Assert.assertThat(p_k1.get(2), closeTo(0.0934, 0.0001));
            Assert.assertThat(p_k1.get(3), closeTo(0.0768, 0.0001));
            Assert.assertThat(p_k1.get(4), closeTo(0.0768, 0.0001));
            Assert.assertThat(p_k1.get(5), closeTo(0.0675, 0.0001));
            Assert.assertThat(p_k1.get(6), closeTo(0.2825, 0.0001));
            Assert.assertThat(p_k1.get(7), closeTo(0.2654, 0.0001));


        }

    }

}