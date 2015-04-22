package edu.tuberlin.spex.matrix.io;

import com.google.common.io.Resources;
import edu.tuberlin.spex.utils.io.MatrixReaderInputFormat;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.io.MatrixVectorReader;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.io.FileReader;

public class MatrixMarketReaderTest {

    @Test
    public void testRead() throws Exception {

        String path = Resources.getResource("datasets/smallTest.mtx").getPath();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MatrixMarketReader matrixMarketReader = new MatrixMarketReader(env);

        MatrixReaderInputFormat.MatrixInformation matrixInfo = MatrixReaderInputFormat.getMatrixInfo(path);

        MapOperator<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>> build =
                matrixMarketReader.fromPath(path).withOffsetAdjust(-1).withMatrixInformation(matrixInfo).build();


        build.print();

        env.execute();

        DenseMatrix matrix = new DenseMatrix(new MatrixVectorReader(new FileReader(path)));

        DenseVector vector = new DenseVector(matrix.numColumns());

        System.out.println(matrix.mult(vector, vector.copy()));


    }
}