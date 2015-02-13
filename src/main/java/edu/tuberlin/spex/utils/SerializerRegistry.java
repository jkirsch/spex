package edu.tuberlin.spex.utils;

import edu.tuberlin.spex.matrix.io.AdaptedCompRowMatrixSerializer;
import edu.tuberlin.spex.matrix.io.DenseMatrixSerializer;
import edu.tuberlin.spex.matrix.io.DenseVectorSerializer;
import edu.tuberlin.spex.matrix.io.LinkedSparseMatrixSerializer;
import edu.tuberlin.spex.matrix.io.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Date: 12.02.2015
 * Time: 22:09
 */
public class SerializerRegistry {

    public static void register(ExecutionEnvironment env) {

        // Register Serializer
        env.registerKryoSerializer(DenseMatrix.class, new DenseMatrixSerializer());
        env.registerKryoSerializer(LinkedSparseMatrix.class, new LinkedSparseMatrixSerializer());
        env.registerKryoSerializer(AdaptedCompRowMatrix.class, new AdaptedCompRowMatrixSerializer());
        env.registerKryoSerializer(DenseVector.class, new DenseVectorSerializer());

    }
}
