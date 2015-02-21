package edu.tuberlin.spex.matrix.serializer;

import edu.tuberlin.spex.matrix.adapted.AdaptedCompColMatrix;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Date: 12.02.2015
 * Time: 22:09
 */
public class SerializerRegistry {

    public static void register(ExecutionEnvironment env) {

        // Register Serializer
        env.addDefaultKryoSerializer(DenseMatrix.class, DenseMatrixSerializer.class);
        env.addDefaultKryoSerializer(LinkedSparseMatrix.class, LinkedSparseMatrixSerializer.class);
        env.addDefaultKryoSerializer(AdaptedCompRowMatrix.class, AdaptedCompRowMatrixSerializer.class);
        env.addDefaultKryoSerializer(AdaptedCompColMatrix.class, AdaptedCompColMatrixSerializer.class);
        env.addDefaultKryoSerializer(DenseVector.class, DenseVectorSerializer.class);
        env.addDefaultKryoSerializer(SparseVector.class, SparseVectorSerializer.class);

    }
}