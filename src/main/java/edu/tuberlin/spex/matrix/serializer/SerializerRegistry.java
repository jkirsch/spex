package edu.tuberlin.spex.matrix.serializer;

import edu.tuberlin.spex.algorithms.domain.VectorBlock;
import edu.tuberlin.spex.matrix.adapted.AdaptedCompRowMatrix;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Date: 12.02.2015
 * Time: 22:09
 */
public class SerializerRegistry {

    public static void register(ExecutionEnvironment env) {

        // Register Serializer

        /*env.addDefaultKryoSerializer(DenseMatrix.class, DenseMatrixSerializer.class);
        env.addDefaultKryoSerializer(LinkedSparseMatrix.class, LinkedSparseMatrixSerializer.class);

        env.addDefaultKryoSerializer(AdaptedCompColMatrix.class, AdaptedCompColMatrixSerializer.class);
        //env.addDefaultKryoSerializer(DenseVector.class, DenseVectorSerializer.class);
        //env.addDefaultKryoSerializer(SparseVector.class, SparseVectorSerializer.class);
        //env.addDefaultKryoSerializer(BitSet.class, BitSetSerializer.class);
        env.addDefaultKryoSerializer(MatrixBlock.class, MatrixBlockSerializer.class); */
        env.addDefaultKryoSerializer(AdaptedCompRowMatrix.class, AdaptedCompRowMatrixSerializer.class);
        env.addDefaultKryoSerializer(VectorBlock.class, VectorBlockSerializer.class);

    }
}
