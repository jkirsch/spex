package edu.tuberlin.spex.matrix.serializer;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.BitSet;

/**
 * Date: 12.02.2015
 * Time: 22:09
 */
public class SerializerRegistry {

    public static void register(ExecutionEnvironment env) {

        // Register Serializer

        //env.getConfig().registerTypeWithKryoSerializer(LinkedSparseMatrix.class, LinkedSparseMatrixSerializer.class);
        //env.addDefaultKryoSerializer(FlexCompRowMatrix.class, FlexCompRowMatrixSerializer.class);
        //env.addDefaultKryoSerializer(CompDiagMatrix.class, CompDiagMatrixSerializer.class);

        /*env.addDefaultKryoSerializer(DenseMatrix.class, DenseMatrixSerializer.class);
        env.addDefaultKryoSerializer(LinkedSparseMatrix.class, LinkedSparseMatrixSerializer.class);

        env.addDefaultKryoSerializer(AdaptedCompColMatrix.class, AdaptedCompColMatrixSerializer.class);
        //env.addDefaultKryoSerializer(DenseVector.class, DenseVectorSerializer.class);
        //env.addDefaultKryoSerializer(SparseVector.class, SparseVectorSerializer.class);
        //env.addDefaultKryoSerializer(BitSet.class, BitSetSerializer.class);
        env.addDefaultKryoSerializer(MatrixBlock.class, MatrixBlockSerializer.class); */
        //env.addDefaultKryoSerializer(AdaptedCompRowMatrix.class, AdaptedCompRowMatrixSerializer.class);
        //env.addDefaultKryoSerializer(VectorBlock.class, VectorBlockSerializer.class);

        env.addDefaultKryoSerializer(BitSet.class, ArrayBitSetSerializer.class);

    }
}
