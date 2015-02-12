package edu.tuberlin.spex.matrix.io;

import edu.tuberlin.spex.utils.VectorHelper;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class DenseVectorSerializerTest extends AbstractIOTest {

    @Test
    public void testSerialization() throws Exception {

        Vector ones = VectorHelper.ones(100);

        serialize(ones);
        DenseVector deserialize = deserialize(DenseVector.class);

        Assert.assertThat(
                ones.norm(Vector.Norm.One),
                is(deserialize.norm(Vector.Norm.One)));


    }

}