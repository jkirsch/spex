package flinkbugs;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Testcase for FLINK-1531
 */
public class TestKryoIterationSerializer {

    @Test
    public void testFLINK1531() throws Exception {

        // this is an artificial program, it does not compute anything sensical
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //env.addDefaultKryoSerializer(DomainObject.class, new DomainObjectSerializer());

        DataSource<DomainObject> source = env.fromCollection(generateDomainObjects(10, 10));

        IterativeDataSet<DomainObject> iterate = source.iterate(1);

        DataSet<DomainObject> result = iterate.closeWith(iterate);

        List<DomainObject> resultCollector = new ArrayList<>();
        result.output(new LocalCollectionOutputFormat<>(resultCollector));

        env.execute();

    }

    // Dummy Object
    public static class DomainObject {
        int[] ints;


        public DomainObject(int[] ints) {
            this.ints = ints;
        }


        @Override
        public String toString() {
            return "DomainObject{" +
                    "ints=" + Arrays.toString(ints) +
                    '}';
        }
    }

    private List<DomainObject> generateDomainObjects(int n, int size) {
        Iterable<DomainObject> elements = Iterables.cycle(new DomainObject(new int[size]));
        return Lists.newArrayList(Iterables.limit(elements, n));
    }

    // The Key is to use a custom domain Object serializer to make it fail ...
    public static class DomainObjectSerializer extends Serializer<DomainObject> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, DomainObject object) {
            output.writeInt(object.ints.length);
            output.writeInts(object.ints);
        }

        @Override
        public DomainObject read(Kryo kryo, Input input, Class<DomainObject> type) {
            int size = input.readInt();
            return new DomainObject(input.readInts(size));
        }
    }
}
