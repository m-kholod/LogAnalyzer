import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupReducerTest {
    @Test
    public void ipAddressCount() throws IOException {

        List<Text> reducerInput = new ArrayList<>();
        reducerInput.add(new Text("83.149.9.216"));
        reducerInput.add(new Text("24.236.252.67"));
        IntWritable count = new IntWritable(49);

        new ReduceDriver<IntWritable, Text, IntWritable, Text>()
                .withReducer(new GroupReducer())
                .withInput(count, reducerInput)
                .withOutput(count, new Text("83.149.9.216\t24.236.252.67\t"))
                .runTest();
    }
}
