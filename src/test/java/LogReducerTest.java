import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogReducerTest {
    @Test
    public void ipAddressCount() throws IOException {

        List<IntWritable> reducerInput = new ArrayList<IntWritable>();
        reducerInput.add(new IntWritable(3));
        reducerInput.add(new IntWritable(4));

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new LogReducer())
                .withInput(new Text("83.149.9.216"), reducerInput)
                .withOutput(new Text("83.149.9.216"), new IntWritable(7))
                .runTest();
    }
}
