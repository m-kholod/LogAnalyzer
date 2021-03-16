import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class LogMapperTest {
    @Test
    public void parseIpAddress() throws IOException, InterruptedException {

        Text inputText = new Text("83.149.9.216 - - [17/May/2015:10:05:43 +0000] \"GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png HTTP/1.1\" 200 171717 \"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"\n");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new LogMapper())
                .withInput(new LongWritable(0), inputText)
                .withOutput(new Text("83.149.9.216"), new IntWritable(1))
                .runTest();
    }
}
